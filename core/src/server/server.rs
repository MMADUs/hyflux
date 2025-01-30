//! Copyright (c) 2024-2025 Hyflux, Inc.
//!
//! This file is part of Hyflux
//!
//! This program is free software: you can redistribute it and/or modify
//! it under the terms of the GNU Affero General Public License as published by
//! the Free Software Foundation, either version 3 of the License, or
//! (at your option) any later version.
//!
//! This program is distributed in the hope that it will be useful
//! but WITHOUT ANY WARRANTY; without even the implied warranty of
//! MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//! GNU Affero General Public License for more details.
//!
//! You should have received a copy of the GNU Affero General Public License
//! along with this program.  If not, see <http://www.gnu.org/licenses/>.

use dotenv::dotenv;
use std::sync::Arc;
use std::thread;
use tokio::runtime::{Builder, Handle};
use tokio::signal::unix;
use tokio::sync::{watch, Mutex};
use tokio::time::{sleep, Duration};
use tracing::{error, info};

use crate::server::daemon::daemonize_server;
use crate::service::service::{Service, ServiceType};

use super::daemon::DaemonConfig;
use super::fd::ListenerFd;

/// the system runtime for work stealing
pub struct Runtime(tokio::runtime::Runtime);

impl Runtime {
    /// new runtime builder
    pub fn new(thread_name: &str, alloc_threads: usize) -> Self {
        let built_runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(alloc_threads)
            .thread_name(thread_name)
            .build()
            .unwrap();
        Runtime(built_runtime)
    }

    /// runtime handle thread work
    pub fn handle_work(&self) -> &Handle {
        self.0.handle()
    }

    /// runtime shutdown timeout
    pub fn shutdown(self, timeout: Duration) {
        self.0.shutdown_timeout(timeout)
    }
}

/// a timeout before shutting down
const CLOSE_TIMEOUT: u64 = 5;

/// a timeout for shutting down each service runtime
const RUNTIME_TIMOUT: u64 = 5;

/// a timeout before completely shutting down everything
const EXIT_TIMEOUT: u64 = 60 * 5;

/// shutdown types
enum ShutdownType {
    Graceful,
    Fast,
}

/// the server can run mulitple services
pub struct Server<A> {
    /// list of services
    services: Vec<Service<A>>,
    /// the number of threads allocated for each service
    service_threads: usize,
    /// service runtime shutdown timeouts
    service_shutdown_timeout: Option<u64>,
    /// listener fds
    listener_fd: Option<Arc<Mutex<ListenerFd>>>,
    /// shutdown coordinator
    shutdown_send: watch::Sender<bool>,
    shutdown_recv: watch::Receiver<bool>,
    /// graceful upgrade
    upgrade: bool,
    upgrade_socket: Option<String>,
    /// daemonized process
    daemonize: bool,
    daemon_config: Option<DaemonConfig>,
    /// server shutdown duration when graceful
    shutdown_duration: Option<u64>,
}

impl<A: ServiceType + Send + Sync + 'static> Server<A> {
    /// build the server
    pub fn new() -> Self {
        let (send, recv) = watch::channel(false);
        Server {
            services: Vec::new(),
            service_threads: 1,
            service_shutdown_timeout: None,
            listener_fd: None,
            shutdown_send: send,
            shutdown_recv: recv,
            upgrade: false,
            upgrade_socket: None,
            daemonize: false,
            daemon_config: None,
            shutdown_duration: None,
        }
    }

    /// add service to the server
    pub fn add_service(&mut self, service: Service<A>) {
        self.services.push(service);
    }

    /// add multiple services at once to the server
    pub fn add_services(&mut self, services: Vec<Service<A>>) {
        self.services.extend(services);
    }

    /// set every listener to be gracefully restart
    pub fn with_upgrade(&mut self, enabled: bool, path: &str) {
        self.upgrade = enabled;
        if enabled {
            self.upgrade_socket = Some(path.to_string());
        }
    }

    /// set the server process to be daemonized
    pub fn with_daemon(&mut self, enabled: bool, config: DaemonConfig) {
        self.daemonize = enabled;
        if enabled {
            self.daemon_config = Some(config);
        }
    }

    /// set server timeouts
    pub fn set_timeouts(&mut self, shutdown: Option<u64>, runtime: Option<u64>) {
        self.service_shutdown_timeout = runtime;
        self.shutdown_duration = shutdown;
    }

    /// run the server forever, this will block the main process
    pub fn run_forever(mut self) {
        // load env
        dotenv().ok();
        // setup tracing
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_target(true)
            .pretty()
            .init();
        info!("running server...");
        // run daemon process if provided
        if self.daemonize && self.daemon_config.is_some() {
            info!("daemonized server process");
            // safe to unwrap, check .is_some()
            daemonize_server(self.daemon_config.as_ref().unwrap());
        }
        // generate socket if upgrade
        if self.upgrade && self.upgrade_socket.is_some() {
            info!("generating upgrade socket");
            let mut fds = ListenerFd::new();
            // safe to unwrap, check .is_some()
            let socket_path: &str = self.upgrade_socket.as_ref().unwrap();
            fds.get_fds(socket_path)
                .map_err(|e| {
                    error!("failed to generate socket: {e}");
                })
                .expect("error generating socket");
            self.listener_fd = Some(Arc::new(Mutex::new(fds)));
        }
        // running services
        let mut runtimes: Vec<Runtime> = Vec::new();
        while let Some(service) = self.services.pop() {
            let runtime = Self::run_service(
                service,
                self.service_threads,
                self.listener_fd.clone(),
                self.shutdown_recv.clone(),
            );
            runtimes.push(runtime);
        }
        // the main server runtime
        let main_runtime = Runtime::new("main-runtime", 1);
        let shutdown_type = main_runtime.handle_work().block_on(
            // this block forever until the entire program exit
            self.run_server(),
        );
        // period shutdown when graceful
        if matches!(shutdown_type, ShutdownType::Graceful) {
            let timeout = self.shutdown_duration.unwrap_or(EXIT_TIMEOUT);
            let duration = Duration::from_secs(timeout);
            thread::sleep(duration);
        };
        // runtime timeouts
        let runtime_timeout = match shutdown_type {
            ShutdownType::Graceful => {
                let timeout = self.service_shutdown_timeout.unwrap_or(RUNTIME_TIMOUT);
                Duration::from_secs(timeout)
            }
            ShutdownType::Fast => Duration::from_secs(0),
        };
        // shutdown every service runtime
        let runtime_shutdowns: Vec<_> = runtimes
            .into_iter()
            .map(|runtime| {
                thread::spawn(move || {
                    runtime.shutdown(runtime_timeout);
                    thread::sleep(runtime_timeout);
                })
            })
            .collect();
        // shutting down
        for shutdown in runtime_shutdowns {
            if let Err(e) = shutdown.join() {
                error!("failed to shutting down runtime: {:?}", e);
            }
        }
        info!("exiting program");
        std::process::exit(0) // exit code 0
    }

    /// run every service in the server
    fn run_service(
        service: Service<A>,
        alloc_threads: usize,
        listener_fd: Option<Arc<Mutex<ListenerFd>>>,
        shutdown_notifier: watch::Receiver<bool>,
    ) -> Runtime {
        // a runtime is needed to run all the services
        let rt_name = format!("service-runtime: {}", service.name);
        let runtime = Runtime::new(rt_name.as_str(), alloc_threads);
        // this service will be handled concurrently across threads
        let address_stack = service.address_stack.clone();
        let service = Arc::new(service);
        runtime.handle_work().spawn(async move {
            service
                .start_service(address_stack, listener_fd, shutdown_notifier)
                .await;
        });
        runtime
    }

    /// process will be blocked forever in here & receiving exit signal
    async fn run_server(&self) -> ShutdownType {
        // waiting for exit signal
        let mut graceful_upgrade_signal = unix::signal(unix::SignalKind::quit()).unwrap();
        let mut graceful_terminate_signal = unix::signal(unix::SignalKind::terminate()).unwrap();
        let mut fast_shutdown_signal = unix::signal(unix::SignalKind::interrupt()).unwrap();
        tokio::select! {
            // graceful restart receiver
            _ = graceful_upgrade_signal.recv() => {
                info!("SIGQUIT EXIT SIGNAL: Graceful Upgrade");
                if let Some(fds) = &self.listener_fd {
                    // send every current fd to a new listener
                    let fds = fds.lock().await;
                    match fds.send_fds("/tmp/fds.sock") {
                        Ok(_) => {
                            info!("listener socket sent");
                        }
                        Err(e) => {
                            error!("unable to sent new socket to new process: {e}");
                        }
                    }
                    // timeout before shutdown
                    sleep(Duration::from_secs(CLOSE_TIMEOUT)).await;
                    // send shutdown notifier
                    match self.shutdown_send.send(true) {
                        Ok(_) => {
                            info!("graceful shutdown started");
                        }
                        Err(e) => {
                            error!("graceful shutdown failed: {e}");
                            return ShutdownType::Graceful;
                        }
                    }
                    ShutdownType::Graceful
                } else {
                    info!("no socket to be send, shutting down");
                    ShutdownType::Graceful
                }
            },
            // graceful shutdown receiver
            _ = graceful_terminate_signal.recv() => {
                info!("SIGTERM EXIT SIGNAL: Graceful Shutdown");
                // send shutdown notifier
                match self.shutdown_send.send(true) {
                    Ok(_) => {
                        info!("graceful shutdown started");
                    }
                    Err(e) => {
                        info!("graceful shutdown failed: {e}")
                    }
                }
                ShutdownType::Graceful
            },
            // fast shutdonw (not graceful)
            _ = fast_shutdown_signal.recv() => {
                info!("SIGINT EXIT SIGNAL: Fast Shutdown");
                ShutdownType::Fast
            }
        }
    }
}
