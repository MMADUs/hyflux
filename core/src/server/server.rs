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
use tokio::runtime::{Builder, Handle};
use tokio::signal::unix;
use tokio::sync::{watch, Mutex};
use tokio::time::{sleep, Duration};
use nix::{Error as NixError, NixPath};
use tracing::{error, info};

use crate::service::service::{Service, ServiceType};

use super::fd::ListenerFd;

// just a tokio runtime builder
// used for multithreaded and work stealing configs
pub struct Runtime {
    runtime: tokio::runtime::Runtime,
}

impl Runtime {
    // new runtime builder
    pub fn new(thread_name: &str, alloc_threads: usize) -> Self {
        let built_runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(alloc_threads)
            .thread_name(thread_name)
            .build()
            .unwrap();
        Runtime {
            runtime: built_runtime,
        }
    }
    // handle thread work
    pub fn handle_work(&self) -> &Handle {
        self.runtime.handle()
    }
}

const EXIT_TIMEOUT: u64 = 60 * 5;

const CLOSE_TIMEOUT: u64 = 5;

// shutdown types
enum ShutdownType {
    Graceful,
    Fast,
}

// the main server instance
// the server can held many services
pub struct Server<A> {
    /// list of services
    services: Vec<Service<A>>,
    /// listener fds
    listener_fd: Option<Arc<Mutex<ListenerFd>>>,
    /// shutdown coordinator
    shutdown_send: watch::Sender<bool>,
    shutdown_recv: watch::Receiver<bool>,
    /// graceful upgrade
    upgrade: bool,
    /// daemonized process
    daemonize: bool,
}

impl<A: ServiceType + Send + Sync + 'static> Server<A> {
    /// new server instance
    pub fn new() -> Self {
        let (send, recv) = watch::channel(false);
        Server {
            services: Vec::new(),
            listener_fd: None,
            shutdown_send: send,
            shutdown_recv: recv,
            upgrade: false,
            daemonize: false,
        }
    }

    /// set every listener to be gracefully restart
    pub fn upgrade(&mut self, enabled: bool, path: &str) -> Result<(), NixError> {
        self.upgrade = enabled;
        if self.upgrade {
            let mut fds = ListenerFd::new();
            fds.get_from_socket(path)?;
            self.listener_fd = Some(Arc::new(Mutex::new(fds)));
        } else {
            self.listener_fd = None;
        }
        Ok(())
    }

    /// add service to the server
    pub fn add_service(&mut self, service: Service<A>) {
        self.services.push(service);
    }

    /// run the server forever, this will block the main process
    pub fn run_forever(mut self) {
        info!("running server...");
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
        // running runtimes
        let mut runtimes: Vec<Runtime> = Vec::new();
        while let Some(service) = self.services.pop() {
            // example threads, make it dynamic later
            let alloc_threads = 10;
            let runtime = Self::run_service(service, alloc_threads);
            runtimes.push(runtime);
        }
        // the main server runtime
        let main_runtime = Runtime::new("main-runtime", 1);
        let shutdown_type = main_runtime.handle_work().block_on(
            // this block forever until the entire program exit
            self.run_server(),
        );
        match shutdown_type {
            ShutdownType::Graceful => println!("shutdown gracefully"),
            ShutdownType::Fast => println!("shutdown fast"),
        }
    }

    /// run every service in the server
    fn run_service(service: Service<A>, alloc_threads: usize) -> Runtime {
        // run each listener service on top of the runtime
        let runtime = Runtime::new("service-runtime", alloc_threads);
        let address_stack = service.address_stack.clone();
        // wrap the service into Arc
        // this service will be handled across threads.
        let service = Arc::new(service);
        runtime.handle_work().spawn(async move {
            // run the async service here
            service.start_service(address_stack).await;
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
                    match fds.send_to_socket("") {
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
