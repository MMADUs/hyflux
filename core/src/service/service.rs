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

use futures::future;
use std::fs::Permissions;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tracing::{error, info};

use crate::network::listener::{ListenerAddress, ServiceEndpoint, TcpListenerConfig};
use crate::network::socket::SocketAddress;
use crate::pool::manager::StreamManager;
use crate::server::fd::ListenerFd;
use crate::service::peer::UpstreamPeer;
use crate::stream::stream::Stream;

// TESTING traits for customization soon
pub trait ServiceType: Send + Sync + 'static {
    fn say_hi(&self) -> String;
}

/// service can serve on multiple network
/// many service is served to the main server
pub struct Service<A> {
    pub name: String,
    pub service: A,
    pub address_stack: Vec<ListenerAddress>,
    pub stream_session: StreamManager,
}

// service implementation mainly for managing service
impl<A> Service<A> {
    /// new service instance
    pub fn new(name: &str, service_type: A) -> Self {
        Service {
            name: name.to_string(),
            service: service_type,
            address_stack: Vec::new(),
            stream_session: StreamManager::new(None),
        }
    }

    /// add new tcp address to service
    pub fn add_tcp(&mut self, address: &str, config: Option<TcpListenerConfig>) {
        let tcp_address = ListenerAddress::Tcp(address.to_string(), config);
        self.address_stack.push(tcp_address);
    }

    /// add new unix socket path to service
    pub fn add_unix(&mut self, path: &str, perms: Option<Permissions>) {
        let unix_path = ListenerAddress::Unix(path.to_string(), perms);
        self.address_stack.push(unix_path);
    }
}

// service implementation mainly for running the service
impl<A: ServiceType + Send + Sync + 'static> Service<A> {
    /// preparing to build the listener & start the service
    pub async fn start_service(
        self: &Arc<Self>,
        address_stack: Vec<ListenerAddress>,
        listener_fd: Option<Arc<Mutex<ListenerFd>>>,
        shutdown_notifier: watch::Receiver<bool>,
    ) {
        // build all listeners
        let mut listeners = Vec::with_capacity(address_stack.len());
        for listener_addr in address_stack {
            let address = listener_addr.get_address();
            // check for generated fd
            let fd = match &listener_fd {
                Some(fd_list) => {
                    let list = fd_list.lock().await;
                    list.get_fd(address).copied()
                }
                None => None,
            };
            // build listener
            let listener = listener_addr.bind_to_listener(fd.as_ref()).await;
            listeners.push(listener);
        }
        // spawn task handler for each listener
        let handlers: Vec<_> = listeners
            .into_iter()
            .map(|listener| {
                let service = Arc::clone(self);
                let shutdown_notifier = shutdown_notifier.clone();
                tokio::spawn(async move {
                    service.run_service(listener, shutdown_notifier).await;
                })
            })
            .collect();
        // run the listener handler
        future::join_all(handlers).await;
    }

    /// service io handler
    async fn run_service(
        self: &Arc<Self>,
        listener: ServiceEndpoint,
        mut shutdown_notifier: watch::Receiver<bool>,
    ) {
        // began infinite loop
        // accepting incoming connections
        loop {
            let new_io = tokio::select! {
                new_io = listener.accept_stream() => new_io,
                shutdown_signal = shutdown_notifier.changed() => {
                    match shutdown_signal {
                        Ok(()) => {
                            if !*shutdown_notifier.borrow() {
                                continue;
                            }
                            let address = listener.address.get_address();
                            info!("shutting down: {address}");
                            break;
                        }
                        Err(e) => {
                            error!("shutdown notifier error: {e}");
                            break;
                        }
                    }
                }
            };
            match new_io {
                Ok((downstream, socket_address)) => {
                    // get self reference
                    let service = Arc::clone(self);
                    tokio::spawn(async move {
                        // handle here
                        service.handle_connection(downstream, socket_address).await
                    });
                }
                Err(e) => {
                    println!("failed to accept uds connection: {:?}", e);
                }
            };
        }
    }

    /// handling incoming request
    async fn handle_connection(
        self: &Arc<Self>,
        downstream: Stream,
        _socket_address: SocketAddress,
    ) {
        println!("some message!: {}", self.service.say_hi());

        let address = SocketAddress::parse_tcp("127.0.0.1:8000");

        // simulate a given backend peer
        let peer = UpstreamPeer::new("node 1", &self.name, address);

        // get upstream connection
        let upstream = match self.stream_session.get_connection_from_pool(&peer).await {
            Ok((upstream, is_reused)) => {
                if is_reused {
                    println!("reusing stream from pool");
                } else {
                    println!("connection does not exist in pool, new stream created");
                }
                upstream
            }
            Err(_) => panic!("error getting stream from pool"),
        };

        // handle io copy & returned the upstream
        let upstream = match self.handle_process(downstream, upstream).await {
            Ok(stream) => stream,
            Err(_) => panic!("error during io copy"),
        };

        // return upstream to pool
        self.stream_session
            .return_connection_to_pool(upstream, &peer)
            .await;
        println!("upstream connection returned");
    }
}
