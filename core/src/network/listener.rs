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

use std::fs::{self, Permissions};
use std::net::{SocketAddr as StdSocketAddr, ToSocketAddrs};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::AsRawFd;
use tokio::net::TcpSocket;
use tokio::net::{TcpListener, UnixListener};

use crate::stream::{stream::Stream, types::StreamType};

use super::socket::SocketAddress;
use super::sockopt::{set_dscp, set_tcp_fastopen_backlog, TcpKeepAliveConfig};

/// NOTE: currently only used for unix listener
const LISTENER_BACKLOG: u32 = 65535;

/// Tcp socket listener configuration
#[derive(Clone, Debug, Default)]
pub struct TcpListenerConfig {
    /// IPV6_ONLY flag (if true, limit socket to IPv6 communication only).
    /// This is mostly useful when binding to `[::]`, which on most Unix distributions
    /// will bind to both IPv4 and IPv6 addresses by default.
    pub ipv6_only: Option<bool>,
    pub tcp_fastopen: Option<usize>,
    pub tcp_keepalive: Option<TcpKeepAliveConfig>,
    /// dscp (Diffrentiated Service Code Point)
    /// This used to classify and manage network traffic priority.
    pub dscp: Option<u8>,
}

/// Listener address is the identity itself
/// stores the address & configuration
/// address is a type of string, will be parsed to socket address
#[derive(Clone)]
pub enum ListenerAddress {
    Tcp(String, Option<TcpListenerConfig>),
    Unix(String, Option<Permissions>),
}

impl ListenerAddress {
    /// this is called to bind the Listener
    ///
    /// # Listener generation flow
    /// * set a new `ListenerAddress`
    /// * bind listener with the given `ListenerAddress`
    /// * received the `Listener` type
    /// * wraps result into `ServiceEndpoint`
    pub async fn bind_to_listener(self) -> ServiceEndpoint {
        let listener = self.bind().await;
        ServiceEndpoint {
            address: self,
            listener,
        }
    }

    /// bind logic
    async fn bind(&self) -> Listener {
        match self {
            Self::Tcp(address, socket_conf) => {
                // create socket address from string
                let socket_address = match address.to_socket_addrs() {
                    Ok(mut socket_addr) => match socket_addr.next() {
                        Some(address) => address,
                        None => panic!("could not resolve address"),
                    },
                    Err(e) => panic!("error parsing socket address: {e}"),
                };
                // identify socket address ip
                let tcp_socket = match socket_address {
                    StdSocketAddr::V4(_) => TcpSocket::new_v4(),
                    StdSocketAddr::V6(_) => TcpSocket::new_v6(),
                }
                .expect("unable to parse TCP socket");
                // tcp socket reuseaddr is enabled by default
                // this makes a listener with the same address in a TIME_WAIT
                tcp_socket
                    .set_reuseaddr(true)
                    .expect("unable to set listener socket reuse address");
                // apply socket options
                if let Some(config) = socket_conf {
                    let raw_fd = tcp_socket.as_raw_fd();
                    // check if only ipv6
                    if let Some(flag) = config.ipv6_only {
                        let socket_ref = socket2::SockRef::from(&tcp_socket);
                        socket_ref
                            .set_only_v6(flag)
                            .expect("unable to set ipv6 only");
                    }
                    // set tcp fast open
                    if let Some(value) = config.tcp_fastopen {
                        set_tcp_fastopen_backlog(raw_fd, value)
                            .expect("unable to set listener socket fastopen backlog");
                    }
                    // set dscp
                    if let Some(value) = config.dscp {
                        set_dscp(raw_fd, value).expect("unable to set listener socket dscp");
                    }
                }
                // bind address to socket
                tcp_socket
                    .bind(socket_address)
                    .expect("unable to bind address to socket");
                // listen to tcp socket
                tcp_socket
                    .listen(LISTENER_BACKLOG)
                    .map(Listener::from)
                    .unwrap_or_else(|e| panic!("TCP socket unable to listen: {e}"))
            }
            Self::Unix(path, permission) => {
                // remove existing socket path
                std::fs::remove_file(path)
                    .unwrap_or_else(|e| panic!("unable to remove unix file: {e}"));
                // new unix listener
                let unix_listener = UnixListener::bind(path)
                    .unwrap_or_else(|e| panic!("unable to bind unix listener: {e}"));
                // set socket perms read/write permissions for all users on the socket by default
                let perms = permission.clone().unwrap_or(Permissions::from_mode(0o666));
                if let Err(e) = fs::set_permissions(path, perms) {
                    panic!("setting up path {}, set permission error: {}", path, e);
                }
                // convert tokio unix listener to std listener
                let std_listener = unix_listener
                    .into_std()
                    .expect("unable to convert unix listener");
                // get unix std listener socket
                let socket: socket2::Socket = std_listener.into();
                // listen to socket with backlog
                socket
                    .listen(LISTENER_BACKLOG as i32)
                    .unwrap_or_else(|e| panic!("Unix socket unable to listen: {e}"));
                // convert back to tokio unix listener
                UnixListener::from_std(socket.into())
                    .map(Listener::from)
                    .unwrap_or_else(|e| panic!("{}", e))
            }
        }
    }
}

/// the main listener type
#[derive(Debug)]
pub enum Listener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

impl From<TcpListener> for Listener {
    fn from(s: TcpListener) -> Self {
        Self::Tcp(s)
    }
}

impl From<UnixListener> for Listener {
    fn from(s: UnixListener) -> Self {
        Self::Unix(s)
    }
}

/// the service endpoint type
/// the type is received after a successful listener bind
pub struct ServiceEndpoint {
    address: ListenerAddress,
    listener: Listener,
}

impl ServiceEndpoint {
    /// this is called to accept incoming client request
    pub async fn accept_stream(&self) -> Result<(Stream, SocketAddress), ()> {
        match &self.listener {
            Listener::Tcp(tcp_listener) => match tcp_listener.accept().await {
                Ok((tcp_downstream, address)) => {
                    // type parsing
                    let socket_address = SocketAddress::Tcp(address);
                    let mut stream_type = StreamType::from(tcp_downstream);
                    // set nodelay by default
                    stream_type.set_no_delay();
                    // apply socket config
                    if let ListenerAddress::Tcp(_, socket_conf) = &self.address {
                        if let Some(config) = socket_conf {
                            let raw_fd = stream_type.as_raw_fd();
                            // set keepalive
                            if let Some(keepalive_conf) = &config.tcp_keepalive {
                                stream_type.set_keepalive(keepalive_conf);
                            }
                            // set dscp
                            if let Some(value) = &config.dscp {
                                set_dscp(raw_fd, *value)
                                    .expect("unable to set listener socket dscp");
                            }
                        }
                    }
                    let dyn_stream_type: Stream = Box::new(stream_type);
                    Ok((dyn_stream_type, socket_address))
                }
                Err(e) => {
                    println!("unable to accept downstream connection: {e}");
                    Err(())
                }
            },
            Listener::Unix(unix_listener) => match unix_listener.accept().await {
                Ok((unix_downstream, address)) => {
                    // type parsing
                    let socket_address = SocketAddress::Unix(address.into());
                    let stream_type = StreamType::from(unix_downstream);
                    let dyn_stream_type: Stream = Box::new(stream_type);
                    Ok((dyn_stream_type, socket_address))
                }
                Err(e) => {
                    println!("unable to accept downstream connection: {e}");
                    Err(())
                }
            },
        }
    }
}
