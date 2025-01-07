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

use std::sync::Arc;
use std::time::Duration;

use std::os::unix::io::AsRawFd;
use tokio::net::{TcpSocket, UnixStream};
use tokio::sync::Mutex;

use crate::network::socket::SocketAddress;
use crate::network::sockopt::{
    set_bind_address_no_port, set_dscp, set_local_port_range, set_recv_buf,
    set_tcp_fastopen_connect,
};
use crate::pool::pool::{ConnectionMetadata, ConnectionPool};
use crate::service::peer::{Range, UpstreamPeer};
use crate::stream::{stream::Stream, types::StreamType};

const DEFAULT_POOL_SIZE: usize = 128;

/// used as bridge to connection pool
/// it manages the stream connection
pub struct StreamManager {
    connection_pool: Arc<ConnectionPool<Arc<Mutex<Stream>>>>,
}

impl StreamManager {
    /// new stream manager
    pub fn new(pool_size: Option<usize>) -> Self {
        StreamManager {
            connection_pool: Arc::new(ConnectionPool::new(pool_size.unwrap_or(DEFAULT_POOL_SIZE))),
        }
    }

    /// connect to a given upstream peer
    async fn connect(&self, peer: &UpstreamPeer) -> tokio::io::Result<Stream> {
        let stream = match &peer.address {
            SocketAddress::Tcp(address) => {
                // identify ip address
                let socket = if address.is_ipv4() {
                    TcpSocket::new_v4()
                } else {
                    TcpSocket::new_v6()
                }?;
                let fd = socket.as_raw_fd();
                // set bind address without port
                set_bind_address_no_port(fd, true)
                    .expect("unable to bind peer socket address no port");
                // apply tcp bind options
                if let Some(bind_to) = &peer.tcp_bind {
                    // set port range
                    if let Some(Range(min, max)) = bind_to.port_range {
                        set_local_port_range(fd, min, max)
                            .expect("unable to set peer socket with port range");
                    }
                    // bind address to socket
                    if let Some(address) = bind_to.address {
                        socket
                            .bind(address)
                            .unwrap_or_else(|e| panic!("unable to bind peer socket: {e}"));
                    }
                }
                // apply socket config
                if let Some(config) = &peer.tcp_config {
                    // set tcp fastopen
                    if config.tcp_fast_open {
                        set_tcp_fastopen_connect(fd)
                            .expect("unable to set peer socket tcp fast open");
                    }
                    // set recv buf
                    if let Some(value) = config.tcp_recv_buf {
                        set_recv_buf(fd, value).expect("unable to set peer socket recv buf");
                    }
                    // set dscp
                    if let Some(value) = config.dscp {
                        set_dscp(fd, value).expect("unable to set peer socket dscp");
                    }
                }
                // connect tcp
                match socket.connect(*address).await {
                    Ok(tcp_stream) => {
                        // type parsing
                        let mut stream_type = StreamType::from(tcp_stream);
                        // set no delay by default
                        stream_type.set_no_delay();
                        let dyn_stream_type: Stream = Box::new(stream_type);
                        dyn_stream_type
                    }
                    Err(e) => return Err(e),
                }
            }
            SocketAddress::Unix(socket_path) => {
                // get socket path
                let path = socket_path.as_pathname().expect("invalid unix socket path");
                // connect uds
                match UnixStream::connect(path).await {
                    Ok(unix_stream) => {
                        // type parsing
                        let stream_type = StreamType::from(unix_stream);
                        let dyn_stream_type: Stream = Box::new(stream_type);
                        dyn_stream_type
                    }
                    Err(e) => return Err(e),
                }
            }
        };
        Ok(stream)
    }

    /// find available connection from pool
    async fn find_connection_stream(&self, peer: &UpstreamPeer) -> Option<Stream> {
        // get the peer connection group id
        let connection_group_id = peer.get_group_id();
        // find connection if exist
        match self.connection_pool.find_connection(connection_group_id) {
            Some(wrapped_stream) => {
                // acquire lock
                {
                    let _ = wrapped_stream.lock().await;
                }
                // unwrapping the arc wrapper
                match Arc::try_unwrap(wrapped_stream) {
                    Ok(stream) => {
                        // unwrap the mutex
                        let connection_stream: Stream = stream.into_inner();
                        Some(connection_stream)
                    }
                    Err(_) => None,
                }
            }
            None => None,
        }
    }

    /// query a connection in pool, if does not exist, create a new socket connection
    pub async fn get_connection_from_pool(
        &self,
        peer: &UpstreamPeer,
    ) -> tokio::io::Result<(Stream, bool)> {
        // find connection from pool
        let reused_connection = self.find_connection_stream(&peer).await;
        match reused_connection {
            Some(stream_connection) => return Ok((stream_connection, true)),
            None => {
                // if does not exist, create new connection
                let new_stream_connection = self.connect(&peer).await;
                match new_stream_connection {
                    Ok(stream_connection) => return Ok((stream_connection, false)),
                    Err(e) => return Err(e),
                }
            }
        }
    }

    /// returned used connection after being used
    pub async fn return_connection_to_pool(&self, connection: Stream, peer: &UpstreamPeer) {
        // generate new metadata
        let group_id = peer.get_group_id();
        let unique_id = connection.get_unique_id();
        let metadata = ConnectionMetadata::new(group_id, unique_id);
        // wrapping connection and store it to pool
        let connection_stream = Arc::new(Mutex::new(connection));
        let (closed_connection_notifier, connection_pickup_notification) = self
            .connection_pool
            .add_connection(&metadata, connection_stream);
        let pool = Arc::clone(&self.connection_pool);
        // if the peer provides an idle timeout
        // the returned idle connection will be removed when time exceeded
        if let Some(timeout) = peer.connection_timeout {
            let timeout_duration = Duration::from_secs(timeout as u64);
            tokio::spawn(async move {
                pool.connection_idle_timeout(
                    &metadata,
                    timeout_duration,
                    closed_connection_notifier,
                    connection_pickup_notification,
                )
                .await;
            });
        }
    }
}
