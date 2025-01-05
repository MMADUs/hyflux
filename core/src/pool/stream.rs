use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpSocket, UnixStream};
use tokio::sync::Mutex;
use std::os::unix::io::AsRawFd;

use crate::listener::socket::SocketAddress;
use crate::listener::sys::{set_bind_address_no_port, set_dscp, set_local_port_range, set_recv_buf, set_tcp_fastopen_connect};
use crate::pool::pool::{ConnectionMetadata, ConnectionPool};
use crate::service::peer::{UpstreamPeer, Range};
use crate::stream::{stream::Stream, types::StreamType};

// the stream manager is used as the bridge from request lifetime to connection pool
// used to managing socket stream connection
pub struct StreamManager {
    connection_pool: Arc<ConnectionPool<Arc<Mutex<Stream>>>>,
}

const DEFAULT_POOL_SIZE: usize = 128;

// PUBLIC METHODS
// stream manager implementation
impl StreamManager {
    // new stream manager
    pub fn new(pool_size: Option<usize>) -> Self {
        StreamManager {
            connection_pool: Arc::new(ConnectionPool::new(pool_size.unwrap_or(DEFAULT_POOL_SIZE))),
        }
    }

    // used to retrive connection from pool
    // returns the stream and the bool value determine the newly created stream or reused
    pub async fn get_connection_from_pool(
        &self,
        peer: &UpstreamPeer,
    ) -> tokio::io::Result<(Stream, bool)> {
        self.get_stream_connection(peer).await
    }

    // used to return connection after use
    // returns nothing
    pub async fn return_connection_to_pool(&self, connection: Stream, peer: &UpstreamPeer) {
        self.return_stream_connection(connection, peer).await
    }
}

// PRIVATE METHODS
// stream manager implementation
impl StreamManager {
    // used to make a new socket connection to upstream peer
    // returns the connection stream
    async fn new_stream_connection(&self, peer: &UpstreamPeer) -> tokio::io::Result<Stream> {
        let stream = match &peer.address {
            SocketAddress::Tcp(address) => {
                // identify tcp ip
                let socket = if address.is_ipv4() {
                    TcpSocket::new_v4()
                } else {
                    TcpSocket::new_v6()
                }?;
                let fd = socket.as_raw_fd();
                // set bind address without port
                if let Err(e) = set_bind_address_no_port(fd, true) {
                    panic!("error set bind address no port: {}", e);
                }
                // apply tcp bind options
                if let Some(bind_to) = &peer.tcp_bind {
                    // set port range
                    if let Some(Range(min, max)) = bind_to.port_range {
                        if let Err(e) = set_local_port_range(fd, min, max) {
                            panic!("error set local port range tcp bind: {e}");
                        }
                    }
                    // bind address to socket address
                    if let Some(address) = bind_to.address {
                        if let Err(e) = socket.bind(address) {
                            panic!("error bind tcp socket address: {e}");
                        }
                    }
                }
                // apply socket config
                if let Some(config) = &peer.tcp_config {
                    // set tcp fastopen
                    if config.tcp_fast_open {
                        if let Err(e) = set_tcp_fastopen_connect(fd) {
                            panic!("error unable to set tcp fast open: {}", e)
                        }
                    }
                    // set recv buf
                    if let Some(value) = config.tcp_recv_buf {
                        if let Err(e) = set_recv_buf(fd, value) {
                            panic!("error unable to set tcp recv buf: {}", e);
                        }
                    }
                    // set dscp
                    if let Some(value) = config.dscp {
                        if let Err(e) = set_dscp(fd, value) {
                            panic!("error unable to set dscp: {}", e);
                        }
                    }
                }
                // connect tcp
                match socket.connect(*address).await {
                    Ok(tcp_stream) => {
                        // dynamic type convert
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
                let path = socket_path.as_pathname().expect("none value in unix socket path");
                // connect uds
                match UnixStream::connect(path).await {
                    Ok(unix_stream) => {
                        // dynamic type convert
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

    // used to find a connection from the connection pool
    // returns some stream, there likely a chance stream does not exist
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

    // used to get the stream connection
    // this is the function that is going to be called during request
    // find a connection in pool, if does not exist, create a new socket connection
    // returns the stream and the bool to determine if the connection is new or reused.
    async fn get_stream_connection(
        &self,
        peer: &UpstreamPeer,
    ) -> tokio::io::Result<(Stream, bool)> {
        // find connection from pool
        let reused_connection = self.find_connection_stream(&peer).await;
        match reused_connection {
            Some(stream_connection) => return Ok((stream_connection, true)),
            None => {
                // new socket connection
                let new_stream_connection = self.new_stream_connection(&peer).await;
                match new_stream_connection {
                    Ok(stream_connection) => return Ok((stream_connection, false)),
                    Err(e) => return Err(e),
                }
            }
        }
    }

    // used to return used connection after request finished
    // returns nothing
    async fn return_stream_connection(&self, connection: Stream, peer: &UpstreamPeer) {
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
