use std::fs::{self, Permissions};
use std::net::{SocketAddr as StdSocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::AsRawFd;
use tokio::net::unix::SocketAddr as UnixSocketAddr;
use tokio::net::TcpSocket;
use tokio::net::{TcpListener, UnixListener};

use crate::stream::{stream::Stream, types::StreamType};

use super::sys::TcpKeepAliveConfig;
use super::sys::{set_dscp, set_tcp_fastopen_backlog};

// generic stuff to make the accept implementation working somehow
pub trait DynSocketAddr: Send + Sync {}

impl DynSocketAddr for UnixSocketAddr {}
impl DynSocketAddr for SocketAddrV4 {}
impl DynSocketAddr for SocketAddrV6 {}

pub type Socket = Box<dyn DynSocketAddr>;

// currently applied for unix
const LISTENER_BACKLOG: u32 = 65535;

// tcp socket listener config
#[derive(Clone, Debug, Default)]
pub struct TcpListenerConfig {
    /// IPV6_ONLY flag (if true, limit socket to IPv6 communication only).
    /// This is mostly useful when binding to `[::]`, which on most Unix distributions
    /// will bind to both IPv4 and IPv6 addresses by default.
    pub ipv6_only: Option<bool>,
    pub tcp_fastopen: Option<usize>,
    pub tcp_keepalive: Option<TcpKeepAliveConfig>,
    /// (Diffrentiated Service Code Point)
    pub dscp: Option<u8>,
}

// listener address is a choice
// wether to use tcp or unix, and will be bind by the implementation
#[derive(Clone)]
pub enum ListenerAddress {
    Tcp(String, Option<TcpListenerConfig>),
    Unix(String, Option<Permissions>),
}

impl ListenerAddress {
    pub async fn bind_to_listener(self) -> ServiceEndpoint {
        let listener = self.bind().await;
        ServiceEndpoint {
            address: self,
            listener,
        }
    }

    async fn bind(&self) -> Listener {
        match self {
            Self::Tcp(address, socket_conf) => {
                // create socket address from string
                let socket_address = match address.to_socket_addrs() {
                    Ok(mut socket_addr) => match socket_addr.next() {
                        Some(address) => address,
                        None => panic!("could not resolve address"),
                    },
                    Err(e) => panic!("{}", e),
                };
                // identify socket address as ip
                let tcp_socket = match socket_address {
                    StdSocketAddr::V4(_) => TcpSocket::new_v4(),
                    StdSocketAddr::V6(_) => TcpSocket::new_v6(),
                };
                let tcp_socket = match tcp_socket {
                    Ok(socket) => socket,
                    Err(e) => panic!("{}", e),
                };
                // tcp socket reuseaddr is enabled by default
                // this makes a listener with the same address in a TIME_WAIT
                if let Err(e) = tcp_socket.set_reuseaddr(true) {
                    panic!("{}", e);
                }
                // apply socket options
                if let Some(config) = socket_conf {
                    let raw_fd = tcp_socket.as_raw_fd();
                    // check if only ipv6
                    if let Some(flag) = config.ipv6_only {
                        let socket_ref = socket2::SockRef::from(&tcp_socket);
                        if let Err(e) = socket_ref.set_only_v6(flag) {
                            panic!("failed to set ipv6 only: {}", e);
                        }
                    }
                    // set tcp fast open
                    if let Some(value) = config.tcp_fastopen {
                        if let Err(e) = set_tcp_fastopen_backlog(raw_fd, value) {
                            panic!("error unable to set tcp fastopen backlog: {}", e);
                        }
                    }
                    // set dscp
                    if let Some(value) = config.dscp {
                        if let Err(e) = set_dscp(raw_fd, value) {
                            panic!("error unable to set dscp: {}", e);
                        }
                    }
                }
                // bind tcp socket to the socket address
                if let Err(e) = tcp_socket.bind(socket_address) {
                    panic!("{}", e);
                }
                // listen to tcp socket
                tcp_socket
                    .listen(LISTENER_BACKLOG)
                    .map(Listener::from)
                    .unwrap_or_else(|e| panic!("{}", e))
            }
            Self::Unix(path, permission) => {
                // remove existing socket path
                match std::fs::remove_file(path) {
                    Ok(()) => (),
                    Err(e) => panic!("{}", e),
                }
                // new unix listener
                let unix_listener = match UnixListener::bind(path) {
                    Ok(listener) => listener,
                    Err(e) => panic!("{}", e),
                };
                // set socket perms read/write permissions for all users on the socket by default
                let perms = permission.clone().unwrap_or(Permissions::from_mode(0o666));
                if let Err(e) = fs::set_permissions(path, perms) {
                    panic!("setting up path {}, set permission error: {}", path, e);
                }
                // convert tokio unix listener to std listener
                let std_listener = match unix_listener.into_std() {
                    Ok(std) => std,
                    Err(e) => panic!("{}", e),
                };
                // get unix std listener socket
                let socket: socket2::Socket = std_listener.into();
                // set listener backlog
                if let Err(e) = socket.listen(LISTENER_BACKLOG as i32) {
                    panic!("{}", e);
                }
                // convert back to tokio unix listener
                UnixListener::from_std(socket.into())
                    .map(Listener::from)
                    .unwrap_or_else(|e| panic!("{}", e))
            }
        }
    }
}

// the main listener type
// the listener is returned right after binding connection stream
// any implementation here is used for any event after connection established
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

pub struct ServiceEndpoint {
    address: ListenerAddress,
    listener: Listener,
}

impl ServiceEndpoint {
    pub async fn accept_stream(&self) -> Result<(Stream, Socket), ()> {
        match &self.listener {
            Listener::Tcp(tcp_listener) => match tcp_listener.accept().await {
                Ok((tcp_downstream, socket_addr)) => {
                    let socket_address: Socket = match socket_addr {
                        StdSocketAddr::V4(addr) => Box::new(addr),
                        StdSocketAddr::V6(addr) => Box::new(addr),
                    };
                    // parsing tcp stream to dynamic stream concrete type
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
                                if let Err(e) = set_dscp(raw_fd, *value) {
                                    panic!("failed to set dscp: {}", e);
                                }
                            }
                        }
                    }
                    let dyn_stream_type: Stream = Box::new(stream_type);
                    Ok((dyn_stream_type, socket_address))
                }
                Err(e) => {
                    println!("unable to accept downstream connection: {}", e);
                    Err(())
                }
            },
            Listener::Unix(unix_listener) => match unix_listener.accept().await {
                Ok((unix_downstream, socket_addr)) => {
                    let socket_address: Socket = Box::new(socket_addr);
                    // parsing unix stream to dynamic stream concrete type
                    let stream_type = StreamType::from(unix_downstream);
                    let dyn_stream_type: Stream = Box::new(stream_type);
                    Ok((dyn_stream_type, socket_address))
                }
                Err(e) => {
                    print!("unable to accept downstream connection: {}", e);
                    Err(())
                }
            },
        }
    }
}
