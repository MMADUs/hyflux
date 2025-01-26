use crate::network::socket::SocketAddress;
use crate::network::sockopt::TcpKeepAliveConfig;
use crate::pool::pool::ConnectionGroupID;
use ahash::AHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr as StdSockAddr;

#[derive(Clone, Debug)]
pub struct Range(pub u16, pub u16);

impl Range {
    pub fn is_valid(&self) -> bool {
        let Range(min, max) = *self;
        min > 0 && min < max
    }
}

#[derive(Clone, Debug, Default)]
pub struct TcpBind {
    pub address: Option<StdSockAddr>,
    pub port_range: Option<Range>,
    pub fallback: bool,
}

impl TcpBind {
    pub fn bind_to(&mut self, address: Option<StdSockAddr>, port_range: Option<Range>) -> Self {
        TcpBind {
            address,
            port_range,
            fallback: false,
        }
    }

    pub fn set_port_range(&mut self, range: Option<Range>) {
        if range.is_none() && self.port_range.is_none() {
            return;
        }

        self.port_range = match range {
            None => Some(Range(0, 0)),
            Some(range) if range.is_valid() => Some(range),
            Some(Range(min, max)) => panic!("invalid port range: ({min}, {max})"),
        };
    }

    pub fn set_fallback(&mut self, value: bool) {
        self.fallback = value;
    }

    pub fn will_fallback(&self) -> bool {
        self.fallback && self.port_range.is_some()
    }
}

#[derive(Clone, Debug, Default)]
pub struct TcpUpstreamConfig {
    pub tcp_fast_open: bool,
    pub tcp_keepalive: Option<TcpKeepAliveConfig>,
    pub tcp_recv_buf: Option<usize>,
    pub dscp: Option<u8>,
}

// upstream peer is a metadata for upstream servers
// storing information for server peer
#[derive(Clone, Debug)]
pub struct UpstreamPeer {
    pub name: String,
    pub service: String,
    pub address: SocketAddress,
    pub tcp_bind: Option<TcpBind>,
    pub connection_timeout: Option<usize>,
    pub tcp_config: Option<TcpUpstreamConfig>,
}

impl UpstreamPeer {
    // new upstream peer
    pub fn new(name: &str, service: &str, socket_address: SocketAddress) -> Self {
        UpstreamPeer {
            name: name.to_string(),
            service: service.to_string(),
            address: socket_address,
            tcp_bind: None,
            connection_timeout: None,
            tcp_config: None,
        }
    }

    // get the group id from this peer
    // each peer should give unique group id
    pub fn get_group_id(&self) -> ConnectionGroupID {
        let mut hasher = AHasher::default();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

// hash implementation to generate connection peer id
impl Hash for UpstreamPeer {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.name.hash(hasher);
        self.service.hash(hasher);
        self.address.hash(hasher);
    }
}
