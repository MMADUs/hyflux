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

use nix::sys::socket::{getpeername, getsockname, SockaddrStorage};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr as StdSockAddr;
use std::os::unix::net::SocketAddr as StdUnixSockAddr;
use std::str::FromStr;

/// type used for socket address
#[derive(Debug, Clone)]
pub enum SocketAddress {
    Tcp(StdSockAddr),
    Unix(StdUnixSockAddr),
}

impl SocketAddress {
    /// make tcp socket address from string
    pub fn parse_tcp(raw: &str) -> Self {
        match StdSockAddr::from_str(raw) {
            Ok(address) => SocketAddress::Tcp(address),
            Err(_) => panic!("invalid tcp socket address"),
        }
    }

    /// make unix socket address from string
    pub fn parse_unix(raw: &str) -> Self {
        match StdUnixSockAddr::from_pathname(raw) {
            Ok(path) => SocketAddress::Unix(path),
            Err(_) => panic!("invalid unix path"),
        }
    }

    /// extract tcp socket address from the type
    pub fn as_tcp(&self) -> Option<&StdSockAddr> {
        if let SocketAddress::Tcp(address) = self {
            Some(address)
        } else {
            None
        }
    }

    /// extract unix socket address from the type
    pub fn as_unix(&self) -> Option<&StdUnixSockAddr> {
        if let SocketAddress::Unix(address) = self {
            Some(address)
        } else {
            None
        }
    }

    /// set the port if socket address is tcp
    pub fn set_port(&mut self, port: u16) {
        if let SocketAddress::Tcp(address) = self {
            address.set_port(port);
        }
    }

    /// storages correspond to query for socket addresses
    fn from_storage(sock: &SockaddrStorage) -> Option<SocketAddress> {
        // check for ipv4 & ipv6
        if let Some(v4) = sock.as_sockaddr_in() {
            let socket = std::net::SocketAddrV4::new(v4.ip().into(), v4.port());
            let address = SocketAddress::Tcp(StdSockAddr::V4(socket));
            return Some(address);
        } else if let Some(v6) = sock.as_sockaddr_in6() {
            let socket =
                std::net::SocketAddrV6::new(v6.ip(), v6.port(), v6.flowinfo(), v6.scope_id());
            let address = SocketAddress::Tcp(StdSockAddr::V6(socket));
            return Some(address);
        }
        // check for unix socket
        let unix_socket = sock
            .as_unix_addr()
            .map(|addr| addr.path().map(StdUnixSockAddr::from_pathname))??
            .ok()?;
        let address = SocketAddress::Unix(unix_socket);
        Some(address)
    }

    /// get the socket address type by the given fd (file descriptors)
    pub fn from_raw_fd(fd: std::os::unix::io::RawFd, peer_address: bool) -> Option<SocketAddress> {
        // get address from fd
        let storage = if peer_address {
            getpeername(fd)
        } else {
            getsockname(fd)
        };
        // look for socket in storage
        match storage {
            Ok(socket) => Self::from_storage(&socket),
            Err(_) => None,
        }
    }
}

impl Hash for SocketAddress {
    /// implementation for address hashing
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Tcp(sockaddr) => sockaddr.hash(state),
            Self::Unix(sockaddr) => {
                if let Some(path) = sockaddr.as_pathname() {
                    // use the underlying path as the hash
                    path.hash(state);
                } else {
                    // unnamed or abstract UDS
                    // abstract UDS name not yet exposed by std API
                    // panic for now, we can decide on the right way to hash them later
                    panic!("Unnamed and abstract UDS types not yet supported for hashing")
                }
            }
        }
    }
}

impl PartialEq for SocketAddress {
    /// implementation for address partial equality
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Tcp(addr) => Some(addr) == other.as_tcp(),
            Self::Unix(addr) => {
                let path = addr.as_pathname();
                path.is_some() && path == other.as_unix().and_then(|addr| addr.as_pathname())
            }
        }
    }
}

/// implementation for strict address equality
impl Eq for SocketAddress {}

impl PartialOrd for SocketAddress {
    /// implementation for address partial ordering
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SocketAddress {
    /// implementation for ordering in a collections
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self {
            Self::Tcp(addr) => {
                if let Some(o) = other.as_tcp() {
                    addr.cmp(o)
                } else {
                    Ordering::Less
                }
            }
            Self::Unix(addr) => {
                if let Some(o) = other.as_unix() {
                    addr.as_pathname().cmp(&o.as_pathname())
                } else {
                    Ordering::Greater
                }
            }
        }
    }
}
