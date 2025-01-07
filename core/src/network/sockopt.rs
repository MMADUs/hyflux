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

use crate::network::socket::SocketAddress;
use libc::{self, c_int, c_void, socklen_t};
use std::{
    io::{self, Error, ErrorKind},
    mem,
    os::unix::io::RawFd,
    time::Duration,
};

/// wrapper used to set socket options
fn set_socket_option<T: Copy>(
    fd: RawFd,
    level: c_int,
    optname: c_int,
    value: &T,
) -> io::Result<()> {
    let result = unsafe {
        libc::setsockopt(
            fd,
            level,
            optname,
            value as *const T as *const c_void,
            mem::size_of::<T>() as socklen_t,
        )
    };

    if result == -1 {
        Err(Error::last_os_error().into())
    } else {
        Ok(())
    }
}

/// TCP keep-alive config
#[derive(Clone, Debug)]
pub struct TcpKeepAliveConfig {
    /// The time a connection needs to be idle before TCP begins sending out keep-alive probes.
    pub idle: Duration,
    /// The number of seconds between TCP keep-alive probes.
    pub interval: Duration,
    /// The maximum number of TCP keep-alive probes to send before giving up and killing the connection
    pub count: usize,
}

impl TcpKeepAliveConfig {
    /// make a new TCP keep-alive config
    pub fn new(idle_secs: u64, interval_secs: u64, count: usize) -> Self {
        TcpKeepAliveConfig {
            idle: Duration::from_secs(idle_secs),
            interval: Duration::from_secs(interval_secs),
            count,
        }
    }
}

impl Default for TcpKeepAliveConfig {
    /// default TCP keep-alive config
    fn default() -> Self {
        TcpKeepAliveConfig {
            idle: Duration::from_secs(5),
            interval: Duration::from_secs(5),
            count: 5,
        }
    }
}

/// apply the given TCP keep-alive config
pub fn set_tcp_keepalive(fd: RawFd, config: &TcpKeepAliveConfig) -> io::Result<()> {
    set_keepalive_flag(fd, true)?;
    set_keepalive_idle(fd, config.idle)?;
    set_keepalive_interval(fd, config.interval)?;
    set_keepalive_count(fd, config.count)
}

/// set the TCP keep-alive flag
/// this helps maintain idle TCP connections by sending periodic keepalive probes.
pub fn set_keepalive_flag(fd: RawFd, val: bool) -> io::Result<()> {
    set_socket_option(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, &(val as c_int))
}

/// Set the TCP keepalive idle time in seconds.
/// this specifies the duration (in seconds) the connection must remain idle before
/// TCP sends the first keepalive probe.
pub fn set_keepalive_idle(fd: RawFd, idle_secs: Duration) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_TCP,
        libc::TCP_KEEPIDLE,
        &(idle_secs.as_secs() as c_int),
    )
}

/// Set the TCP keepalive probe interval in seconds.
/// This specifies the interval (in seconds) between successive keepalive probes
/// if no acknowledgment is received.
pub fn set_keepalive_interval(fd: RawFd, interval_secs: Duration) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_TCP,
        libc::TCP_KEEPINTVL,
        &(interval_secs.as_secs() as c_int),
    )
}

/// Set the TCP keepalive probe count.
/// This specifies the maximum number of keepalive probes sent without receiving
/// a response before the connection is considered broken.
pub fn set_keepalive_count(fd: RawFd, count: usize) -> io::Result<()> {
    set_socket_option(fd, libc::IPPROTO_TCP, libc::TCP_KEEPCNT, &(count as c_int))
}

/// Enable the TCP Fast Open connect option.
/// This allows sending data in the initial SYN packet during the TCP handshake,
/// reducing the latency for the first data exchange.
pub fn set_tcp_fastopen_connect(fd: RawFd) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_TCP,
        libc::TCP_FASTOPEN_CONNECT,
        &(1 as c_int),
    )
}

// Set the TCP Fast Open backlog size.
// This specifies the maximum number of pending Fast Open connections that the
// socket can accept. It is useful for servers to improve connection establishment
// performance.
pub fn set_tcp_fastopen_backlog(fd: RawFd, backlog: usize) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_TCP,
        libc::TCP_FASTOPEN,
        &(backlog as c_int),
    )
}

/// Set TCP_NODELAY flag (Nagle's algorithm).
/// When enabled, Nagle's algorithm is disabled, allowing small packets to be sent immediately without waiting for more data.
/// This reduces latency but may increase the number of packets sent.
/// When disabled, Nagle's algorithm is used to combine small packets into larger ones to optimize network efficiency.
pub fn set_tcp_nodelay(fd: RawFd, enable: bool) -> io::Result<()> {
    set_socket_option(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, &(enable as c_int))
}

/// Set TCP_QUICKACK flag.
/// When enabled, the TCP stack sends acknowledgments (ACKs) immediately for received packets, reducing latency.
/// When disabled, the TCP stack may delay ACKs to optimize network performance by reducing the number of packets sent.
pub fn set_tcp_quickack(fd: RawFd, enable: bool) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_TCP,
        libc::TCP_QUICKACK,
        &(enable as c_int),
    )
}

/// Enable port reuse on a socket.
/// This allows multiple sockets to bind to the same port.
/// The idle socket that listen to same port will be in the TIME-WAIT state
pub fn set_reuse_port(fd: RawFd) -> io::Result<()> {
    set_socket_option(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, &(1 as c_int))
}

/// Set the size of the receive buffer for a socket.
/// This specifies the maximum amount of incoming data that can be queued by the
/// operating system for the socket before the application processes it.
pub fn set_recv_buf(fd: RawFd, size: usize) -> io::Result<()> {
    set_socket_option(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &(size as c_int))
}

/// Set the TCP defer accept timeout.
/// This specifies the duration (in seconds) the connection will wait for incoming
/// data before the accept system call returns, reducing resource usage for
/// connections with no immediate data.
pub fn set_defer_accept(fd: RawFd, timeout: i32) -> io::Result<()> {
    set_socket_option(fd, libc::IPPROTO_TCP, libc::TCP_DEFER_ACCEPT, &timeout)
}

/// Set the TCP window clamp size.
/// This specifies the maximum receive window size for the connection, overriding
/// the system's default window size to optimize network performance.
pub fn set_window_clamp(fd: RawFd, size: i32) -> io::Result<()> {
    set_socket_option(fd, libc::IPPROTO_TCP, libc::TCP_WINDOW_CLAMP, &size)
}

/// Set the priority for a socket.
/// This assigns a priority value to the socket, influencing the queuing
/// and scheduling of outgoing packets. Higher values indicate higher priority.
pub fn set_priority(fd: RawFd, priority: i32) -> io::Result<()> {
    set_socket_option(fd, libc::SOL_SOCKET, libc::SO_PRIORITY, &priority)
}

/// Set IP_BIND_ADDRESS_NO_PORT flag.
/// When enabled, the kernel allows binding a socket to an IP address without assigning a port,
/// deferring the port assignment until the socket is connected or data is sent.
pub fn set_bind_address_no_port(fd: RawFd, enable: bool) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_IP,
        libc::IP_BIND_ADDRESS_NO_PORT,
        &(enable as c_int),
    )
}

/// Set the local port range for ephemeral ports.
/// This specifies the minimum (`min`) and maximum (`max`) values for the range of
/// local ports that the kernel can use for automatic port assignment.
/// If the option is not supported (e.g., ENOPROTOOPT), the function succeeds
/// silently.
pub fn set_local_port_range(fd: RawFd, min: u16, max: u16) -> io::Result<()> {
    const IP_LOCAL_PORT_RANGE: i32 = 51;
    let range: u32 = (min as u32) | ((max as u32) << 16);

    let result = set_socket_option(fd, libc::IPPROTO_IP, IP_LOCAL_PORT_RANGE, &(range as c_int));
    match result {
        Err(e) if e.raw_os_error() != Some(libc::ENOPROTOOPT) => Err(e),
        _ => Ok(()), // no error or ENOPROTOOPT
    }
}

/// Set the Differentiated Services Code Point (DSCP) for a socket.
/// DSCP is used to classify network traffic for Quality of Service (QoS).
/// This function determines if the socket is IPv4 or IPv6 and sets the DSCP
/// value in the appropriate header field (IP_TOS for IPv4, IPV6_TCLASS for IPv6).
pub fn set_dscp(fd: RawFd, value: u8) -> io::Result<()> {
    // Convert the file descriptor to a SocketAddr
    let sock = SocketAddress::from_raw_fd(fd, false);
    let addr = match sock.as_ref().and_then(|s| s.as_tcp()) {
        Some(a) => a,
        None => {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "failed to set dscp, invalid IP socket",
            ));
        }
    };
    // Set the DSCP value based on whether it's IPv6 or IPv4
    if addr.is_ipv6() {
        set_socket_option(fd, libc::IPPROTO_IPV6, libc::IPV6_TCLASS, &(value as c_int))
    } else {
        set_socket_option(fd, libc::IPPROTO_IP, libc::IP_TOS, &(value as c_int))
    }
}
