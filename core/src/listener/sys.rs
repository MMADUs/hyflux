use crate::listener::socket::SocketAddress;
use libc::{self, c_int, c_void, socklen_t};
use std::{
    io::{self, Error, ErrorKind},
    mem,
    os::unix::io::RawFd,
    time::Duration,
};

// a wrapper for setting socket options
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

// tcp keep alive config
#[derive(Clone, Debug)]
pub struct TcpKeepAliveConfig {
    /// The time a connection needs to be idle before TCP begins sending out keep-alive probes.
    pub idle: Duration,
    /// The number of seconds between TCP keep-alive probes.
    pub interval: Duration,
    /// The maximum number of TCP keep-alive probes to send before giving up and killing the connection
    pub count: usize,
}

// new socket configuration with options
impl TcpKeepAliveConfig {
    pub fn new(idle_secs: u64, interval_secs: u64, count: usize) -> Self {
        TcpKeepAliveConfig {
            idle: Duration::from_secs(idle_secs),
            interval: Duration::from_secs(interval_secs),
            count,
        }
    }
}

// new socket by default configuration
impl Default for TcpKeepAliveConfig {
    fn default() -> Self {
        TcpKeepAliveConfig {
            idle: Duration::from_secs(5),
            interval: Duration::from_secs(5),
            count: 5,
        }
    }
}

// used to inject the keep alvie configuration
// enabling keepalive with configurations at once.
pub fn set_tcp_keepalive(fd: RawFd, config: &TcpKeepAliveConfig) -> io::Result<()> {
    set_keepalive_flag(fd, true)?;
    set_keepalive_idle(fd, config.idle)?;
    set_keepalive_interval(fd, config.interval)?;
    set_keepalive_count(fd, config.count)
}

// enable TCP keepalive
pub fn set_keepalive_flag(fd: RawFd, val: bool) -> io::Result<()> {
    set_socket_option(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, &(val as c_int))
}

// set TCP keepalive idle time in seconds
pub fn set_keepalive_idle(fd: RawFd, idle_secs: Duration) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_TCP,
        libc::TCP_KEEPIDLE,
        &(idle_secs.as_secs() as c_int),
    )
}

// set TCP keepalive probe interval in seconds
pub fn set_keepalive_interval(fd: RawFd, interval_secs: Duration) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_TCP,
        libc::TCP_KEEPINTVL,
        &(interval_secs.as_secs() as c_int),
    )
}

// set TCP keepalive probe count
pub fn set_keepalive_count(fd: RawFd, count: usize) -> io::Result<()> {
    set_socket_option(fd, libc::IPPROTO_TCP, libc::TCP_KEEPCNT, &(count as c_int))
}

// set TCP fast open queue length
pub fn set_tcp_fastopen(fd: RawFd, qlen: i32) -> io::Result<()> {
    set_socket_option(fd, libc::IPPROTO_TCP, libc::TCP_FASTOPEN, &qlen)
}

// set TCP fast open connect option
pub fn set_tcp_fastopen_connect(fd: RawFd) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_TCP,
        libc::TCP_FASTOPEN_CONNECT,
        &(1 as c_int),
    )
}

// set TCP fast open backlog
pub fn set_tcp_fastopen_backlog(fd: RawFd, backlog: usize) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_TCP,
        libc::TCP_FASTOPEN,
        &(backlog as c_int),
    )
}

// enable/disable TCP nodelay (Nagle's algorithm)
pub fn set_tcp_nodelay(fd: RawFd, enable: bool) -> io::Result<()> {
    set_socket_option(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, &(enable as c_int))
}

// enable/disable TCP quickack
pub fn set_tcp_quickack(fd: RawFd, enable: bool) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_TCP,
        libc::TCP_QUICKACK,
        &(enable as c_int),
    )
}

// enable port reuse
pub fn set_reuse_port(fd: RawFd) -> io::Result<()> {
    set_socket_option(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, &(1 as c_int))
}

// set receive buffer size
pub fn set_recv_buf(fd: RawFd, size: usize) -> io::Result<()> {
    set_socket_option(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &(size as c_int))
}

// set TCP defer accept timeout
pub fn set_defer_accept(fd: RawFd, timeout: i32) -> io::Result<()> {
    set_socket_option(fd, libc::IPPROTO_TCP, libc::TCP_DEFER_ACCEPT, &timeout)
}

// set TCP window clamp size
pub fn set_window_clamp(fd: RawFd, size: i32) -> io::Result<()> {
    set_socket_option(fd, libc::IPPROTO_TCP, libc::TCP_WINDOW_CLAMP, &size)
}

// set socket priority
pub fn set_priority(fd: RawFd, priority: i32) -> io::Result<()> {
    set_socket_option(fd, libc::SOL_SOCKET, libc::SO_PRIORITY, &priority)
}

// set bind address no port option
pub fn set_bind_address_no_port(fd: RawFd, enable: bool) -> io::Result<()> {
    set_socket_option(
        fd,
        libc::IPPROTO_IP,
        libc::IP_BIND_ADDRESS_NO_PORT,
        &(enable as c_int),
    )
}

// set local port range
pub fn ip_local_port_range(fd: RawFd, low: u16, high: u16) -> io::Result<()> {
    const IP_LOCAL_PORT_RANGE: i32 = 51;
    let range: u32 = (low as u32) | ((high as u32) << 16);

    let result = set_socket_option(fd, libc::IPPROTO_IP, IP_LOCAL_PORT_RANGE, &(range as c_int));
    match result {
        Err(e) if e.raw_os_error() != Some(libc::ENOPROTOOPT) => Err(e),
        _ => Ok(()), // no error or ENOPROTOOPT
    }
}

// set dscp
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
