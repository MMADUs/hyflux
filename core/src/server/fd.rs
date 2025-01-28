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

use nix::errno::Errno;
use nix::sys::socket::{self, AddressFamily, RecvMsg, SockFlag, SockType, UnixAddr};
use nix::sys::stat;
use nix::{Error as NixError, NixPath};
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{IoSlice, IoSliceMut, Write};
use std::os::unix::io::RawFd;
use std::thread;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// a list of listener file descriptors
pub struct ListenerFd {
    fds: HashMap<String, RawFd>,
}

impl ListenerFd {
    /// new fds list
    pub fn new() -> Self {
        ListenerFd {
            fds: HashMap::new(),
        }
    }

    /// add listener fd to list
    pub fn add(&mut self, address: String, fd: RawFd) {
        self.fds.insert(address, fd);
    }

    /// get listener fd from list
    pub fn get(&self, address: &str) -> Option<&RawFd> {
        self.fds.get(address)
    }

    /// split both addresses and fds
    pub fn serialize(&self) -> (Vec<String>, Vec<RawFd>) {
        self.fds.iter().map(|(k, v)| (k.clone(), v)).unzip()
    }

    /// merge back address & fds to list
    pub fn deserialize(&mut self, addresses: Vec<String>, fds: Vec<RawFd>) {
        if addresses.len() == fds.len() {
            for (addr, fd) in addresses.into_iter().zip(fds) {
                self.fds.insert(addr, fd);
            }
        } else {
            panic!("address and fd size must be equal");
        }
    }

    /// send the fds to the new process
    pub fn send_to_socket<P>(&self, path: &P) -> Result<usize, NixError>
    where
        P: ?Sized + NixPath + Display,
    {
        let (addrs, fds) = self.serialize();
        let mut buffer: [u8; 2048] = [0; 2048];
        let key_size = serialize_vec_string(&addrs, &mut buffer);
        send_fds(fds, &buffer[..key_size], path)
    }

    /// get all the fds from the current process
    pub fn get_from_socket<P>(&mut self, path: &P) -> Result<(), NixError>
    where
        P: ?Sized + NixPath + Display,
    {
        let mut buffer: [u8; 2048] = [0; 2048];
        let (fds, bytes) = get_fds(path, &mut buffer)?;
        let keys = deserialize_vec_string(&buffer[..bytes])?;
        self.deserialize(keys, fds);
        Ok(())
    }
}

fn serialize_vec_string(vec_string: &[String], mut buffer: &mut [u8]) -> usize {
    let joined = vec_string.join(" ");
    buffer.write(joined.as_bytes()).unwrap()
}

fn deserialize_vec_string(buffer: &[u8]) -> Result<Vec<String>, NixError> {
    let joined = std::str::from_utf8(buffer).map_err(|_| NixError::EINVAL)?;
    let str = joined.split_ascii_whitespace().map(String::from).collect();
    Ok(str)
}

const MAX_RETRY: usize = 5;

const RETRY_INTERVAL: Duration = Duration::from_secs(1);

fn accept_with_retry(listen_fd: i32) -> Result<i32, NixError> {
    let mut retried = 0;
    loop {
        match socket::accept(listen_fd) {
            Ok(fd) => return Ok(fd),
            Err(e) => {
                if retried > MAX_RETRY {
                    return Err(e);
                }
                match e {
                    Errno::EAGAIN => {
                        error!(
                            "No incoming socket transfer, sleep {RETRY_INTERVAL:?} and try again"
                        );
                        retried += 1;
                        thread::sleep(RETRY_INTERVAL);
                    }
                    _ => {
                        error!("Error accepting socket transfer: {e}");
                        return Err(e);
                    }
                }
            }
        }
    }
}

pub fn get_fds<P>(path: &P, payload: &mut [u8]) -> Result<(Vec<RawFd>, usize), NixError>
where
    P: ?Sized + NixPath + Display,
{
    const MAX_FDS: usize = 32;

    let listener_fd = socket::socket(
        AddressFamily::Unix,
        SockType::Stream,
        SockFlag::SOCK_NONBLOCK,
        None,
    )
    .unwrap();

    let unix_address = UnixAddr::new(path).unwrap();

    match nix::unistd::unlink(path) {
        Ok(()) => {
            info!("success unlink: {path}");
        }
        Err(e) => {
            debug!("error unlink: {path}: {e}");
        }
    }

    socket::bind(listener_fd, &unix_address).unwrap();

    /* sock is created before we change user, need to give permission to all */
    stat::fchmodat(
        None,
        path,
        stat::Mode::all(),
        stat::FchmodatFlags::FollowSymlink,
    )
    .unwrap();

    socket::listen(listener_fd, 8).unwrap();

    let fd = match accept_with_retry(listener_fd) {
        Ok(fd) => fd,
        Err(e) => {
            error!("Giving up reading socket from: {path}, error: {e:?}");
            //cleanup
            if nix::unistd::close(listener_fd).is_ok() {
                nix::unistd::unlink(path).unwrap();
            }
            return Err(e);
        }
    };

    let mut io_vec = [IoSliceMut::new(payload); 1];
    let mut cmsg_buf = nix::cmsg_space!([RawFd; MAX_FDS]);
    let msg: RecvMsg<UnixAddr> = socket::recvmsg(
        fd,
        &mut io_vec,
        Some(&mut cmsg_buf),
        socket::MsgFlags::empty(),
    )
    .unwrap();

    let mut fds: Vec<RawFd> = Vec::new();
    for cmsg in msg.cmsgs() {
        if let socket::ControlMessageOwned::ScmRights(mut vec_fds) = cmsg {
            fds.append(&mut vec_fds)
        } else {
            warn!("Unexpected control messages: {cmsg:?}")
        }
    }

    //cleanup
    if nix::unistd::close(listener_fd).is_ok() {
        nix::unistd::unlink(path).unwrap();
    }

    Ok((fds, msg.bytes))
}

pub fn send_fds<P>(fds: Vec<RawFd>, payload: &[u8], path: &P) -> Result<usize, NixError>
where
    P: ?Sized + NixPath + std::fmt::Display,
{
    const MAX_NONBLOCKING_POLLS: usize = 20;
    const NONBLOCKING_POLL_INTERVAL: Duration = Duration::from_millis(500);

    let send_fd = socket::socket(
        AddressFamily::Unix,
        SockType::Stream,
        SockFlag::SOCK_NONBLOCK,
        None,
    )?;
    let unix_addr = UnixAddr::new(path)?;
    let mut retried = 0;
    let mut nonblocking_polls = 0;

    let conn_result: Result<usize, NixError> = loop {
        match socket::connect(send_fd, &unix_addr) {
            Ok(_) => break Ok(0),
            Err(e) => match e {
                /* If the new process hasn't created the upgrade sock we'll get an ENOENT.
                ECONNREFUSED may happen if the sock wasn't cleaned up
                and the old process tries sending before the new one is listening.
                EACCES may happen if connect() happen before the correct permission is set */
                Errno::ENOENT | Errno::ECONNREFUSED | Errno::EACCES => {
                    /*the server is not ready yet*/
                    retried += 1;
                    if retried > MAX_RETRY {
                        error!(
                            "Max retry: {} reached. Giving up sending socket to: {}, error: {:?}",
                            MAX_RETRY, path, e
                        );
                        break Err(e);
                    }
                    warn!("server not ready, will try again in {RETRY_INTERVAL:?}");
                    thread::sleep(RETRY_INTERVAL);
                }
                /* handle nonblocking IO */
                Errno::EINPROGRESS => {
                    nonblocking_polls += 1;
                    if nonblocking_polls >= MAX_NONBLOCKING_POLLS {
                        error!("Connect() not ready after retries when sending socket to: {path}",);
                        break Err(e);
                    }
                    warn!("Connect() not ready, will try again in {NONBLOCKING_POLL_INTERVAL:?}",);
                    thread::sleep(NONBLOCKING_POLL_INTERVAL);
                }
                _ => {
                    error!("Error sending socket to: {path}, error: {e:?}");
                    break Err(e);
                }
            },
        }
    };

    let result = match conn_result {
        Ok(_) => {
            let io_vec = [IoSlice::new(payload); 1];
            let scm = socket::ControlMessage::ScmRights(fds.as_slice());
            let cmsg = [scm; 1];
            loop {
                match socket::sendmsg(
                    send_fd,
                    &io_vec,
                    &cmsg,
                    socket::MsgFlags::empty(),
                    None::<&UnixAddr>,
                ) {
                    Ok(result) => break Ok(result),
                    Err(e) => match e {
                        /* handle nonblocking IO */
                        Errno::EAGAIN => {
                            nonblocking_polls += 1;
                            if nonblocking_polls >= MAX_NONBLOCKING_POLLS {
                                error!(
                                    "Sendmsg() not ready after retries when sending socket to: {}",
                                    path
                                );
                                break Err(e);
                            }
                            warn!(
                                "Sendmsg() not ready, will try again in {:?}",
                                NONBLOCKING_POLL_INTERVAL
                            );
                            thread::sleep(NONBLOCKING_POLL_INTERVAL);
                        }
                        _ => break Err(e),
                    },
                }
            }
        }
        Err(_) => conn_result,
    };

    nix::unistd::close(send_fd).unwrap();
    result
}
