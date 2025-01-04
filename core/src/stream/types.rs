use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{self, AsyncRead, AsyncWrite, BufStream, ReadBuf};
use tokio::net::TcpStream;
use tokio::net::UnixStream;

use crate::listener::sys::{set_tcp_keepalive, TcpKeepAliveConfig};
use crate::pool::pool::ConnectionID;
use crate::stream::{duration::AccumulatedDuration, raw::RawStream, traits::UniqueID};

// Large read buffering helps reducing syscalls with little trade-off
// Ssl layer always does "small" reads in 16k (TLS record size) so L4 read buffer helps a lot.
const BUF_READ_SIZE: usize = 64 * 1024;

// Small write buf to match MSS. Too large write buf delays real time communication.
// This buffering effectively implements something similar to Nagle's algorithm.
// The benefit is that user space can control when to flush, where Nagle's can't be controlled.
// And userspace buffering reduce both syscalls and small packets.
const BUF_WRITE_SIZE: usize = 1460;

// NOTE: with writer buffering, users need to call flush() to make sure the data is actually
// sent. Otherwise data could be stuck in the buffer forever or get lost when stream is closed.

// A concrete type for the connection stream implementations.
#[derive(Debug)]
pub struct StreamType {
    stream: BufStream<RawStream>,
    rewind_read_buf: Vec<u8>,
    buffer_write: bool,
    read_pending_time: AccumulatedDuration,
    write_pending_time: AccumulatedDuration,
}

impl StreamType {
    // only works if the stream type is tcp, otherwise do nothing
    pub fn set_no_delay(&mut self) {
        if let RawStream::Tcp(stream) = &self.stream.get_mut() {
            if let Err(e) = stream.set_nodelay(true) {
                panic!("{}", e);
            }
        }
    }

    // only works if stream type is tcp, otherwise do nothing
    pub fn set_keepalive(&mut self, config: &TcpKeepAliveConfig) {
        if let RawStream::Tcp(stream) = &self.stream.get_mut() {
            let fd = stream.as_raw_fd();
            // calling from sys utils: providing low level socket optimization
            if let Err(e) = set_tcp_keepalive(fd, config) {
                panic!("{}", e);
            }
        }
    }

    // set the buffer write flag, default set to true
    pub fn set_buf_write(&mut self, flag: bool) {
        self.buffer_write = flag
    }
}

// stream type implementation from tcp stream
impl From<TcpStream> for StreamType {
    fn from(tcp_stream: TcpStream) -> Self {
        StreamType {
            stream: BufStream::with_capacity(
                BUF_READ_SIZE,
                BUF_WRITE_SIZE,
                RawStream::Tcp(tcp_stream),
            ),
            rewind_read_buf: Vec::new(),
            buffer_write: true,
            read_pending_time: AccumulatedDuration::new(),
            write_pending_time: AccumulatedDuration::new(),
        }
    }
}

// stream type implementation from unix stream
impl From<UnixStream> for StreamType {
    fn from(unix_stream: UnixStream) -> Self {
        StreamType {
            stream: BufStream::with_capacity(
                BUF_READ_SIZE,
                BUF_WRITE_SIZE,
                RawStream::Unix(unix_stream),
            ),
            rewind_read_buf: Vec::new(),
            buffer_write: true,
            read_pending_time: AccumulatedDuration::new(),
            write_pending_time: AccumulatedDuration::new(),
        }
    }
}

// as raw fd (file descriptor) implementation for raw stream
// this used to extract file descriptors from raw stream
// returns file descriptor
impl AsRawFd for StreamType {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        self.stream.get_ref().as_raw_fd()
    }
}

// unique implementation using file descriptors retrival
impl UniqueID for StreamType {
    fn get_unique_id(&self) -> ConnectionID {
        self.as_raw_fd()
    }
}

// async read implementation for stream type
impl AsyncRead for StreamType {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let result = if !self.rewind_read_buf.is_empty() {
            let mut data_to_read = self.rewind_read_buf.as_slice();
            let result = Pin::new(&mut data_to_read).poll_read(cx, buf);
            let remaining_buf = Vec::from(data_to_read);
            let _ = std::mem::replace(&mut self.rewind_read_buf, remaining_buf);
            result
        } else {
            Pin::new(&mut self.stream).poll_read(cx, buf)
        };
        self.read_pending_time.poll_time(&result);
        result
    }
}

// async write implementation for stream type
impl AsyncWrite for StreamType {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = if self.buffer_write {
            Pin::new(&mut self.stream).poll_write(cx, buf)
        } else {
            Pin::new(&mut self.stream.get_mut()).poll_write(cx, buf)
        };
        self.write_pending_time.poll_write_time(&result, buf.len());
        result
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let total_size = bufs.iter().fold(0, |acc, s| acc + s.len());
        let result = if self.buffer_write {
            Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
        } else {
            Pin::new(&mut self.stream.get_mut()).poll_write_vectored(cx, bufs)
        };
        self.write_pending_time.poll_write_time(&result, total_size);
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.stream).poll_flush(cx);
        self.write_pending_time.poll_time(&result);
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        if self.buffer_write {
            self.stream.is_write_vectored()
        } else {
            self.stream.get_ref().is_write_vectored()
        }
    }
}
