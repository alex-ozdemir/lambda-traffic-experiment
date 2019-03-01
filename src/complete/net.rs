#![allow(dead_code)]

use std::net::{SocketAddr, TcpStream, UdpSocket};

use std::io::{self, Read, Write};

use serde::de::DeserializeOwned;
use serde::ser::Serialize;

/// Create a UDP socker and determine it's public `SocketAddr`.
pub fn open_public_udp(port: u16) -> (UdpSocket, SocketAddr) {
    let socket = UdpSocket::bind(("0.0.0.0", port)).unwrap();
    let stun_addr = ("stun4.l.google.com", 19302);
    // TODO: There are a few Vec<u8>'s in here that should be &[u8].
    // This library forces that, but it's stupid and we should write this ourselves or find a
    // different library.
    for _i in 0..3 {
        socket
            .send_to(&stun::Message::request().encode(), stun_addr)
            .expect("Could not send request to STUN server");
        let mut buf = [0; 512];
        let (bytes, _) = socket
            .recv_from(&mut buf)
            .expect("No response from STUN server");
        let response = stun::Message::decode(buf[..bytes].to_vec());
        for attr in response.attributes {
            match attr {
                stun::Attribute::XorMappedAddress(stun::XorMappedAddress(addr)) => {
                    socket.set_nonblocking(true).unwrap();
                    return (socket, addr);
                }
                _ => {}
            }
        }
    }
    panic!();
}

pub struct TcpMsgStream {
    inner: TcpStream,
    buf: Vec<u8>,
    size: usize,
}

#[derive(Debug)]
pub enum ReadResult<T> {
    WouldBlock,
    EOF,
    Data(T),
}

impl TcpMsgStream {
    pub fn new(inner: TcpStream) -> Self {
        inner.set_nonblocking(true).unwrap();
        let mut buf = Vec::new();
        buf.resize(10000, 0);
        Self {
            inner,
            buf,
            size: 0,
        }
    }

    // Returns `io::ErrorKind::WouldBlock` if no data has been received, and none is available.
    // Returns `false` if the stream is closed
    // Otherwise return `true`, and the size is in the buf.
    // If `block` is true, then will never return WouldBlock.
    fn read_at_least(&mut self, target: usize, block: bool) -> io::Result<bool> {
        if block {
            self.inner.set_nonblocking(false).unwrap();
        }
        if self.buf.len() < target {
            self.buf.resize(target, 0);
        }
        while self.size < target {
            match self.inner.read(&mut self.buf[self.size..]) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        if self.size == 0 {
                            self.inner.set_nonblocking(true).unwrap();
                            return Err(e);
                        } else {
                            continue;
                        }
                    } else {
                        self.inner.set_nonblocking(true).unwrap();
                        return Err(e);
                    }
                }
                Ok(0) => {
                    self.inner.set_nonblocking(true).unwrap();
                    return Ok(false)
                }
                Ok(n) => {
                    self.size += n;
                }
            }
        }
        self.inner.set_nonblocking(true).unwrap();
        return Ok(true);
    }

    fn clear_bytes(&mut self, n: usize) {
        let to_clear = std::cmp::min(n, self.size);
        self.buf.drain(..to_clear);
        self.size -= to_clear;
    }

    pub fn read<D: DeserializeOwned>(&mut self, block: bool) -> Result<ReadResult<D>, String> {
        use byteorder::ReadBytesExt;
        // Get the message size
        match self.read_at_least(8, block) {
            Ok(false) => return Ok(ReadResult::EOF),
            Ok(true) => {}
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    return Ok(ReadResult::WouldBlock);
                } else {
                    return Err(format!("Error getting message size: {}", e));
                }
            }
        }
        let size = (&self.buf[..8])
            .read_u64::<byteorder::NetworkEndian>()
            .map_err(|e| format!("Error parsing message size: {}", e))? as usize;
        self.clear_bytes(8);
        // Get the message
        if !self
            .read_at_least(size, true)
            .map_err(|e| format!("Error getting message: {}", e))?
        {
            return Ok(ReadResult::EOF);
        }
        let msg = bincode::deserialize(&self.buf[..size])
            .map_err(|e| format!("Error parsing message: {}", e))?;
        self.clear_bytes(size);
        Ok(ReadResult::Data(msg))
    }

    pub fn send<S: Serialize>(&mut self, msg: &S) -> io::Result<()> {
        use byteorder::WriteBytesExt;
        // Get the message size
        let mut data = Vec::new();
        let s = bincode::serialized_size(msg).unwrap() as u64;
        data.write_u64::<byteorder::NetworkEndian>(s)
            .unwrap();
        bincode::serialize_into(&mut data, msg).unwrap();
        self.inner.set_nonblocking(false).unwrap();
        self.inner.write_all(data.as_slice())?;
        self.inner.flush()?;
        self.inner.set_nonblocking(true).unwrap();
        Ok(())
    }
}
