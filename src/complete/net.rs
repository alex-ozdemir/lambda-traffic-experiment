#![allow(dead_code)]

use std::net::{SocketAddr, UdpSocket};

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
