#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate rusoto_core as aws;
extern crate rusoto_lambda as aws_lambda;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate stun;

use std::error::Error;
use std::io::{Read, Write};
use std::net::{TcpListener, UdpSocket};
use std::time::Duration;

use aws_lambda::Lambda;

mod consts;
mod msg;
mod net;

use msg::{LambdaSenderStart, LambdaReceiverStart, LocalMessage, SenderMessage};

struct Local {
    socket: UdpSocket,
}

impl Local {
    fn new() -> Result<Self, Box<dyn Error>> {
        let tcp_socket = TcpListener::bind(("0.0.0.0", net::TCP_PORT)).expect("Could not bind TCP socket");
        info!("Starting Local::new");
        let (socket, my_addr) = net::open_public_udp();
        info!("Making lambda client");
        let client = aws_lambda::LambdaClient::new(aws::Region::UsWest2);

        let (sender_addr, mut sender_tcp) = {
            info!("Starting sender");
            let start = LambdaSenderStart {
                local_addr: my_addr,
            };
            let lambda_status = client
                .invoke(aws_lambda::InvocationRequest {
                    function_name: consts::SENDER_FUNCTION_NAME.to_owned(),
                    invocation_type: Some("Event".to_owned()),
                    payload: Some(serde_json::to_vec(&start).unwrap()),
                    ..aws_lambda::InvocationRequest::default()
                })
            .sync()?;
            assert!(lambda_status.status_code.unwrap() == 202);
            info!("Waiting for sender");
            let (mut stream, _) = tcp_socket.accept().unwrap();
            let mut data = Vec::new();
            stream.read_to_end(&mut data).expect("Failed to read from sender");
            info!("Sender contact");
            match bincode::deserialize(&data).unwrap() {
                LocalMessage::SenderPing(addr) => (addr, stream),
                _ => panic!("Expected the sender's address"),
            }
        };

        let receiver_addr = {
            info!("Starting receiver");
            let start = LambdaReceiverStart {
                local_addr: my_addr,
                sender_addr,
            };
            let lambda_status = client
                .invoke(aws_lambda::InvocationRequest {
                    function_name: consts::RECEIVER_FUNCTION_NAME.to_owned(),
                    invocation_type: Some("Event".to_owned()),
                    payload: Some(serde_json::to_vec(&start).unwrap()),
                    ..aws_lambda::InvocationRequest::default()
                })
            .sync()?;
            assert!(lambda_status.status_code.unwrap() == 202);
            info!("Waiting for receiver");
            let (mut stream, _) = tcp_socket.accept().unwrap();
            let mut data = Vec::new();
            stream.read_to_end(&mut data).expect("Failed to read from receiver");
            info!("Receiver contact");
            match bincode::deserialize(&data).unwrap() {
                LocalMessage::ReceiverPing(addr) => addr,
                _ => panic!("Expected the sender's address"),
            }
        };

        sender_tcp.write_all(bincode::serialize(&SenderMessage::ReceiverAddr(receiver_addr)).unwrap().as_slice()).expect("Could not write receiver_addr to sender");
        std::mem::drop(sender_tcp);

        Ok(Local{
            socket,
        })
    }

    fn poll_socket(&mut self) {
        let mut udp_buf = [0; 65_507];
        match self.socket.recv_from(&mut udp_buf) {
            Err(e) => assert!(
                e.kind() == std::io::ErrorKind::WouldBlock,
                "Socket error {:?}",
                e
                ),
                Ok((size, _source_addr)) => {
                    let message = bincode::deserialize(&udp_buf[..size])
                        .expect("Couldn't parse message from worker");
                    match message {
                        LocalMessage::SenderPing(_) =>
                            unimplemented!(),
                        LocalMessage::ReceiverPing(_) =>
                            unimplemented!(),
                        LocalMessage::ProgressMessage(m) => {
                            info!("Progress: {}", m);
                        }
                    }
                }
        }
    }

    fn run_forever(&mut self) {
        loop {
            self.poll_socket();
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    Local::new()?.run_forever();

    Ok(())
}
