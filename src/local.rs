extern crate ctrlc;
#[macro_use]
extern crate log;
extern crate rand;
extern crate rusoto_core as aws;
extern crate rusoto_lambda as aws_lambda;
extern crate simple_logger;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate stun;

use std::error::Error;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aws_lambda::Lambda;

mod consts;
mod msg;
mod net;

use msg::{LambdaReceiverStart, LambdaSenderStart, LocalMessage, SenderMessage};

struct Local {
    socket: UdpSocket,
    sender_addr: SocketAddr,
    running: Arc<AtomicBool>,
    exp_id: u32,
}

impl Local {
    fn new() -> Result<Self, Box<dyn Error>> {
        let tcp_socket =
            TcpListener::bind(("0.0.0.0", net::TCP_PORT)).expect("Could not bind TCP socket");
        let exp_id: u32 = rand::random();
        info!("Starting experiment {}", exp_id);
        let (socket, my_addr) = net::open_public_udp();
        info!("Making lambda client");
        let client = aws_lambda::LambdaClient::new(aws::Region::UsWest2);

        let (sender_addr, mut sender_tcp) = {
            info!("Starting sender");
            let start = LambdaSenderStart {
                local_addr: my_addr,
                exp_id,
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
            let mut data = [0; 1000];
            let bytes = stream.read(&mut data).expect("Failed to read from sender");
            info!("Sender contact, {}", bytes);
            match bincode::deserialize(&data[..bytes]).unwrap() {
                LocalMessage::SenderPing(addr) => (addr, stream),
                _ => panic!("Expected the sender's address"),
            }
        };

        let receiver_addr = {
            info!("Starting receiver");
            let start = LambdaReceiverStart {
                local_addr: my_addr,
                sender_addr,
                exp_id,
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
            let mut data = [0; 1000];
            let bytes = stream
                .read(&mut data)
                .expect("Failed to read from receiver");
            info!("Receiver contact, {}", bytes);
            match bincode::deserialize(&data[..bytes]).unwrap() {
                LocalMessage::ReceiverPing(addr) => addr,
                _ => panic!("Expected the receiver's address"),
            }
        };

        sender_tcp
            .write_all(
                bincode::serialize(&SenderMessage::ReceiverAddr(receiver_addr))
                    .unwrap()
                    .as_slice(),
            )
            .expect("Could not write receiver_addr to sender");
        sender_tcp.flush().unwrap();
        std::mem::drop(sender_tcp);

        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        ctrlc::set_handler(move || {
            r.store(false, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");

        Ok(Local {
            socket,
            running,
            sender_addr,
            exp_id,
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
                    LocalMessage::SenderPing(_) => unimplemented!(),
                    LocalMessage::ReceiverPing(_) => unimplemented!(),
                    LocalMessage::StartRound(plan) => {
                        info!("Starting round: {:#?}", plan);
                    }
                    LocalMessage::FinishRound(result) => {
                        info!("Finished round: {:#?}", result);
                    }
                    LocalMessage::ReceiverStats {
                        byte_count,
                        packet_count,
                        errors,
                    } => {
                        info!(
                            "Received{:12} bytes in{:9} packets.{:9} errors.",
                            byte_count, packet_count, errors,
                        );
                    }
                }
            }
        }
    }

    fn maybe_die(&mut self) {
        if !self.running.load(Ordering::SeqCst) {
            self.socket
                .send_to(
                    bincode::serialize(&SenderMessage::Die).unwrap().as_slice(),
                    self.sender_addr,
                )
                .unwrap();
            info!("Experiment {} conluding", self.exp_id);
            std::process::exit(2)
        }
    }

    fn run_forever(&mut self) {
        loop {
            self.maybe_die();
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
