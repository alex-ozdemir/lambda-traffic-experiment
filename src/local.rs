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

use msg::experiment::ExperimentPlan;
use msg::{LambdaReceiverStart, LambdaSenderStart, LocalMessage, LocalTCPMessage, SenderMessage};

struct Local {
    socket: UdpSocket,
    sender_addr: SocketAddr,
    running: Arc<AtomicBool>,
    exp_id: u32,
    sender_host: String,
    receiver_host: String,
}

impl Local {
    fn invoke_dummies(
        l_client: &mut aws_lambda::LambdaClient,
        my_addr: SocketAddr,
        sender_addr: SocketAddr,
        exp_id: u32,
    ) {
        for dummy_id in 0..50 {
            let start = LambdaReceiverStart {
                local_addr: my_addr,
                sender_addr,
                exp_id,
                dummy_id: Some(dummy_id),
            };
            let lambda_status = l_client
                .invoke(aws_lambda::InvocationRequest {
                    function_name: consts::RECEIVER_FUNCTION_NAME.to_owned(),
                    invocation_type: Some("Event".to_owned()),
                    payload: Some(serde_json::to_vec(&start).unwrap()),
                    ..aws_lambda::InvocationRequest::default()
                })
                .sync()
                .unwrap();
            assert!(lambda_status.status_code.unwrap() == 202);
            info!("Started dummy #{}", dummy_id);
        }
    }

    fn new() -> Result<Self, Box<dyn Error>> {
        let plan = ExperimentPlan::with_varying_counts(
            Duration::from_millis(5),
            Duration::from_secs(8),
            [20, 30, 40, 50, 60, 70, 80, 90, 100, 120].iter().cloned(),
        );
        let tcp_socket =
            TcpListener::bind(("0.0.0.0", net::TCP_PORT)).expect("Could not bind TCP socket");
        let exp_id: u32 = rand::random();
        info!("Starting experiment {}", exp_id);
        let (socket, my_addr) = net::open_public_udp();
        info!("Making lambda client");
        let mut client = aws_lambda::LambdaClient::new(aws::Region::UsWest2);

        let (sender_addr, mut sender_tcp, sender_host) = {
            info!("Starting sender");
            let start = LambdaSenderStart {
                local_addr: my_addr,
                exp_id,
                plan,
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
            let (mut stream, tcp_addr) = tcp_socket.accept().unwrap();
            info!("Sender TCP addr {:?}", tcp_addr);
            let mut data = [0; 1000];
            let bytes = stream.read(&mut data).expect("Failed to read from sender");
            info!("Sender contact");
            match bincode::deserialize(&data[..bytes]).unwrap() {
                LocalTCPMessage::SenderPing(addr, id) => (addr, stream, id),
                _ => panic!("Expected the sender's address"),
            }
        };

        Local::invoke_dummies(&mut client, my_addr, sender_addr, exp_id);

        let (receiver_addr, receiver_host) = {
            info!("Starting receiver");
            let start = LambdaReceiverStart {
                local_addr: my_addr,
                sender_addr,
                exp_id,
                dummy_id: None,
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
            let (mut stream, tcp_addr) = tcp_socket.accept().unwrap();
            info!("Receiver TCP addr {:?}", tcp_addr);
            let mut data = [0; 1000];
            let bytes = stream
                .read(&mut data)
                .expect("Failed to read from receiver");
            info!("Receiver contact");
            match bincode::deserialize(&data[..bytes]).unwrap() {
                LocalTCPMessage::ReceiverPing(addr, id) => (addr, id),
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

        if sender_host == receiver_host {
            warn!(
                "The sender and receiver both have hostname {}!",
                sender_host
            );
        } else {
            info!(
                "The lambda instances have hostnames {} and {}",
                sender_host, receiver_host
            );
        }

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
            sender_host,
            receiver_host,
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
                    LocalMessage::DummyPing(addr, dummy_id, machine_id) => {
                        info!(
                            "Dummy #{} on machine {} @ address {} pinged",
                            dummy_id, machine_id, addr
                        );
                    }
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
            info!(
                "Sender was {} and receiver was {}",
                self.sender_host, self.receiver_host
            );
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
