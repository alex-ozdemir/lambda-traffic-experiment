#[macro_use]
extern crate lambda_runtime as lambda;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate log;
extern crate bincode;
extern crate byteorder;
extern crate futures;
extern crate rusoto_core as aws;
extern crate rusoto_s3 as aws_s3;
extern crate simple_logger;
extern crate stun;

use aws_s3::S3;
use byteorder::{NativeEndian, ReadBytesExt};
use lambda::error::HandlerError;

use std::cell::RefCell;
use std::error::Error;
use std::io::Write;
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::time::{Duration, Instant};

mod consts;
mod msg;
mod net;

use msg::{LambdaReceiverStart, LambdaResult, LocalTCPMessage, LocalMessage, SenderMessage};

thread_local! {
    pub static UDP_BUF: RefCell<[u8; 5_000]> = RefCell::new([0; 5_000]);
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Info)?;
    lambda!(my_handler);

    Ok(())
}

struct Receiver {
    socket: UdpSocket,
    sender_addrs: Vec<SocketAddr>,
    next_sender_ping: Instant,
    last_packet_time: Option<Instant>,
    local_addr: SocketAddr,
    received_packet_ids: Vec<u64>,
    received_bytes: u64,
    errors: u64,
    exp_id: u32,
}

impl Receiver {
    fn new(start_command: LambdaReceiverStart) -> Result<Self, Box<dyn Error>> {
        info!("Staring with invocation {:?}", start_command);
        let (socket, my_addr) = net::open_public_udp();
        let my_machine_id = net::get_machine_id();
        let mut tcp = TcpStream::connect((start_command.local_addr.ip(), net::TCP_PORT)).unwrap();
        tcp.write_all(
            bincode::serialize(&LocalTCPMessage::ReceiverPing(my_addr, my_machine_id))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        std::mem::drop(tcp);
        Ok(Receiver {
            socket,
            sender_addrs: start_command.sender_addrs,
            next_sender_ping: Instant::now(),
            local_addr: start_command.local_addr,
            received_packet_ids: Vec::new(),
            received_bytes: 0,
            errors: 0,
            last_packet_time: None,
            exp_id: start_command.exp_id,
        })
    }

    fn ping_sender_if_needed(&mut self) {
        if Instant::now() > self.next_sender_ping {
            info!("Sending data");
            info!("Pinging senders {:?}", self.sender_addrs);
            for sender_addr in &self.sender_addrs {
                let enc = bincode::serialize(&SenderMessage::ReceiverPing).unwrap();
                self.socket.send_to(&enc, *sender_addr).unwrap();
            }
            info!("Pinging local {}", self.local_addr);
            let enc = bincode::serialize(&LocalMessage::ReceiverStats {
                packet_count: self.received_packet_ids.len() as u64,
                byte_count: self.received_bytes,
                errors: self.errors,
            })
            .unwrap();
            self.socket.send_to(&enc, self.local_addr).unwrap();
            self.next_sender_ping = Instant::now() + consts::FIREWALL_HEAL_TIME;
        }
    }

    fn poll_socket(&mut self) {
        UDP_BUF.with(|buf_cell| {
            let udp_buf: &mut [u8; 5000] = &mut buf_cell.borrow_mut();
            match self.socket.recv_from(udp_buf) {
                Err(e) => assert!(
                    e.kind() == std::io::ErrorKind::WouldBlock,
                    "Socket error {:?}",
                    e
                ),
                Ok((size, source_addr)) => {
                    if size == consts::UDP_PAYLOAD_BYTES {
                        self.last_packet_time = Some(Instant::now());
                        let id = (&udp_buf[..size]).read_u64::<NativeEndian>().unwrap();
                        self.received_packet_ids.push(id);
                        self.received_bytes += size as u64;
                    } else {
                        self.errors += 1;
                        warn!(
                            "Packet received from {} with wrong payload size {}",
                            source_addr, size
                        );
                    }
                }
            }
        })
    }

    fn run_until(&mut self, stop_time: Instant) {
        while Instant::now() < stop_time {
            if let Some(t) = self.last_packet_time {
                if Instant::now() > t + Duration::from_secs(10) {
                    break;
                }
            }
            for _i in 0..10000 {
                self.poll_socket();
            }
            self.ping_sender_if_needed();
        }
    }

    fn save_results(&mut self) {
        info!("Going to save the results");
        let s3client = aws_s3::S3Client::new(aws::Region::UsWest2);
        let object_name = format!("receiver-results-{}.csv", self.exp_id);
        let data = std::mem::replace(&mut self.received_packet_ids, Vec::new());
        let mut s = Vec::<u8>::new();
        write!(s, "packet_id\n").unwrap();
        for i in data {
            write!(s, "{}\n", i).unwrap();
        }

        s3client
            .put_object(aws_s3::PutObjectRequest {
                bucket: consts::S3_BUCKET.to_owned(),
                key: object_name,
                body: Some(s.into()),
                ..aws_s3::PutObjectRequest::default()
            })
            .sync()
            .unwrap();
        info!("Done saving");
    }
}

fn my_handler(e: LambdaReceiverStart, _c: lambda::Context) -> Result<LambdaResult, HandlerError> {
    info!("Lambda with event {:?} is alive", e);

    if let Some(id) = e.dummy_id {
        let (socket, my_addr) = net::open_public_udp();
        let my_machine_id = net::get_machine_id();
        let enc = bincode::serialize(&LocalMessage::DummyPing(my_addr, id, my_machine_id)).unwrap();
        socket.send_to(&enc, e.local_addr).unwrap();

        //std::thread::sleep(Duration::from_secs(20));

        return Ok(LambdaResult {});
    }

    let stop_time =
        Instant::now() + Duration::from_millis(_c.get_time_remaining_millis() as u64 - 10_000);

    let mut receiver = Receiver::new(e).unwrap();

    receiver.run_until(stop_time);

    receiver.save_results();

    Ok(LambdaResult {})
}
