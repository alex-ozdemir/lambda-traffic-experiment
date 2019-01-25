#[macro_use]
extern crate lambda_runtime as lambda;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate log;
extern crate bincode;
extern crate byteorder;
extern crate rand;
extern crate rusoto_core as aws;
extern crate rusoto_s3 as aws_s3;
extern crate simple_logger;
extern crate stun;

use aws_s3::S3;
use byteorder::{NativeEndian, WriteBytesExt};
use lambda::error::HandlerError;

use std::cell::RefCell;
use std::error::Error;
use std::fmt::Write as FmtWrite;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::time::{Duration, Instant};

mod consts;
mod msg;
mod net;

use msg::experiment::{RoundPlan, RoundSenderResults};
use msg::{LambdaResult, LambdaSenderStart, LocalMessage, SenderMessage};

const UDP_PAYLOAD_BYTES: usize = 1400;
const EXP_PLAN: [RoundPlan; 7] = [
    RoundPlan {
        round_index: 0,
        burst_period: Duration::from_millis(100),
        packets_per_burst: 1,
        duration: Duration::from_secs(5),
    },
    RoundPlan {
        round_index: 1,
        burst_period: Duration::from_millis(100),
        packets_per_burst: 3,
        duration: Duration::from_secs(5),
    },
    RoundPlan {
        round_index: 2,
        burst_period: Duration::from_millis(10),
        packets_per_burst: 1,
        duration: Duration::from_secs(5),
    },
    RoundPlan {
        round_index: 3,
        burst_period: Duration::from_millis(10),
        packets_per_burst: 3,
        duration: Duration::from_secs(5),
    },
    RoundPlan {
        round_index: 4,
        burst_period: Duration::from_millis(10),
        packets_per_burst: 10,
        duration: Duration::from_secs(5),
    },
    RoundPlan {
        round_index: 5,
        burst_period: Duration::from_millis(10),
        packets_per_burst: 20,
        duration: Duration::from_secs(5),
    },
    RoundPlan {
        round_index: 6,
        burst_period: Duration::from_millis(10),
        packets_per_burst: 30,
        duration: Duration::from_secs(5),
    },
];

thread_local! {
    pub static UDP_BUF: RefCell<[u8; consts::UDP_PAYLOAD_BYTES]> =
        RefCell::new([b'Q'; consts::UDP_PAYLOAD_BYTES]);
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Info)?;
    lambda!(my_handler);

    Ok(())
}

struct Sender {
    socket: UdpSocket,
    receiver_addr: SocketAddr,
    local_addr: SocketAddr,
    next_packet_id: u64,
}

impl Sender {
    fn new(start_command: &LambdaSenderStart) -> Result<Self, Box<dyn Error>> {
        info!("Staring with invocation {:?}", start_command);
        let (socket, my_addr) = net::open_public_udp();
        let mut tcp = TcpStream::connect((start_command.local_addr.ip(), net::TCP_PORT)).unwrap();
        tcp.write_all(
            bincode::serialize(&LocalMessage::SenderPing(my_addr))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        tcp.flush().unwrap();
        let receiver_addr = {
            let mut data = Vec::new();
            tcp.read_to_end(&mut data).unwrap();
            match bincode::deserialize(&data).unwrap() {
                SenderMessage::ReceiverAddr(addr) => (addr),
                _ => panic!("Expected the sender's address"),
            }
        };
        std::mem::drop(tcp);
        Ok(Sender {
            socket,
            receiver_addr,
            local_addr: start_command.local_addr,
            next_packet_id: 0,
        })
    }

    fn send_packet_to_receiver(&mut self) -> io::Result<usize> {
        UDP_BUF.with(|buf_cell| {
            let buf: &mut [u8; UDP_PAYLOAD_BYTES] = &mut buf_cell.borrow_mut();
            {
                let mut buf_writer: &mut [u8] = buf;
                buf_writer
                    .write_u64::<NativeEndian>(self.next_packet_id)
                    .unwrap();
            }
            self.next_packet_id += 1;
            self.socket.send_to(buf, self.receiver_addr)
        })
    }

    fn send_to_master(&mut self, msg: &LocalMessage) {
        let enc = bincode::serialize(msg).unwrap();
        self.socket.send_to(&enc, self.local_addr).unwrap();
    }

    fn run_round(&mut self, round_plan: &RoundPlan) -> RoundSenderResults {
        let end_time = Instant::now() + round_plan.duration;
        let first_packet_id = self.next_packet_id;
        let mut packets_sent = 0;
        let mut bytes_sent = 0;
        let mut errors = 0;

        while Instant::now() < end_time {
            let burst_start_time = Instant::now();
            for _i in 0..round_plan.packets_per_burst {
                match self.send_packet_to_receiver() {
                    Ok(bytes_sent_in_this_packet) => {
                        packets_sent += 1;
                        bytes_sent += bytes_sent_in_this_packet as u64;
                    }
                    Err(_) => {
                        errors += 1;
                    }
                }
            }
            let now = Instant::now();
            if now < burst_start_time + round_plan.burst_period {
                let sleep_time = round_plan.burst_period - (Instant::now() - burst_start_time);
                std::thread::sleep(sleep_time);
            }
        }
        RoundSenderResults {
            plan: round_plan.clone(),
            errors,
            first_packet_id,
            packets_sent,
            bytes_sent,
        }
    }

    fn run_exp(&mut self) -> Vec<RoundSenderResults> {
        EXP_PLAN
            .into_iter()
            .map(|round_plan| {
                self.send_to_master(&LocalMessage::StartRound(round_plan.clone()));
                let round_result = self.run_round(round_plan);
                std::thread::sleep(Duration::from_millis(200));
                self.send_to_master(&LocalMessage::FinishRound(round_result.clone()));
                std::thread::sleep(Duration::from_secs(2));
                round_result
            })
            .collect()
    }
}

fn format_results<'a>(results: impl Iterator<Item = &'a RoundSenderResults>) -> String {
    let mut formatted = String::new();
    write!(
        formatted,
        "{},{},{},{},{},{},{}\n",
        "round_index",
        "burst_period",
        "packets_per_burst",
        "duration",
        "first_packet_id",
        "packets_sent",
        "bytes_sent",
    )
    .unwrap();
    for res in results {
        write!(
            formatted,
            "{},{},{},{},{},{},{}\n",
            res.plan.round_index,
            res.plan.burst_period.as_nanos(),
            res.plan.packets_per_burst,
            res.plan.duration.as_nanos(),
            res.first_packet_id,
            res.packets_sent,
            res.bytes_sent
        )
        .unwrap();
    }
    formatted
}

fn my_handler(e: LambdaSenderStart, _c: lambda::Context) -> Result<LambdaResult, HandlerError> {
    info!("Lambda with event {:?} is alive", e);

    let exp_results = Sender::new(&e).unwrap().run_exp();

    let formatted_results = format_results(exp_results.iter());

    let s3client = aws_s3::S3Client::new(aws::Region::UsWest2);
    let object_name = format!("sender-results-{}.csv", e.exp_id);

    s3client
        .put_object(aws_s3::PutObjectRequest {
            bucket: consts::S3_BUCKET.to_owned(),
            key: object_name,
            body: Some(formatted_results.into_bytes().into()),
            ..aws_s3::PutObjectRequest::default()
        })
        .sync()
        .unwrap();

    Ok(LambdaResult {})
}
