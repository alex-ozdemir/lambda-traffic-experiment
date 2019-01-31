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

use std::error::Error;
use std::fmt::Write as FmtWrite;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::cell::RefCell;
use std::time::{Duration, Instant};

mod consts;
mod msg;
mod net;

use msg::experiment::{ExperimentPlan, RoundPlan, RoundSenderResults};
use msg::{LambdaResult, LambdaSenderStart, LocalMessage, LocalTCPMessage, SenderMessage};

thread_local! {
    pub static UDP_BUF: RefCell<[u8; consts::UDP_PAYLOAD_BYTES]> = RefCell::new([b'Q'; consts::UDP_PAYLOAD_BYTES]);
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Info)?;
    lambda!(my_handler);

    Ok(())
}

struct Sender {
    socket: UdpSocket,
    receiver_addrs: Vec<SocketAddr>,
    local_addr: SocketAddr,
    next_packet_id: u64,
    plan: ExperimentPlan,
}

impl Sender {
    fn new(start_command: &LambdaSenderStart) -> Result<Self, Box<dyn Error>> {
        info!("Staring with invocation {:?}", start_command);
        let (socket, my_addr) = net::open_public_udp();
        let my_machine_id = net::get_machine_id();
        let mut tcp = TcpStream::connect((start_command.local_addr.ip(), net::TCP_PORT)).unwrap();
        tcp.write_all(
            bincode::serialize(&LocalTCPMessage::SenderPing(my_addr, my_machine_id))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        tcp.flush().unwrap();
        let receiver_addrs = {
            let mut data = Vec::new();
            tcp.read_to_end(&mut data).unwrap();
            match bincode::deserialize(&data).unwrap() {
                SenderMessage::ReceiverAddrs(addrs) => (addrs),
                _ => panic!("Expected the senders' addresses"),
            }
        };
        assert!(receiver_addrs.len() > 0);
        std::mem::drop(tcp);
        Ok(Sender {
            socket,
            receiver_addrs,
            local_addr: start_command.local_addr,
            next_packet_id: start_command.first_packet_id,
            plan: start_command.plan.clone(),
        })
    }

    fn send_packet_to_receiver(&mut self) -> io::Result<usize> {
        let receiver_addr = self.receiver_addrs[(self.next_packet_id % self.receiver_addrs.len() as u64) as usize];
        UDP_BUF.with(|b| {
            let udp_buf: &mut [u8; consts::UDP_PAYLOAD_BYTES] = &mut b.borrow_mut();
            let mut buf_writer: &mut [u8] = udp_buf;
            buf_writer
                .write_u64::<NativeEndian>(self.next_packet_id)
                .unwrap();
            self.next_packet_id += 1;
            self.socket.send_to(udp_buf, receiver_addr)
        })
    }

    fn send_to_master(&mut self, msg: &LocalMessage) {
        let enc = bincode::serialize(msg).unwrap();
        self.socket.send_to(&enc, self.local_addr).unwrap();
    }

    fn run_round(&mut self, round_plan: &RoundPlan) -> RoundSenderResults {
        let round_start_time = Instant::now();
        let first_packet_id = self.next_packet_id;
        let mut packets_sent = 0;
        let mut bytes_sent = 0;
        let mut would_blocks = 0;
        let mut errors = 0;
        let mut sleep_time = Duration::from_nanos(0);
        let mut write_time = Duration::from_nanos(0);

        let mut burst_start_time = Instant::now();
        'round_loop: loop {
            let elapsed_time = burst_start_time - round_start_time;
            if elapsed_time >= round_plan.duration {
                break;
            }

            let packet_count_target =
                elapsed_time.as_millis() as u64 * round_plan.packets_per_ms as u64;
            let packets_behind = packet_count_target - packets_sent;
            for _i in 0..packets_behind {
                match self.send_packet_to_receiver() {
                    Ok(bytes_sent_in_this_packet) => {
                        packets_sent += 1;
                        bytes_sent += bytes_sent_in_this_packet as u64;
                    }
                    Err(e) => match e.kind() {
                        ErrorKind::WouldBlock => {
                            would_blocks += 1;
                        }
                        _ => {
                            errors += 1;
                        }
                    },
                }
                if _i % 1024 == 0 {
                    let now = Instant::now();
                    let elapsed = now - round_start_time;
                    if elapsed >= round_plan.duration {
                        write_time += now - burst_start_time;
                        break 'round_loop;
                    }
                }
            }
            let burst_end_time = Instant::now();
            write_time += burst_end_time - burst_start_time;

            std::thread::sleep(round_plan.sleep_period);
            burst_start_time = Instant::now();
            sleep_time += burst_start_time - burst_end_time;
        }

        RoundSenderResults {
            plan: round_plan.clone(),
            first_packet_id,
            packets_sent,
            bytes_sent,
            would_blocks,
            errors,
            sleep_time,
            write_time,
        }
    }

    fn run_exp(&mut self) -> Vec<RoundSenderResults> {
        std::mem::replace(&mut self.plan.rounds, Vec::new())
            .into_iter()
            .map(|round_plan| {
                let round_start_time = Instant::now();
                self.send_to_master(&LocalMessage::StartRound(round_plan.clone()));
                let round_result = self.run_round(&round_plan);
                std::thread::sleep(Duration::from_millis(200));
                self.send_to_master(&LocalMessage::FinishRound(round_result.clone()));
                let sleep_time = round_plan.duration + Duration::from_secs(2)
                    - (Instant::now() - round_start_time);
                std::thread::sleep(sleep_time);
                round_result
            })
            .collect()
    }
}

fn format_results<'a>(
    results: impl Iterator<Item = &'a RoundSenderResults>,
    sender_id: u8,
) -> String {
    let mut formatted = String::new();
    write!(
        formatted,
        "{},{},{},{},{},{},{},{},{},{},{}\n",
        "sleep_period",
        "packets_per_ms",
        "sender_id",
        "duration",
        "first_packet_id",
        "packets_sent",
        "bytes_sent",
        "sleep_time",
        "write_time",
        "errors",
        "would_blocks",
    )
    .unwrap();
    for res in results {
        write!(
            formatted,
            "{},{},{},{},{},{},{},{},{},{},{}\n",
            res.plan.sleep_period.as_nanos(),
            res.plan.packets_per_ms,
            sender_id,
            res.plan.duration.as_nanos(),
            res.first_packet_id,
            res.packets_sent,
            res.bytes_sent,
            res.sleep_time.as_nanos(),
            res.write_time.as_nanos(),
            res.errors,
            res.would_blocks,
        )
        .unwrap();
    }
    formatted
}

fn my_handler(e: LambdaSenderStart, _c: lambda::Context) -> Result<LambdaResult, HandlerError> {
    info!("Lambda with event {:?} is alive", e);

    let exp_results = Sender::new(&e).unwrap().run_exp();

    let formatted_results = format_results(exp_results.iter(), e.sender_id);

    let s3client = aws_s3::S3Client::new(aws::Region::UsWest2);
    let object_name = format!(
        "sender-results-{}-{}-of-{}.csv",
        e.exp_id, e.sender_id, e.n_senders
    );

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
