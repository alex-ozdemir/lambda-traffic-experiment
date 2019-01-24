#[macro_use]
extern crate lambda_runtime as lambda;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate log;
extern crate bincode;
extern crate byteorder;
extern crate rusoto_core as aws;
extern crate rusoto_s3 as aws_s3;
extern crate simple_logger;
extern crate stun;

use aws_s3::S3;
use lambda::error::HandlerError;

use std::cell::RefCell;
use std::error::Error;
use std::io::{Read,Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::time::{Duration, Instant};

mod msg;
mod net;

use msg::{LambdaSenderStart, LocalMessage, SenderMessage, LambdaResult};
use msg::experiment::{RoundPlan};

const UDP_PAYLOAD_BYTES: usize = 508;
const EXP_PLAN: [RoundPlan; 1] = [
    RoundPlan {
        round_i: 0,
        interval: Duration::from_millis(10),
        packets_per_interval: 1,
        duration: Duration::from_secs(2),
    }
];

thread_local!{
    pub static UDP_BUF: RefCell<[u8; UDP_PAYLOAD_BYTES]> = RefCell::new([0; UDP_PAYLOAD_BYTES]);
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Info)?;
    lambda!(my_handler);

    Ok(())
}

struct Sender {
    socket: UdpSocket,
    receiver_addr: SocketAddr,
    next_packet_id: usize,
}

impl Sender {
    fn new(start_command: LambdaSenderStart) -> Result<Self, Box<dyn Error>> {
        info!("Staring with invocation {:?}", start_command);
        let (socket, my_addr) = net::open_public_udp();
        let mut tcp = TcpStream::connect(start_command.local_addr).unwrap();
        tcp.write_all(
            bincode::serialize(&LocalMessage::SenderPing(my_addr))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
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
            next_packet_id: 0,
        })
    }

    fn send_packet(&mut self) {
        UDP_BUF.with(|buf_cell| {
            let buf: &mut [u8; UDP_PAYLOAD_BYTES] = &mut buf_cell.borrow_mut();
        });
    }

    fn send_to_master(&mut self, msg: &LocalMessage) {
        let enc = bincode::serialize(msg).unwrap();
        self.socket.send_to(self.socket, &enc).unwrap();
    }

    fn run_round(&mut self, round_plan: &RoundPlan) {
        let end_time = Instant::now() + round_plan.duration;

        while Instant::now() < end_time {

        }
    }

    fn run_exp(&mut self) {
        for ref round_plan in &EXP_PLAN {
            self.send_to_master(&LocalMessage::StartRound(round_plan.clone()));
            self.run_round(round_plan);
            std::thread::sleep(Duration::from_millis(200));
            self.send_to_master(&LocalMessage::FinishRound);
            std::thread::sleep(Duration::from_secs(2));
        }
    }
}

fn my_handler(e: LambdaSenderStart, _c: lambda::Context) -> Result<LambdaResult, HandlerError> {
    info!("Lambda with event {:?} is alive", e);

    Sender::new(e).unwrap().run_exp();

    Ok(LambdaResult {})
}
