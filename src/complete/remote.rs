#[macro_use]
extern crate lambda_runtime as lambda;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate log;
extern crate bincode;
extern crate rand;
extern crate simple_logger;
extern crate stun;

use lambda::error::HandlerError;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::error::Error;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::time::{Duration, Instant};

mod msg;
mod net;

use msg::experiment::{ExperimentPlan, RoundReceiverResults, RoundSenderResults};
use msg::{LambdaResult, LambdaStart, LocalTcpMessage, RemoteId, RemoteTcpMessage, RoundId};
pub const UDP_PAYLOAD_BYTES: usize = 1372; // For an IP packet of size 1400

thread_local! {
    pub static UDP_OUT_BUF: RefCell<[u8; UDP_PAYLOAD_BYTES]> = RefCell::new([b'Q'; UDP_PAYLOAD_BYTES]);
    pub static UDP_IN_BUF: RefCell<[u8; UDP_PAYLOAD_BYTES]> = RefCell::new([b'Q'; UDP_PAYLOAD_BYTES]);
}

#[derive(Deserialize, Serialize, Debug, Clone)]
enum Msg {
    Ping {
        from: RemoteId,
        to: RemoteId,
    },
    Packet {
        from: RemoteId,
        to: RemoteId,
        round: RoundId,
    },
}

fn noneify_would_block<T>(e: io::Error) -> io::Result<Option<T>> {
    if e.kind() == io::ErrorKind::WouldBlock {
        Ok(None)
    } else {
        Err(e)
    }
}

fn mk_err(m: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, m)
}

fn write(buf: &mut [u8; UDP_PAYLOAD_BYTES], msg: &Msg) {
    let buf_writer: &mut [u8] = buf;
    bincode::serialize_into(buf_writer, &msg).unwrap();
}

fn read(buf: &[u8]) -> io::Result<Msg> {
    let buf_reader: &[u8] = &buf;
    bincode::deserialize_from(buf_reader).map_err(|e| mk_err(format!("deser error: {}", e)))
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Info)?;
    lambda!(my_handler);

    Ok(())
}

struct Remote {
    socket: UdpSocket,
    other_addrs: Vec<(RemoteId, SocketAddr)>,
    confirmed_peers: BTreeSet<RemoteId>,
    local_connection: TcpStream,
    round_id: RoundId,
    remote_id: RemoteId,
    plan: ExperimentPlan,
    send_results: RoundSenderResults,
    receive_results: RoundReceiverResults,
    all_send_results: Vec<RoundSenderResults>,
    all_receive_results: Vec<RoundReceiverResults>,
    state: RemoteState,
    round_start: Instant,
    round_end: Instant,
    packets_this_round: u64,
    rate: u16,
}

#[derive(Debug)]
enum RemoteState {
    Establish,
    WaitForStart,
    Receive,
    Send,
}

impl Remote {
    fn new(start: LambdaStart, c: lambda::Context) -> Self {
        info!("Staring with invocation {:?}", start);
        let (socket, my_addr) = net::open_public_udp(start.port);
        let mut tcp = TcpStream::connect(start.local_addr).unwrap();
        tcp.write_all(
            bincode::serialize(&LocalTcpMessage::MyAddress(
                my_addr,
                start.remote_id,
                c.log_stream_name,
            ))
            .unwrap()
            .as_slice(),
        )
        .unwrap();
        tcp.flush().unwrap();

        let mut other_addrs = {
            let mut data = [b' '; 100000];
            let mut bytes = 0;
            loop {
                bytes += tcp.read(&mut data[bytes..]).unwrap();
                match bincode::deserialize(&data[..bytes]) {
                    Ok(RemoteTcpMessage::AllAddrs(addrs)) => {
                        break addrs;
                    }
                    Err(_) => info!(
                        "Read {} bytes worth of addresses, but still need more.",
                        bytes
                    ),
                    _ => panic!("Expected the senders' addresses"),
                }
            }
        };

        // Remove this remote's entry from the store.
        assert!(other_addrs.contains_key(&start.remote_id));
        assert!(other_addrs.len() == start.n_remotes as usize);
        other_addrs.remove(&start.remote_id);

        assert!(other_addrs.len() > 0);
        let rate: u16 = start.plan.rounds[0].packets_per_ms;
        Remote {
            socket,
            other_addrs: other_addrs.into_iter().collect(),
            confirmed_peers: BTreeSet::new(),
            local_connection: tcp,
            remote_id: start.remote_id,
            round_id: 0,
            send_results: RoundSenderResults::new(start.remote_id, 0, &BTreeSet::new()),
            receive_results: RoundReceiverResults::new(start.remote_id, 0, &BTreeSet::new()),
            all_send_results: Vec::new(),
            all_receive_results: Vec::new(),
            rate,
            plan: start.plan,
            state: RemoteState::Establish,
            round_start: Instant::now(),
            round_end: Instant::now(),
            packets_this_round: 0,
        }
    }

    fn send_packet_to_next(&mut self) -> io::Result<Option<()>> {
        let (id, addr) =
            self.other_addrs[self.packets_this_round as usize % self.other_addrs.len()];
        UDP_OUT_BUF.with(|b| {
            let udp_buf: &mut [u8; UDP_PAYLOAD_BYTES] = &mut b.borrow_mut();
            write(
                udp_buf,
                &Msg::Packet {
                    from: self.remote_id,
                    to: id,
                    round: self.round_id,
                },
            );
            self.socket
                .send_to(udp_buf, addr)
                .map(|_| {
                    self.packets_this_round += 1;
                    let traffic = self.send_results.data_by_receiver.get_mut(&id).unwrap();
                    traffic.packets += 1;
                    traffic.bytes += (UDP_PAYLOAD_BYTES + 28) as u64;
                })
                .map(Some)
                .or_else(noneify_would_block)
        })
    }

    fn ping_one(&mut self) -> io::Result<()> {
        self.round_start = Instant::now();
        UDP_OUT_BUF.with(|b| {
            let udp_buf: &mut [u8; UDP_PAYLOAD_BYTES] = &mut b.borrow_mut();
            let (id, addr) =
                self.other_addrs[self.packets_this_round as usize % self.other_addrs.len()];
            write(
                udp_buf,
                &Msg::Ping {
                    to: id,
                    from: self.remote_id,
                },
            );
            match self.socket.send_to(udp_buf, addr) {
                Ok(n) => {
                    if n != UDP_PAYLOAD_BYTES {
                        return Err(mk_err(format!("Only sent {} bytes", n)));
                    } else {
                        self.packets_this_round += 1;
                        Ok(())
                    }
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        Ok(())
                    } else {
                        return Err(mk_err(format!("Error sending ping {}", e)));
                    }
                }
            }
        })
    }

    fn read_local(&mut self) -> io::Result<Option<RemoteTcpMessage>> {
        let mut data = [b' '; 1000];
        self.local_connection
            .read(&mut data)
            .map(Some)
            .or_else(noneify_would_block)
            .and_then(|bytes| match bytes {
                Some(b) => bincode::deserialize(&data[..b])
                    .map_err(|e| mk_err(format!("Local derser: {}", e)))
                    .map(Some),
                None => Ok(None),
            })
    }

    fn read_socket(&mut self) -> io::Result<Option<Msg>> {
        UDP_IN_BUF.with(|b| {
            let udp_buf: &mut [u8; UDP_PAYLOAD_BYTES] = &mut b.borrow_mut();
            self.socket
                .recv_from(udp_buf)
                .and_then(|(size, _)| {
                    if size != UDP_PAYLOAD_BYTES {
                        return Err(mk_err(format!("Received message of length {}", size)))
                            .map(Some);
                    } else {
                        read(&udp_buf[..]).map(Some).or_else(noneify_would_block)
                    }
                })
                .or_else(noneify_would_block)
        })
    }

    fn send_to_master(&mut self, msg: &LocalTcpMessage) -> io::Result<()> {
        info!("Sending {:?} to master", msg);
        let enc = bincode::serialize(msg).unwrap();
        self.local_connection.write(&enc).map(|_| ())
    }

    fn read_all_pings(&mut self) -> io::Result<bool> {
        loop {
            match self.read_socket()? {
                Some(Msg::Ping { from, to }) => {
                    if to != self.remote_id {
                        return Err(mk_err(format!(
                            "I am {} but I got a ping for {}",
                            self.remote_id, to
                        )));
                    }
                    self.confirmed_peers.insert(from);
                    if self.confirmed_peers.len() == self.other_addrs.len() {
                        return Ok(true);
                    }
                }
                Some(p @ Msg::Packet { .. }) => {
                    return Err(mk_err(format!(
                        "Expected ping during Establish but received {:?}",
                        p
                    )));
                }
                None => return Ok(false),
            }
        }
    }

    fn read_all_packets(&mut self) -> io::Result<()> {
        match self.read_socket()? {
            Some(Msg::Packet { from, to, round }) => {
                if round == self.round_id && to == self.remote_id {
                    let traffic = self.receive_results.data_by_sender.get_mut(&from).unwrap();
                    traffic.packets += 1;
                    traffic.bytes += (UDP_PAYLOAD_BYTES + 28) as u64;
                    Ok(())
                } else {
                    self.receive_results.errors += 1;
                    Ok(())
                }
            }
            Some(_) => Ok(()),
            None => Ok(()),
        }
    }

    fn send_packets(&mut self) -> io::Result<()> {
        let target_packets = (self.round_start.elapsed().as_millis() as u64) * (self.rate as u64);
        if target_packets > self.packets_this_round {
            let send_now = std::cmp::min(50, target_packets - self.packets_this_round);
            for _ in 0..send_now {
                self.send_packet_to_next()?;
            }
        }
        Ok(())
    }

    fn step(&mut self) -> io::Result<Option<Duration>> {
        match self.state {
            RemoteState::Establish => {
                info!(
                    "in Establish state with {}/{} confirmations",
                    self.confirmed_peers.len(),
                    self.other_addrs.len()
                );
                info!("Have on {:?}", self.confirmed_peers);
                self.ping_one()?;
                if self.read_all_pings()? {
                    self.send_to_master(&LocalTcpMessage::AllConfirmed).ok();
                    self.send_results = RoundSenderResults::new(
                        self.remote_id,
                        self.round_id,
                        &self.confirmed_peers,
                    );
                    self.receive_results = RoundReceiverResults::new(
                        self.remote_id,
                        self.round_id,
                        &self.confirmed_peers,
                    );
                    self.state = RemoteState::WaitForStart;
                    Ok(None)
                } else {
                    Ok(Some(Duration::from_millis(100)))
                }
            }
            RemoteState::WaitForStart => match self.read_local()? {
                Some(RemoteTcpMessage::Start) => {
                    self.round_start = Instant::now() + Duration::from_secs(2);
                    self.round_end = self.round_start + self.plan.rounds[0].duration;
                    self.packets_this_round = 0;
                    self.state = RemoteState::Receive;
                    Ok(None)
                }
                Some(_) => Err(mk_err(format!("Did not receive start"))),
                None => {
                    self.ping_one()?;
                    self.read_all_pings()?;
                    Ok(Some(Duration::from_millis(10)))
                }
            },
            RemoteState::Receive => {
                self.read_all_packets()?;
                self.state = RemoteState::Send;
                Ok(None)
            }
            RemoteState::Send => {
                let now = Instant::now();
                self.state = RemoteState::Receive;
                if now >= self.round_start {
                    if now < self.round_end {
                        self.send_packets()?;
                    } else {
                        self.round_start =
                            self.round_end + self.plan.rounds[self.round_id as usize].pause;
                        self.round_id += 1;
                        let ps = self
                            .plan
                            .rounds
                            .iter()
                            .nth(self.round_id as usize)
                            .map(|r| r.packets_per_ms)
                            .unwrap_or(0);
                        self.rate = ps;
                        info!("Starting round {} with {} p/s", self.round_id, self.rate);
                        let sr = std::mem::replace(
                            &mut self.send_results,
                            RoundSenderResults::new(
                                self.remote_id,
                                self.round_id,
                                &self.confirmed_peers,
                            ),
                        );
                        let rr = std::mem::replace(
                            &mut self.receive_results,
                            RoundReceiverResults::new(
                                self.remote_id,
                                self.round_id,
                                &self.confirmed_peers,
                            ),
                        );
                        self.all_send_results.push(sr);
                        self.all_receive_results.push(rr);
                        if self.round_id as usize == self.plan.rounds.len() {
                            return Err(mk_err(format!("Done")));
                        }
                        self.packets_this_round = 0;
                        self.round_end =
                            self.round_start + self.plan.rounds[self.round_id as usize].duration;
                    }
                    Ok(None)
                } else {
                    Ok(Some(Duration::from_millis(10)))
                }
            }
        }
    }

    fn run(&mut self, kill_time: Instant) {
        loop {
            if Instant::now() > kill_time {
                self.send_to_master(&LocalTcpMessage::Error(format!(
                    "Timout remote {}",
                    self.remote_id
                )))
                .ok();
                return;
            }
            match self.step() {
                Ok(Some(sleep_time)) => {
                    std::thread::sleep(sleep_time);
                }
                Ok(None) => {}
                Err(e) => {
                    if self.round_id as usize == self.plan.rounds.len() {
                        let sr = std::mem::replace(&mut self.all_send_results, Vec::new());
                        let rr = std::mem::replace(&mut self.all_receive_results, Vec::new());
                        info!("Done: {:?}", (&sr, &rr));
                        self.local_connection.set_nonblocking(false).unwrap();
                        self.send_to_master(&LocalTcpMessage::Stats(sr, rr)).ok();
                        self.local_connection.flush().unwrap();
                        std::thread::sleep(Duration::from_secs(self.plan.rounds.len() as u64)); // Just in case
                        return; // Done
                    }
                    error!("Notifying local of '{}' and finishing", e);
                    self.send_to_master(&LocalTcpMessage::Error(format!("{}", e)))
                        .ok();
                    return;
                }
            }
        }
    }
}

fn my_handler(e: LambdaStart, c: lambda::Context) -> Result<LambdaResult, HandlerError> {
    info!("Lambda with event {:?} is alive", e);
    let kill_time = Instant::now() + Duration::from_millis(c.get_time_remaining_millis() as u64)
        - Duration::from_secs(2);

    let mut remote = Remote::new(e, c);
    remote.run(kill_time);

    Ok(LambdaResult {})
}
