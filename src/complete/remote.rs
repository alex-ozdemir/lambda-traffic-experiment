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

use serde::de::DeserializeOwned;
use serde::Serialize;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io;
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::time::{Duration, Instant};

mod msg;
mod net;

use net::{ReadResult, TcpMsgStream};

use msg::experiment::{RoundPlan, RoundReceiverResults, RoundSenderResults};
use msg::{LambdaResult, LambdaStart, LocalTcpMessage, RemoteId, RemoteTcpMessage, RoundId, StateUpdate};
pub const UDP_PAYLOAD_BYTES: usize = 1372; // For an IP packet of size 1400

thread_local! {
    pub static UDP_BUF: RefCell<[u8; UDP_PAYLOAD_BYTES]> = RefCell::new([b'Q'; UDP_PAYLOAD_BYTES]);
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

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Info)?;
    lambda!(my_handler);

    Ok(())
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct Packet {
    to: RemoteId,
    from: RemoteId,
    round: RoundId,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
enum EstMsg {
    Ping(RemoteId),
    Pong(RemoteId),
}

/// For punching through firewalls
struct UdpPuncher {
    socket: UdpSocket,
    addr_map: BTreeMap<RemoteId, SocketAddr>,
    to_connect: BTreeMap<RemoteId, Instant>,
    to_accept: BTreeSet<RemoteId>,
    to_pong: BTreeMap<RemoteId, Instant>,
    id: RemoteId,
}

impl UdpPuncher {
    fn new(
        socket: UdpSocket,
        id: RemoteId,
        addr_map: BTreeMap<RemoteId, SocketAddr>,
        to_connect: BTreeSet<RemoteId>,
        to_accept: BTreeSet<RemoteId>,
    ) -> Self {
        let now = Instant::now();
        Self {
            socket,
            addr_map,
            to_connect: to_connect.into_iter().map(|i| (i, now)).collect(),
            to_accept,
            to_pong: BTreeMap::new(),
            id,
        }
    }

    fn maintain_connections(&mut self) -> io::Result<()> {
        // Send pings
        let now = Instant::now();
        for (id, next_ping_time) in &mut self.to_connect {
            if now >= *next_ping_time {
                let addr = self
                    .addr_map
                    .get(id)
                    .ok_or_else(|| mk_err(format!("Unknown id {}", id)))?;
                let ping_buf = bincode::serialize(&EstMsg::Ping(self.id)).unwrap();
                let ping_len = ping_buf.len();
                match self.socket.send_to(ping_buf.as_slice(), addr) {
                    Ok(len) => {
                        if len == ping_len {
                            *next_ping_time = now + Duration::from_millis(2000);
                        } else {
                            return Err(mk_err(format!(
                                "Ping length was {} but only {} was sent",
                                ping_len, len
                            )));
                        }
                    }
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            break;
                        // We'll just send again...
                        } else {
                            return Err(mk_err(format!("Ping send error: {}", e)));
                        }
                    }
                }
            }
        }

        // Poll pings and pongs
        while let Some(msg) = self.read_socket::<EstMsg>()? {
            match msg.1 {
                EstMsg::Ping(from) => {
                    self.to_pong.insert(from, Instant::now());
                    self.to_accept.remove(&from);
                }
                EstMsg::Pong(from) => {
                    self.to_connect.remove(&from);
                }
            }
        }

        // Send pongs
        let now = Instant::now();
        for (id, next_pong_time) in &mut self.to_pong {
            if now >= *next_pong_time {
                let addr = self
                    .addr_map
                    .get(id)
                    .ok_or_else(|| mk_err(format!("Unknown id {}", id)))?;
                let pong_buf = bincode::serialize(&EstMsg::Pong(self.id)).unwrap();
                let pong_len = pong_buf.len();
                match self.socket.send_to(pong_buf.as_slice(), addr) {
                    Ok(len) => {
                        if len == pong_len {
                            *next_pong_time = now + Duration::from_millis(2000);
                        } else {
                            return Err(mk_err(format!(
                                "Pong length was {} but only {} was sent",
                                pong_len, len
                            )));
                        }
                    }
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            break;
                        // We'll just send again...
                        } else {
                            return Err(mk_err(format!("Pong send error: {}", e)));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn read_socket<M: DeserializeOwned>(&mut self) -> io::Result<Option<(usize, M)>> {
        UDP_BUF.with(|b| {
            let udp_buf: &mut [u8; UDP_PAYLOAD_BYTES] = &mut b.borrow_mut();
            self.socket
                .recv_from(udp_buf)
                .and_then(|(size, _)| {
                    bincode::deserialize(&udp_buf[..size])
                        .map(|v: M| Some((size, v)))
                        .map_err(|e| mk_err(format!("Deser packet error: {}", e)))
                })
                .or_else(noneify_would_block)
        })
    }

    fn accepted_all(&self) -> bool {
        self.to_accept.is_empty()
    }

    fn cease_traffic(&mut self) {
        self.to_pong.clear();
        self.to_accept.clear();
        self.to_connect.clear();
    }

    fn send_to<M: Serialize>(&self, msg: &M, to: RemoteId) -> io::Result<Option<()>> {
        let addr = self
            .addr_map
            .get(&to)
            .ok_or_else(|| mk_err(format!("Unknown destination id {}", to)))?;
        UDP_BUF.with(|b| {
            let udp_buf: &mut [u8; UDP_PAYLOAD_BYTES] = &mut b.borrow_mut();
            let buf_writer: &mut [u8] = udp_buf;
            bincode::serialize_into(buf_writer, &msg).unwrap();
            self.socket
                .send_to(udp_buf, addr)
                .and_then(|len| {
                    if len == UDP_PAYLOAD_BYTES {
                        Ok(Some(()))
                    } else {
                        Err(mk_err(format!(
                            "Only wrote {}/{} bytes",
                            len, UDP_PAYLOAD_BYTES
                        )))
                    }
                })
                .or_else(noneify_would_block)
        })
    }
}

struct ExperimentProgress {
    round_id: RoundId,
    send_results: RoundSenderResults,
    receive_results: RoundReceiverResults,
    all_send_results: Vec<RoundSenderResults>,
    all_receive_results: Vec<RoundReceiverResults>,
    round_start: Instant,
    round_end: Instant,
    packets_this_round: u64,
    packets_r_this_round: u64,
    packets_per_ms: u16,
    round_plans: Vec<RoundPlan>,
}

impl ExperimentProgress {
    fn new(id: RemoteId, round_plans: Vec<RoundPlan>) -> Self {
        Self {
            round_id: 0,
            send_results: RoundSenderResults::new(id, 0, &Vec::new()),
            receive_results: RoundReceiverResults::new(id, 0, &Vec::new()),
            all_send_results: Vec::new(),
            all_receive_results: Vec::new(),
            round_start: Instant::now(),
            round_end: Instant::now(),
            packets_this_round: 0,
            packets_r_this_round: 0,
            packets_per_ms: round_plans[0].packets_per_ms,
            round_plans,
        }
    }
}

struct Remote {
    puncher: UdpPuncher,
    recipients: Vec<RemoteId>,
    upstream: Vec<RemoteId>,
    local_connection: TcpMsgStream,
    remote_id: RemoteId,
    progress: ExperimentProgress,
    state: RemoteState,
    last_local_time: Instant,
}

#[derive(Debug)]
enum RemoteState {
    Establish,
    WaitForStart,
    Receive,
    Send,
}

impl Remote {
    fn new(mut start: LambdaStart, c: lambda::Context) -> Self {
        info!("Starting with invocation {:?}", start);
        let (socket, my_addr) = net::open_public_udp(start.port);
        let remote_id = start.remote_id;
        let tcp = TcpStream::connect(start.local_addr).unwrap();
        let mut tcp = TcpMsgStream::new(tcp);
        info!("Connected to the master");
        tcp.send(&LocalTcpMessage::MyAddress(
            my_addr,
            start.remote_id,
            c.log_stream_name,
        ))
        .unwrap();

        let addr_map = {
            info!("Getting addresses");
            match tcp.read::<RemoteTcpMessage>(true).unwrap() {
                ReadResult::Data(RemoteTcpMessage::AllAddrs(addrs)) => addrs,
                other => {
                    panic!("Expected addrs, found {:?}", other);
                }
            }
        };

        let to_accept: BTreeSet<RemoteId> = start
            .plan
            .recipients
            .iter()
            .filter(|(_, recipients)| recipients.contains(&remote_id))
            .map(|(i, _)| *i)
            .collect();
        let to_connect: BTreeSet<RemoteId> = start.plan.recipients.remove(&remote_id).unwrap();
        let recipients: Vec<RemoteId> = to_connect.iter().cloned().collect();
        let upstream: Vec<RemoteId> = to_accept.iter().cloned().collect();
        let puncher = UdpPuncher::new(socket, remote_id, addr_map, to_connect, to_accept);

        let progress = ExperimentProgress::new(remote_id, start.plan.rounds);
        Remote {
            puncher,
            recipients,
            upstream,
            local_connection: tcp,
            remote_id,
            progress,
            state: RemoteState::Establish,
            last_local_time: Instant::now(),
        }
    }

    fn send_packet_to_next(&mut self) -> io::Result<Option<()>> {
        let id = self.recipients[self.progress.packets_this_round as usize % self.recipients.len()];
        self.puncher
            .send_to(
                &Packet {
                    from: self.remote_id,
                    to: id,
                    round: self.progress.round_id,
                },
                id,
            )
            .map(|o| {
                o.map(|()| {
                    self.progress.packets_this_round += 1;
                    let traffic = self
                        .progress
                        .send_results
                        .data_by_receiver
                        .get_mut(&id)
                        .unwrap();
                    traffic.packets += 1;
                    traffic.bytes += (UDP_PAYLOAD_BYTES + 28) as u64;
                })
            })
    }

    fn read_local(&mut self) -> io::Result<Option<RemoteTcpMessage>> {
        match self
            .local_connection
            .read::<RemoteTcpMessage>(false)
            .map_err(mk_err)?
        {
            ReadResult::WouldBlock => Ok(None),
            ReadResult::EOF => Err(mk_err(format!("Master connection closed"))),
            ReadResult::Data(t) => Ok(Some(t)),
        }
    }

    fn send_to_master(&mut self, msg: &LocalTcpMessage) -> io::Result<()> {
        info!("Sending {:?} to master", msg);
        self.local_connection.send(&msg)
    }

    fn read_all_packets(&mut self) -> io::Result<()> {
        match self.puncher.read_socket::<Packet>()? {
            Some((size, Packet { from, to, round })) => {
                if round == self.progress.round_id && to == self.remote_id {
                    let traffic = self
                        .progress
                        .receive_results
                        .data_by_sender
                        .get_mut(&from)
                        .unwrap();
                    traffic.packets += 1;
                    self.progress.packets_r_this_round += 1;
                    traffic.bytes += (size + 28) as u64;
                    Ok(())
                } else {
                    self.progress.receive_results.errors += 1;
                    Ok(())
                }
            }
            None => Ok(()),
        }
    }

    fn send_packets(&mut self) -> io::Result<()> {
        let target_packets = (self.progress.round_start.elapsed().as_millis() as u64)
            * (self.progress.packets_per_ms as u64);
        if target_packets > self.progress.packets_this_round {
            self.send_packet_to_next()?;
        }
        Ok(())
    }

    fn step(&mut self) -> io::Result<Option<Duration>> {
        match self.state {
            RemoteState::Establish => {
                self.puncher.maintain_connections()?;
                if self.puncher.accepted_all() {
                    self.send_to_master(&LocalTcpMessage::AllConfirmed).ok();
                    self.progress.send_results = RoundSenderResults::new(
                        self.remote_id,
                        self.progress.round_id,
                        &self.recipients,
                    );
                    self.progress.receive_results = RoundReceiverResults::new(
                        self.remote_id,
                        self.progress.round_id,
                        &self.upstream,
                    );
                    self.state = RemoteState::WaitForStart;
                }
                Ok(Some(Duration::from_millis(10)))
            }
            RemoteState::WaitForStart => match self.read_local()? {
                Some(RemoteTcpMessage::Start) => {
                    self.puncher.cease_traffic();
                    std::thread::sleep(Duration::from_secs(2));
                    self.puncher.maintain_connections()?;
                    std::thread::sleep(Duration::from_secs(2));
                    self.progress.round_start = Instant::now() + Duration::from_secs(2);
                    self.progress.round_end =
                        self.progress.round_start + self.progress.round_plans[0].duration;
                    self.progress.packets_this_round = 0;
                    self.progress.packets_r_this_round = 0;
                    self.state = RemoteState::Receive;
                    Ok(None)
                }
                Some(_) => Err(mk_err(format!("Did not receive start"))),
                None => {
                    self.puncher.maintain_connections()?;
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
                if now >= self.progress.round_start {
                    if now < self.progress.round_end {
                        self.send_packets()?;
                    } else {
                        self.progress.round_start = self.progress.round_end
                            + self.progress.round_plans[self.progress.round_id as usize].pause;
                        self.progress.round_id += 1;
                        let ps = self
                            .progress
                            .round_plans
                            .iter()
                            .nth(self.progress.round_id as usize)
                            .map(|r| r.packets_per_ms)
                            .unwrap_or(0);
                        self.progress.packets_per_ms = ps;
                        info!(
                            "Starting round {} with {} p/s",
                            self.progress.round_id, self.progress.packets_per_ms
                        );
                        let sr = std::mem::replace(
                            &mut self.progress.send_results,
                            RoundSenderResults::new(
                                self.remote_id,
                                self.progress.round_id,
                                &self.recipients,
                            ),
                        );
                        let rr = std::mem::replace(
                            &mut self.progress.receive_results,
                            RoundReceiverResults::new(
                                self.remote_id,
                                self.progress.round_id,
                                &self.upstream,
                            ),
                        );
                        self.progress.all_send_results.push(sr);
                        self.progress.all_receive_results.push(rr);
                        if self.progress.round_id as usize == self.progress.round_plans.len() {
                            return Err(mk_err(format!("Done")));
                        }
                        self.progress.packets_this_round = 0;
                        self.progress.packets_r_this_round = 0;
                        self.progress.round_end = self.progress.round_start
                            + self.progress.round_plans[self.progress.round_id as usize].duration;
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
            if self.last_local_time.elapsed() >= Duration::from_secs(1) {
                self.last_local_time = Instant::now();
                let update = match self.state {
                    RemoteState::Establish => StateUpdate::Connecting {
                        unconnected: self.puncher.to_accept.len() as u16,
                        desired: self.upstream.len() as u16,
                    },
                    RemoteState::Receive | RemoteState::Send => StateUpdate::InRound {
                        id: self.progress.round_id,
                        packets_r: self.progress.packets_r_this_round,
                        packets_s: self.progress.packets_this_round,
                    },
                    RemoteState::WaitForStart => StateUpdate::Connected,
                };
                info!(
                    "Local update: {:?}",
                    self.send_to_master(&LocalTcpMessage::State(update))
                );
            }
            match self.step() {
                Ok(Some(sleep_time)) => {
                    std::thread::sleep(sleep_time);
                }
                Ok(None) => {}
                Err(e) => {
                    if self.progress.round_id as usize == self.progress.round_plans.len() {
                        let sr = std::mem::replace(&mut self.progress.all_send_results, Vec::new());
                        let rr =
                            std::mem::replace(&mut self.progress.all_receive_results, Vec::new());
                        info!("Done: {:?}", (&sr, &rr));
                        let sr = self.send_to_master(&LocalTcpMessage::Stats(sr, rr));
                        info!("Send result: {:?}", sr);
                        std::thread::sleep(Duration::from_secs(
                            4 * self.progress.round_plans.len() as u64,
                        )); // Just in case
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
