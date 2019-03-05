#[macro_use]
extern crate lambda_runtime as lambda;
#[macro_use]
extern crate serde_derive;
extern crate rusoto_core;
extern crate rusoto_s3;
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

use rusoto_s3::S3;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io;
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::time::{Duration, Instant};

mod msg;
mod net;

use net::{ReadResult, TcpMsgStream};

use msg::experiment::{LinkData, Results, RoundPlan};
use msg::{
    LambdaResult, LambdaStart, LocalTcpMessage, RemoteId, RemoteTcpMessage, RoundId, StateUpdate,
};
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
    to_accept: BTreeMap<RemoteId, Instant>,
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
            to_accept: to_accept.into_iter().map(|i| (i, now)).collect(),
            to_pong: BTreeMap::new(),
            id,
        }
    }

    fn maintain_connections(&mut self) -> io::Result<()> {
        // Send pings
        let now = Instant::now();
        for (id, next_ping_time) in self.to_connect.iter_mut().chain(self.to_accept.iter_mut()) {
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
                    if self.to_accept.contains_key(&from) {
                        info!("First ping from {}", from);
                        self.to_pong.insert(from, Instant::now());
                        self.to_accept.remove(&from);
                    }
                }
                EstMsg::Pong(from) => {
                    if self.to_connect.contains_key(&from) {
                        info!("First pong from {}", from);
                        self.to_connect.remove(&from);
                    }
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
                            *next_pong_time = now + Duration::from_millis(5000);
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
    downstream: Vec<RemoteId>,
    upstream: Vec<RemoteId>,
    remote_id: RemoteId,
    round_id: RoundId,
    results: Results,
    round_start: Instant,
    round_end: Instant,
    packets_this_round: u64,
    packets_r_this_round: u64,
    packets_per_ms: u16,
    round_plans: Vec<RoundPlan>,
}

impl ExperimentProgress {
    fn new(
        remote_id: RemoteId,
        downstream: Vec<RemoteId>,
        upstream: Vec<RemoteId>,
        round_plans: Vec<RoundPlan>,
    ) -> Self {
        Self {
            remote_id,
            upstream,
            downstream,
            round_id: 0,
            results: Results::new(),
            round_start: Instant::now(),
            round_end: Instant::now(),
            packets_this_round: 0,
            packets_r_this_round: 0,
            packets_per_ms: round_plans[0].packets_per_ms,
            round_plans,
        }
    }

    pub fn record_send(&mut self, to: RemoteId, bytes: u64) {
        self.packets_this_round += 1;
        let link_data = self
            .results
            .links
            .get_mut(&(self.remote_id, to, self.round_id))
            .unwrap();
        link_data.sent.packets += 1;
        link_data.sent.bytes += bytes;
    }

    pub fn record_receipt(&mut self, from: RemoteId, bytes: u64) {
        self.packets_this_round += 1;
        let link_data = self
            .results
            .links
            .get_mut(&(from, self.remote_id, self.round_id))
            .unwrap();
        link_data.sent.packets += 1;
        link_data.sent.bytes += bytes;
    }

    pub fn start_experiment(&mut self, start_time: Instant) -> io::Result<()> {
        self.round_start = start_time;
        self.round_id = 0;
        let ps = self
            .round_plans
            .iter()
            .nth(self.round_id as usize)
            .map(|r| r.packets_per_ms)
            .unwrap_or(0);
        self.packets_per_ms = ps;
        self.initialize_round(self.round_id);
        if self.round_id as usize == self.round_plans.len() {
            return Err(mk_err(format!("Done")));
        }
        self.packets_this_round = 0;
        self.packets_r_this_round = 0;
        self.round_end = self.round_start + self.round_plans[self.round_id as usize].duration;
        info!(
            "Starting round {} with {} p/s from {:?}, to {:?}, @ {:?}",
            self.round_id,
            self.packets_per_ms,
            self.round_start,
            self.round_end,
            Instant::now(),
        );
        Ok(())
    }

    pub fn advance_round(&mut self) -> io::Result<()> {
        self.round_start = self.round_end + self.round_plans[self.round_id as usize].pause;
        self.round_id += 1;
        if self.round_id as usize == self.round_plans.len() {
            return Err(mk_err(format!("Done")));
        }
        if (self.round_id as usize) < self.round_plans.len() {
            let ps = self
                .round_plans
                .iter()
                .nth(self.round_id as usize)
                .map(|r| r.packets_per_ms)
                .unwrap_or(0);
            self.packets_per_ms = ps;
            self.initialize_round(self.round_id);
            self.packets_this_round = 0;
            self.packets_r_this_round = 0;
            self.round_end = self.round_start + self.round_plans[self.round_id as usize].duration;
            info!(
                "Starting round {} with {} p/s from {:?}, to {:?}, @ {:?}",
                self.round_id,
                self.packets_per_ms,
                self.round_start,
                self.round_end,
                Instant::now(),
            );
        } else {
            info!("No more rounds");
        }
        Ok(())
    }

    pub fn initialize_round(&mut self, round: RoundId) {
        for u in &self.upstream {
            self.results.links.insert(
                (*u, self.remote_id, round),
                LinkData::new(&self.round_plans[round as usize]),
            );
        }
        for d in &self.downstream {
            self.results.links.insert(
                (self.remote_id, *d, round),
                LinkData::new(&self.round_plans[round as usize]),
            );
        }
    }
}

struct Remote {
    puncher: UdpPuncher,
    downstream: Vec<RemoteId>,
    upstream: Vec<RemoteId>,
    local_connection: Option<TcpMsgStream>,
    remote_id: RemoteId,
    progress: ExperimentProgress,
    state: RemoteState,
    exp_name: String,
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
    fn new(start: LambdaStart, c: lambda::Context) -> Self {
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

        let to_accept: BTreeSet<RemoteId> = start.upstream.clone().into_iter().collect();
        let to_connect: BTreeSet<RemoteId> = start.downstream.clone().into_iter().collect();
        let downstream = start.downstream;
        let upstream = start.upstream;
        let puncher = UdpPuncher::new(socket, remote_id, addr_map, to_connect, to_accept);

        let progress = ExperimentProgress::new(
            start.remote_id,
            downstream.clone(),
            upstream.clone(),
            start.rounds,
        );
        Remote {
            puncher,
            downstream,
            upstream,
            local_connection: Some(tcp),
            remote_id,
            progress,
            exp_name: start.exp_name,
            state: RemoteState::Establish,
            last_local_time: Instant::now(),
        }
    }

    fn send_packet_to_next(&mut self) -> io::Result<Option<()>> {
        if self.downstream.len() > 0 {
            let id =
                self.downstream[self.progress.packets_this_round as usize % self.downstream.len()];
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
                        self.progress
                            .record_send(id, (UDP_PAYLOAD_BYTES + 28) as u64)
                    })
                })
        } else {
            Ok(Some(()))
        }
    }

    fn read_local(&mut self) -> io::Result<Option<RemoteTcpMessage>> {
        self.local_connection
            .as_mut()
            .map(
                |c| match c.read::<RemoteTcpMessage>(false).map_err(mk_err)? {
                    ReadResult::WouldBlock => Ok(None),
                    ReadResult::EOF => Err(mk_err(format!("Master connection closed"))),
                    ReadResult::Data(t) => Ok(Some(t)),
                },
            )
            .unwrap_or(Err(mk_err(format!(
                "Tried to read a message from the master, but the connection was already closed"
            ))))
    }

    fn send_to_master(&mut self, msg: &LocalTcpMessage) -> io::Result<()> {
        info!("Sending {:?} to master", msg);
        self.local_connection
            .as_mut()
            .map(|c| c.send(&msg))
            .unwrap_or(Err(mk_err(format!(
                "Tried to send a message to the master, but the connection was already closed"
            ))))
    }

    fn read_all_packets(&mut self) -> io::Result<()> {
        match self.puncher.read_socket::<Packet>()? {
            Some((size, Packet { from, to, round })) => {
                if round == self.progress.round_id && to == self.remote_id {
                    self.progress.record_receipt(from, (size + 28) as u64);
                    Ok(())
                } else {
                    warn!(
                        "I am {} doing round {}, but got a packet for {} doing round {}",
                        self.remote_id, self.progress.round_id, to, round
                    );
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
                    self.send_to_master(&LocalTcpMessage::AllConfirmed)?;
                    self.state = RemoteState::WaitForStart;
                }
                Ok(Some(Duration::from_millis(10)))
            }
            RemoteState::WaitForStart => match self.read_local()? {
                Some(RemoteTcpMessage::Start) => {
                    self.puncher.cease_traffic();
                    {
                        self.local_connection.take();
                    }
                    let start_time = Instant::now() + Duration::from_secs(6);
                    self.progress.start_experiment(start_time)?;
                    std::thread::sleep(Duration::from_secs(2));
                    self.puncher.maintain_connections()?; // Drain the pings
                    std::thread::sleep(Duration::from_secs(2));
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
                        self.progress.advance_round()?;
                    }
                    Ok(None)
                } else {
                    Ok(Some(Duration::from_millis(10)))
                }
            }
        }
    }

    #[allow(dead_code)]
    fn update_master(&mut self) {
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
            //self.update_master();
            match self.step() {
                Ok(Some(sleep_time)) => {
                    std::thread::sleep(sleep_time);
                }
                Ok(None) => {}
                Err(e) => {
                    if self.progress.round_id as usize == self.progress.round_plans.len() {
                        info!("Done: {:?}", self.progress.results);
                        let client = rusoto_s3::S3Client::new(rusoto_core::Region::UsWest1);
                        let string = match serde_json::to_string(&self.progress.results) {
                            Ok(s) => s,
                            Err(e) => {
                                error!("{}", e);
                                return;
                            }
                        };

                        let put = client
                            .put_object(rusoto_s3::PutObjectRequest {
                                bucket: "aozdemir-network-test".to_owned(),
                                key: format!("{}/{}.json", self.exp_name, self.remote_id),
                                body: Some(string.into_bytes().into()),
                                ..rusoto_s3::PutObjectRequest::default()
                            })
                            .sync()
                            .unwrap();
                        info!("S3 Response: {:#?}", put);
                        return; // Done
                    } else {
                        error!("Notifying local of '{}' and finishing", e);
                        self.send_to_master(&LocalTcpMessage::Error(format!("{}", e)))
                            .ok();
                        return;
                    }
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
