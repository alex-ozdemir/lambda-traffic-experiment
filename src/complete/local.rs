extern crate ctrlc;
extern crate indicatif;
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

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_lambda::Lambda;

use indicatif::{ProgressBar, ProgressStyle};

mod msg;
mod net;
mod options;

use msg::experiment::{ExperimentPlan, RoundReceiverResults, RoundSenderResults, TrafficData};
use msg::{LambdaStart, LocalTcpMessage, RemoteTcpMessage};
use msg::{RemoteId, RoundId};

struct Progress {
    start_time: Instant,
    last_update: Instant,
    display: ProgressBar,
}

impl Progress {
    fn new(expected_duration: Duration) -> Self {
        let now = Instant::now();
        let pb = ProgressBar::new(expected_duration.as_millis() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed}] {wide_bar:.blue/cyan} Estimate: {eta}")
                .progress_chars("##-"),
        );
        Self {
            start_time: now,
            last_update: now,
            display: pb,
        }
    }
    fn maybe_update(&mut self) {
        if self.last_update.elapsed() > Duration::from_millis(100) {
            self.last_update = Instant::now();
            self.display
                .set_position(self.start_time.elapsed().as_millis() as u64);
        }
    }
    fn finish(&mut self) {
        self.display.finish_with_message("Done!");
    }
}

struct Local {
    n_remotes: u16,
    remotes: BTreeMap<RemoteId, (SocketAddr, TcpStream, String)>,
    running: Arc<AtomicBool>,
    exp_name: String,
    results: Vec<(Vec<RoundSenderResults>, Vec<RoundReceiverResults>)>,
    progress: Progress,
}

impl Local {
    fn new(n_remotes: u16, port: u16, exp_name: String, plan: ExperimentPlan) -> Self {
        let (_, udp_addr) = net::open_public_udp(port + 1);

        println!("Binding TCP socket");
        let tcp_socket = TcpListener::bind(("0.0.0.0", port)).expect("Could not bind TCP socket");

        println!("Making lambda client");
        let client = aws_lambda::LambdaClient::new(aws::Region::UsWest2);

        println!("Issuing invocations");
        for i in 0..n_remotes {
            let start = LambdaStart {
                local_addr: SocketAddr::new(udp_addr.ip(), port),
                plan: plan.clone(),
                port,
                remote_id: i,
                n_remotes,
            };
            let lambda_status = client
                .invoke(aws_lambda::InvocationRequest {
                    function_name: "rust-test-complete-remote".to_owned(),
                    invocation_type: Some("Event".to_owned()),
                    payload: Some(serde_json::to_vec(&start).unwrap()),
                    ..aws_lambda::InvocationRequest::default()
                })
                .sync()
                .unwrap();
            assert!(lambda_status.status_code.unwrap() == 202);
        }

        let mut remotes: BTreeMap<RemoteId, (SocketAddr, TcpStream, String)> = (0..n_remotes)
            .map(|i| {
                println!("Waiting for remote #{}/{}", i, n_remotes);
                let (mut stream, tcp_addr) = tcp_socket.accept().unwrap();
                let mut data = [0; 1400];
                let bytes = stream.read(&mut data).expect("Failed to read from remote");
                match bincode::deserialize(&data[..bytes]).unwrap() {
                    LocalTcpMessage::MyAddress(addr, id, log) => {
                        assert_eq!(tcp_addr.ip(), addr.ip());
                        (id, (addr, stream, log))
                    }
                    _ => panic!("Expected the remote's address"),
                }
            })
            .collect();

        let remote_addrs: BTreeMap<_, _> = remotes
            .iter()
            .map(|(id, (addr, _, _))| (*id, *addr))
            .collect();

        println!("Remote addresses {:?}", remote_addrs);

        remotes
            .iter_mut()
            .map(|(id, (_, ref mut stream, _))| {
                stream
                    .write_all(
                        &bincode::serialize(&RemoteTcpMessage::AllAddrs(remote_addrs.clone()))
                            .unwrap(),
                    )
                    .unwrap_or_else(|_| panic!("Could not send addresses to {}", id));
                stream
                    .flush()
                    .unwrap_or_else(|_| panic!("Could not send addresses to {}", id));
                stream.set_nonblocking(true).unwrap();
            })
            .count();
        {
            let mut unconfirmed: BTreeSet<_> = remotes.keys().cloned().collect();
            let mut contact_times: BTreeMap<_, _> =
                remotes.keys().map(|i| (*i, Instant::now())).collect();
            let mut status_t = Instant::now();
            println!(
                "Waiting for {} workers to confirm connections",
                unconfirmed.len()
            );
            let logs = remotes
                .iter()
                .map(|(id, (_, _, log))| {
                    println!("{} {}", id, log);
                    (*id, log.clone())
                })
                .collect::<BTreeMap<_, _>>();
            while !unconfirmed.is_empty() {
                for (id, (_, ref mut stream, _)) in &mut remotes {
                    let mut data = [0; 1400];
                    let bytes = match stream.read(&mut data) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            if e.kind() == io::ErrorKind::WouldBlock {
                                continue;
                            } else {
                                panic!("IO Error during peer setup: {}", e);
                            }
                        }
                    };
                    match bincode::deserialize(&data[..bytes]) {
                        Ok(LocalTcpMessage::AllConfirmed) => {
                            unconfirmed.remove(id);
                            println!(
                                "Waiting for {} workers to confirm connections",
                                unconfirmed.len()
                            );
                            if unconfirmed.len() < 5 {
                                for i in &unconfirmed {
                                    println!("  {} {}", i, logs.get(i).unwrap());
                                }
                            }
                        }
                        Ok(LocalTcpMessage::Confirming(id, _cf)) => {
                            *contact_times.get_mut(&id).unwrap() = Instant::now();
                        }
                        Ok(LocalTcpMessage::Error(s)) => {
                            panic!("Got an error during peer setup: '{}'", s);
                        }
                        e => panic!(
                            "Expected the remotes to confirm peers, got {:?} from {}",
                            e, id
                        ),
                    }
                }
                if status_t.elapsed() >= Duration::from_secs(1) {
                    let now = Instant::now();
                    status_t = now;
                    for id in contact_times.keys() {
                        print!("{:5} ", id);
                    }
                    println!();
                    for t in contact_times.values() {
                        print!("{:5} ", (now - *t).as_millis());
                    }
                    println!();
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        println!("Sending start signal");
        remotes
            .iter_mut()
            .map(|(id, (_, ref mut stream, _))| {
                stream
                    .write_all(&bincode::serialize(&RemoteTcpMessage::Start).unwrap())
                    .unwrap_or_else(|_| panic!("Could not send addresses to {}", id));
                stream
                    .flush()
                    .unwrap_or_else(|_| panic!("Could not send addresses to {}", id));
            })
            .count();

        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        ctrlc::set_handler(move || {
            r.store(false, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");

        let estimated_round_time: Duration = plan
            .rounds
            .iter()
            .map(|round| round.duration + round.pause)
            .sum();
        let est_time =
            Duration::from_millis((estimated_round_time.as_millis() as f64 * 1.1) as u64)
                + Duration::from_secs(4);

        Local {
            remotes,
            n_remotes,
            running,
            exp_name,
            results: Vec::new(),
            progress: Progress::new(est_time),
        }
    }

    fn poll_remotes(&mut self) {
        let mut results = Vec::new();
        self.remotes
            .iter_mut()
            .map(|(i, (_, ref mut stream, _))| {
                let mut buf = [0; 65_507];
                let mut bytes = 0;
                let mut reading = false;
                let mut first = true;
                while first || reading {
                    first = false;
                    match stream.read(&mut buf[bytes..]) {
                        Err(e) => assert!(
                            e.kind() == std::io::ErrorKind::WouldBlock,
                            "Socket error {:?}",
                            e
                        ),
                        Ok(size) => {
                            reading = true;
                            bytes += size;
                            let message = match bincode::deserialize(&buf[..bytes]) {
                                Ok(m) => m,
                                Err(e) => {
                                    warn!(
                                        "Couldn't parse message from remote {} with size {}, err {}",
                                        i, bytes, e
                                    );
                                    continue;
                                }
                            };
                            match message {
                                LocalTcpMessage::Stats(sr, rr) => {
                                    results.push((sr, rr));
                                    break;
                                }
                                m => panic!("Expected results but found {:?}", m),
                            }
                        }
                    }
                }
            })
        .count();
        if results.len() > 0 {
            println!("Got results {}/{}", self.results.len(), self.n_remotes);
        }
        self.results.extend(results);
    }

    fn maybe_die(&mut self) {
        if !self.running.load(Ordering::SeqCst) {
            self.remotes
                .iter_mut()
                .map(|(id, (_, ref mut stream, _))| {
                    stream
                        .write_all(&bincode::serialize(&RemoteTcpMessage::Die).unwrap())
                        .unwrap_or_else(|_| panic!("Could not send die to {}", id));
                    stream
                        .flush()
                        .unwrap_or_else(|_| panic!("Could not send die to {}", id));
                })
                .count();
            println!("Experiment {} conluding", self.exp_name);
            std::process::exit(2)
        }
    }

    fn run_till_results(&mut self) {
        while self.results.len() < self.n_remotes as usize {
            self.progress.maybe_update();
            self.maybe_die();
            self.poll_remotes();
            std::thread::sleep(Duration::from_millis(50));
        }
        self.progress.finish();
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    let args = options::parse_args();
    let rounds = msg::experiment::rounds_with_range_of_counts(
        Duration::from_secs(args.flag_duration as u64),
        Duration::from_secs(args.flag_sleep as u64),
        args.flag_rounds,
        args.flag_packets,
    );
    let plan = ExperimentPlan {
        rounds,
        recipients: msg::experiment::recipients_complete(args.arg_remotes),
    };
    println!("Plan: {:#?}", plan);
    let mut local = Local::new(
        args.arg_remotes,
        args.flag_port,
        args.arg_exp_name.clone(),
        plan.clone(),
    );
    local.run_till_results();
    let durations_and_rates = plan
        .rounds
        .iter()
        .enumerate()
        .map(|(i, rd)| {
            (
                i as u16,
                (rd.duration.as_millis() as f64 / 1000.0, rd.packets_per_ms),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut d: BTreeMap<(RoundId, RemoteId, RemoteId), (TrafficData, TrafficData, f64, u16)> =
        BTreeMap::new();
    for (srs, rrs) in &local.results {
        for sr in srs {
            for (r, t) in &sr.data_by_receiver {
                d.entry((sr.round_id, sr.remote_id, *r))
                    .or_insert_with(|| {
                        let (d, r) = *durations_and_rates.get(&sr.round_id).unwrap();
                        (TrafficData::new(), TrafficData::new(), d, r)
                    })
                    .0 = t.clone();
            }
        }
        for rr in rrs {
            for (s, t) in &rr.data_by_sender {
                d.entry((rr.round_id, *s, rr.remote_id))
                    .or_insert_with(|| {
                        let (d, r) = *durations_and_rates.get(&rr.round_id).unwrap();
                        (TrafficData::new(), TrafficData::new(), d, r)
                    })
                    .1 = t.clone();
            }
        }
    }
    std::fs::create_dir_all("data").unwrap();
    let mut f = std::fs::File::create(format!("data/{}.csv", args.arg_exp_name)).unwrap();
    writeln!(
        &mut f,
        "round,secs,rate,from,to,bytes_s,bytes_r,packets_s,packets_r"
    )
    .unwrap();
    for ((rd, from, to), (t_s, t_r, dur, rt)) in d {
        writeln!(
            &mut f,
            "{},{},{},{},{},{},{},{},{}",
            rd, dur, rt, from, to, t_s.bytes, t_r.bytes, t_s.packets, t_r.packets,
        )
        .unwrap()
    }

    Ok(())
}
