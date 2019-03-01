extern crate bincode;
extern crate byteorder;
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
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_lambda::Lambda;

use indicatif::{ProgressBar, ProgressStyle};

mod msg;
mod net;
mod options;

use msg::experiment::{ExperimentPlan, RoundReceiverResults, RoundSenderResults, TrafficData};
use msg::{LambdaStart, LocalTcpMessage, RemoteTcpMessage, StateUpdate};
use msg::{RemoteId, RoundId};

use net::{ReadResult, TcpMsgStream};

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
    remotes: BTreeMap<RemoteId, (SocketAddr, TcpMsgStream, String)>,
    running: Arc<AtomicBool>,
    exp_name: String,
    results: Vec<(Vec<RoundSenderResults>, Vec<RoundReceiverResults>)>,
    remote_states: BTreeMap<RemoteId, (Instant, Option<StateUpdate>)>,
    last_status_update: Instant,
    //progress: Progress,
}

impl Local {
    fn new(n_remotes: u16, port: u16, exp_name: String, plan: ExperimentPlan) -> Self {
        let (_, udp_addr) = net::open_public_udp(port + 1);

        println!("Binding TCP socket");
        let tcp_socket = TcpListener::bind(("0.0.0.0", port)).expect("Could not bind TCP socket");

        println!("Making lambda client");
        let client = aws_lambda::LambdaClient::new(aws::Region::UsWest1);

        let mut upstreams: BTreeMap<RemoteId, Vec<RemoteId>> =
            (0..n_remotes).map(|i| (i, Vec::new())).collect();
        for (src, sinks) in &plan.recipients {
            for sink in sinks {
                upstreams.get_mut(&sink).unwrap().push(*src);
            }
        }
        println!("Issuing invocations");
        for i in 0..n_remotes {
            let upstream = upstreams.remove(&i).unwrap();
            let downstream = plan.recipients.get(&i).unwrap().clone();
            println!("{} from {:?} to {:?}", i, upstream, downstream);
            let start = LambdaStart {
                local_addr: SocketAddr::new(udp_addr.ip(), port),
                rounds: plan.rounds.clone(),
                downstream,
                upstream,
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

        let mut remotes: BTreeMap<RemoteId, (SocketAddr, TcpMsgStream, String)> = (0..n_remotes)
            .map(|i| {
                println!("Waiting for remote #{}/{}", i + 1, n_remotes);
                let (stream, tcp_addr) = tcp_socket.accept().unwrap();
                let mut stream = TcpMsgStream::new(stream);
                let msg = stream.read::<LocalTcpMessage>(true).unwrap();
                match msg {
                    ReadResult::Data(LocalTcpMessage::MyAddress(addr, id, log)) => {
                        assert_eq!(tcp_addr.ip(), addr.ip());
                        (id, (addr, stream, log))
                    }
                    e => panic!("Expected the remote's address, got: {:?}", e),
                }
            })
            .collect();

        let remote_addrs: BTreeMap<_, _> = remotes
            .iter()
            .map(|(id, (addr, _, _))| (*id, *addr))
            .collect();

        println!("Remote addresses {:?}", remote_addrs);
        let mut contact_times: BTreeMap<_, _> = remotes
            .keys()
            .map(|i| (*i, (Instant::now(), None)))
            .collect();

        remotes
            .iter_mut()
            .map(|(id, (_, ref mut stream, _))| {
                stream
                    .send(&RemoteTcpMessage::AllAddrs(remote_addrs.clone()))
                    .unwrap_or_else(|_| panic!("Could not send addresses to {}", id));
            })
            .count();
        let logs = remotes
            .iter()
            .map(|(id, (_, _, log))| {
                println!("{} {}", id, log);
                (*id, log.clone())
            })
            .collect::<BTreeMap<_, _>>();
        {
            let mut unconfirmed: BTreeSet<_> = remotes.keys().cloned().collect();
            println!(
                "Waiting for {} workers to confirm connections",
                unconfirmed.len()
            );
            let mut last_local_time = Instant::now();
            while !unconfirmed.is_empty() {
                if last_local_time.elapsed() > Duration::from_secs(1) {
                    last_local_time = Instant::now();
                    println!("{:?}", contact_times);
                }
                for (id, (_, ref mut stream, _)) in &mut remotes {
                    match stream.read::<LocalTcpMessage>(false) {
                        Ok(ReadResult::Data(LocalTcpMessage::AllConfirmed)) => {
                            unconfirmed.remove(id);
                        }
                        Ok(ReadResult::Data(LocalTcpMessage::State(update))) => {
                            *contact_times.get_mut(&id).unwrap() = (Instant::now(), Some(update));
                        }
                        Ok(ReadResult::WouldBlock) => {}
                        e => panic!(
                            "Expected the remotes to confirm peers, got {:?} from {}",
                            e, id
                        ),
                    }
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        println!("Sending start signal");
        remotes
            .iter_mut()
            .map(|(_, (_, ref mut stream, _))| {
                stream
                    .send::<RemoteTcpMessage>(&RemoteTcpMessage::Start)
                    .unwrap();
            })
            .count();

        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();

        ctrlc::set_handler(move || {
            r.store(false, Ordering::SeqCst);
            for (id, log) in &logs {
                println!("{} {}", id, log);
            }
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
            remote_states: contact_times,
            last_status_update: Instant::now(),
            //progress: Progress::new(est_time),
        }
    }

    fn poll_remotes(&mut self) -> Result<(), String> {
        let mut results = Vec::new();
        for (i, (_, ref mut stream, _)) in &mut self.remotes {
            match stream
                .read::<LocalTcpMessage>(false)
                .map_err(|e| format!("{}", e))?
            {
                ReadResult::WouldBlock => {}
                ReadResult::Data(LocalTcpMessage::Stats(sr, rr)) => {
                    results.push((*i, (sr, rr)));
                }
                ReadResult::Data(LocalTcpMessage::State(update)) => {
                    *self.remote_states.get_mut(i).unwrap() = (Instant::now(), Some(update));
                }
                wat => {
                    return Err(format!("Got {:?} while reading results from {}", wat, i));
                }
            }
        }
        if results.len() > 0 {
            println!("Got results {}/{}", results.len() + self.results.len(), self.n_remotes);
            println!("Waiting on {:?}", self.remotes.iter().map(|(i,_)| i).collect::<BTreeSet<_>>());
        }
        for (id, _) in &results {
            self.remotes.remove(id).unwrap();
        }
        self.results.extend(results.into_iter().map(|(_, b)| b));
        Ok(())
    }

    fn maybe_print_status(&mut self) {
        if self.last_status_update.elapsed() >= Duration::from_millis(5000) {
            self.last_status_update = Instant::now();
            for (id, _) in &self.remote_states {
                print!("{:5}", id)
            }
            println!("");
            let n = Instant::now();
            for (_, (t, _)) in &self.remote_states {
                print!("{:5.1}", (n - *t).as_millis() as f64 / 1000.0);
            }
            println!("");
            for (_, (_, s)) in &self.remote_states {
                match s {
                    Some(StateUpdate::Connected) => print!("    c"),
                    Some(StateUpdate::Connecting { .. }) => print!("    w"),
                    Some(StateUpdate::InRound { id, .. }) => print!("{:5}", id),
                    None => print!("    -"),
                };
            }
            println!("\n");
        }
    }

    fn maybe_die(&mut self) {
        if !self.running.load(Ordering::SeqCst) {
            self.remotes
                .iter_mut()
                .map(|(_, (_, ref mut stream, _))| {
                    stream
                        .send::<RemoteTcpMessage>(&RemoteTcpMessage::Die)
                        .unwrap();
                })
                .count();
            println!("Experiment {} dieing", self.exp_name);
            std::process::exit(2)
        }
    }

    fn run_till_results(&mut self) {
        while self.results.len() < self.n_remotes as usize {
            //self.progress.maybe_update();
            self.maybe_die();
            self.poll_remotes()
                .map_err(|e| {
                    error!("{}", e);
                    self.running.store(false, Ordering::SeqCst);
                })
                .ok();
            self.maybe_print_status();
            std::thread::sleep(Duration::from_millis(50));
        }
        //self.progress.finish();
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
    let (n_remotes, recipients) = if args.cmd_complete {
        (
            args.arg_remotes.unwrap(),
            msg::experiment::recipients_complete(args.arg_remotes.unwrap()),
        )
    } else if args.cmd_bipartite {
        (
            args.arg_senders.unwrap() + args.arg_receivers.unwrap(),
            msg::experiment::recipients_bipartite(
                args.arg_senders.unwrap(),
                args.arg_receivers.unwrap(),
            ),
        )
    } else if args.cmd_dipairs {
        (
            2 * args.arg_pairs.unwrap(),
            msg::experiment::recipients_dipairs(args.arg_pairs.unwrap()),
        )
    } else if args.cmd_bipairs {
        (
            2 * args.arg_pairs.unwrap(),
            msg::experiment::recipients_bipairs(args.arg_pairs.unwrap()),
        )
    } else {
        panic!("Unknown command")
    };
    let plan = ExperimentPlan { rounds, recipients };
    let mut local = Local::new(
        n_remotes,
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
    println!("Writing file: {:?}", f);
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
