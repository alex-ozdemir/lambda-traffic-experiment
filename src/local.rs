extern crate bincode;
extern crate byteorder;
extern crate ctrlc;
extern crate indicatif;
extern crate rand;
extern crate rusoto_core as aws;
extern crate rusoto_lambda as aws_lambda;
extern crate simple_logger;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate stun;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::net::{SocketAddr, TcpListener};
use std::str::FromStr;
use std::time::{Duration, Instant};

use aws_lambda::Lambda;

use rand::seq::SliceRandom;

use indicatif::{ProgressBar, ProgressStyle};

mod msg;
mod net;
mod options;

use msg::experiment::ExperimentPlan;
use msg::RemoteId;
use msg::{LambdaStart, LocalTcpMessage, RemoteTcpMessage};

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
    progress: Progress,
    end_time: Instant,
}

impl Local {
    fn new(
        n_remotes: u16,
        port: u16,
        exp_name: String,
        plan: ExperimentPlan,
        region: aws::Region,
    ) -> Self {
        let (_, udp_addr) = net::open_public_udp(port + 1);

        println!("Binding TCP socket");
        let tcp_socket = TcpListener::bind(("0.0.0.0", port)).expect("Could not bind TCP socket");

        println!("Making lambda client");
        let client = aws_lambda::LambdaClient::new(region.clone());

        let mut upstreams: BTreeMap<RemoteId, Vec<RemoteId>> =
            (0..n_remotes).map(|i| (i, Vec::new())).collect();
        for (src, sinks) in &plan.recipients {
            for sink in sinks {
                upstreams.get_mut(&sink).unwrap().push(*src);
            }
        }
        println!("Issuing invocations");
        let mut ids: Vec<_> = (0..n_remotes).collect();
        ids.shuffle(&mut rand::thread_rng());
        for i in ids {
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
                exp_name: exp_name.clone(),
            };
            let lambda_status = client
                .invoke(aws_lambda::InvocationRequest {
                    function_name: "rust-test-remote".to_owned(),
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
        let _logs = remotes
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
            while !unconfirmed.is_empty() {
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

        let estimated_round_time: Duration = plan
            .rounds
            .iter()
            .map(|round| round.duration + round.pause)
            .sum();
        let est_time =
            Duration::from_millis((estimated_round_time.as_millis() as f64 * 1.1) as u64)
                + Duration::from_secs(4);

        Local {
            progress: Progress::new(est_time),
            end_time: Instant::now() + est_time,
        }
    }

    fn run_till_results(&mut self) {
        while Instant::now() < self.end_time {
            self.progress.maybe_update();
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
    let region = aws::Region::from_str(&env::var("AWS_REGION").expect("AWS_REGION must be set"))
        .expect("Invalid region");
    let (mut n_remotes, mut recipients) = if args.cmd_complete {
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
    msg::experiment::add_extras(&mut recipients, args.flag_extra);
    n_remotes += args.flag_extra;
    let plan = ExperimentPlan { rounds, recipients };
    let mut local = Local::new(
        n_remotes,
        args.flag_port,
        args.arg_exp_name.clone(),
        plan.clone(),
        region,
    );
    local.run_till_results();
    Ok(())
}
