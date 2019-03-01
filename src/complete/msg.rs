#![allow(dead_code)]

use std::net::SocketAddr;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub type RemoteId = u16;
pub type RoundId = u16;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LambdaStart {
    pub local_addr: SocketAddr,
    pub port: u16,
    pub plan: experiment::ExperimentPlan,
    pub remote_id: RemoteId,
    pub n_remotes: u16,
}

pub mod experiment {
    use std::time::Duration;
    use super::*;

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct RoundPlan {
        pub packets_per_ms: u16,
        pub duration: Duration,
        pub pause: Duration,
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct TrafficData {
        pub bytes: u64,
        pub packets: u64,
    }

    impl TrafficData {
        pub fn new() -> Self {
            Self {
                bytes: 0,
                packets: 0,
            }
        }
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct RoundSenderResults {
        pub remote_id: RemoteId,
        pub round_id: RoundId,
        pub data_by_receiver: BTreeMap<u16, TrafficData>,
        pub errors: u64,
    }

    impl RoundSenderResults {
        pub fn new(remote_id: RemoteId, round_id: RoundId, downstream: &Vec<RemoteId>) -> Self {
            Self {
                remote_id,
                round_id,
                data_by_receiver: downstream.iter().map(|i| (*i,TrafficData{ packets: 0, bytes: 0 })).collect(),
                errors: 0,
            }
        }
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct RoundReceiverResults {
        pub remote_id: RemoteId,
        pub round_id: RoundId,
        pub data_by_sender: BTreeMap<u16, TrafficData>,
        /// Packets with the wrong round id or the wrong sender id.
        pub errors: u64,
    }

    impl RoundReceiverResults {
        pub fn new(remote_id: RemoteId, round_id: RoundId, upstream: &Vec<RemoteId>) -> Self {
            Self {
                remote_id,
                round_id,
                data_by_sender: upstream.iter().map(|i| (*i,TrafficData{ packets: 0, bytes: 0 })).collect(),
                errors: 0,
            }
        }
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct ExperimentPlan {
        pub rounds: Vec<RoundPlan>,
        pub recipients: BTreeMap<RemoteId, BTreeSet<RemoteId>>,
    }

    pub fn rounds_with_varying_counts(
        duration: Duration,
        pause: Duration,
        packet_rates_per_ms: impl Iterator<Item = u16>,
        ) -> Vec<RoundPlan> {
        packet_rates_per_ms
            .map(|c| RoundPlan {
                packets_per_ms: c,
                duration: duration.clone(),
                pause: pause.clone(),
            })
        .collect()
    }
    pub fn rounds_with_range_of_counts(
        duration: Duration,
        pause: Duration,
        rounds: u16,
        max_packets_per_ms: u16,
        ) -> Vec<RoundPlan> {
        rounds_with_varying_counts(
            duration,
            pause,
            (1..=rounds)
            .map(|i| ((i as u16 * max_packets_per_ms) as f64 / rounds as f64) as u16),
            )
    }

    pub fn recipients_complete(n_remotes: u16) -> BTreeMap<RemoteId, BTreeSet<RemoteId>> {
        (0..n_remotes).map(|id| {
            (id, (0..id).chain((id + 1)..n_remotes).collect())
        }).collect()
    }
}

use experiment::{RoundSenderResults,RoundReceiverResults};

#[derive(Serialize, Debug, Clone)]
pub struct LambdaResult {
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum StateUpdate {
    Connecting {
        unconnected: u16,
        desired: u16,
    },
    Connected,
    InRound {
        id: RoundId,
        packets_s: u64,
        packets_r: u64,
    },
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum LocalTcpMessage {
    MyAddress(SocketAddr, RemoteId, String),
    /// Sender, one which remains to be confirmed
    State(StateUpdate),
    AllConfirmed,
    Stats(Vec<RoundSenderResults>, Vec<RoundReceiverResults>),
    Error(String),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum RemoteTcpMessage {
    Die,
    AllAddrs(BTreeMap<RemoteId,SocketAddr>),
    Start
}
