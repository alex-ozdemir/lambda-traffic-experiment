#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::SocketAddr;

pub type RemoteId = u16;
pub type RoundId = u16;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LambdaStart {
    pub local_addr: SocketAddr,
    pub port: u16,
    pub rounds: Vec<experiment::RoundPlan>,
    pub upstream: Vec<RemoteId>,
    pub downstream: Vec<RemoteId>,
    pub remote_id: RemoteId,
    pub exp_name: String,
    pub n_remotes: u16,
}

pub mod experiment {
    use super::*;
    use std::time::Duration;

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct RoundPlan {
        pub packets_per_ms: u16,
        pub duration: Duration,
        pub pause: Duration,
    }

    #[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Debug)]
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

    impl std::default::Default for TrafficData {
        fn default() -> Self {
            TrafficData::new()
        }
    }

    impl std::ops::Add for TrafficData {
        type Output = TrafficData;
        fn add(self, other: TrafficData) -> TrafficData {
            TrafficData {
                bytes: self.bytes + other.bytes,
                packets: self.packets + other.packets,
            }
        }
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct ExperimentPlan {
        pub rounds: Vec<RoundPlan>,
        pub recipients: BTreeMap<RemoteId, Vec<RemoteId>>,
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
            (1..=rounds).map(|i| ((i as u16 * max_packets_per_ms) as f64 / rounds as f64) as u16),
        )
    }

    pub fn recipients_complete(n_remotes: u16) -> BTreeMap<RemoteId, Vec<RemoteId>> {
        (0..n_remotes)
            .map(|id| (id, (0..id).chain((id + 1)..n_remotes).collect()))
            .collect()
    }
    pub fn recipients_bipartite(senders: u16, receivers: u16) -> BTreeMap<RemoteId, Vec<RemoteId>> {
        (0..senders)
            .map(|id| (id, (senders..(receivers + senders)).collect()))
            .chain((senders..(receivers + senders)).map(|id| (id, Vec::new())))
            .collect()
    }
    pub fn recipients_dipairs(pairs: u16) -> BTreeMap<RemoteId, Vec<RemoteId>> {
        (0..pairs)
            .map(|id| (2 * id, vec![2 * id + 1]))
            .chain((0..pairs).map(|id| (2 * id + 1, Vec::new())))
            .collect()
    }
    pub fn recipients_bipairs(pairs: u16) -> BTreeMap<RemoteId, Vec<RemoteId>> {
        (0..pairs)
            .map(|id| (2 * id, vec![2 * id + 1]))
            .chain((0..pairs).map(|id| (2 * id + 1, vec![2 * id])))
            .collect()
    }

    #[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Debug)]
    pub struct LinkParams {
        pub duration: Duration,
        pub packets_per_ms: u16,
    }

    #[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Debug)]
    pub struct LinkData {
        pub sent: TrafficData,
        pub received: TrafficData,
        pub params: LinkParams,
    }

    impl LinkData {
        pub fn new(plan: &RoundPlan) -> Self {
            Self {
                sent: TrafficData::new(),
                received: TrafficData::new(),
                params: LinkParams {
                    duration: plan.duration,
                    packets_per_ms: plan.packets_per_ms,
                },
            }
        }
    }

    impl std::ops::Add for &LinkData {
        type Output = LinkData;
        fn add(self, other: &LinkData) -> LinkData {
            assert_eq!(self.params, other.params);
            LinkData {
                sent: self.sent + other.sent,
                received: self.received + other.received,
                params: self.params,
            }
        }
    }

    #[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Debug)]
    pub struct Results {
        pub links: BTreeMap<(RemoteId, RemoteId, RoundId), LinkData>,
    }

    impl Results {
        pub fn new() -> Self {
            Self {
                links: BTreeMap::new(),
            }
        }
    }

    impl std::ops::Add for Results {
        type Output = Results;
        fn add(self, other: Results) -> Results {
            let keys = self
                .links
                .keys()
                .cloned()
                .chain(other.links.keys().cloned());
            Results {
                links: keys
                    .map(|k| {
                        let a = self.links.get(&k);
                        let b = other.links.get(&k);
                        (
                            k.clone(),
                            match (a, b) {
                                (None, None) => unreachable!(),
                                (Some(a), None) => a.clone(),
                                (None, Some(b)) => b.clone(),
                                (Some(a), Some(b)) => a + b,
                            },
                        )
                    })
                    .collect(),
            }
        }
    }

    impl std::fmt::Display for Results {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            writeln!(
                f,
                "from,to,round,duration,packets_per_ms,bytes_s,bytes_r,packets_s,packets_r"
            )?;
            for (
                (from, to, round),
                LinkData {
                    sent,
                    received,
                    params,
                },
            ) in &self.links
            {
                writeln!(
                    f,
                    "{},{},{},{},{},{},{},{},{}",
                    from,
                    to,
                    round,
                    params.duration.as_millis() as f64 / 1000.0,
                    params.packets_per_ms,
                    sent.bytes,
                    received.bytes,
                    sent.packets,
                    received.packets,
                )?;
            }
            Ok(())
        }
    }

}

#[derive(Serialize, Debug, Clone)]
pub struct LambdaResult {}

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
    Error(String),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum RemoteTcpMessage {
    Die,
    AllAddrs(BTreeMap<RemoteId, SocketAddr>),
    Start,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bipartite() {
        let m = experiment::recipients_bipartite(2, 3);
        assert_eq!(m.len(), 5);
        assert_eq!(m[&0].len(), 3);
        assert_eq!(m[&1].len(), 3);
        assert_eq!(m[&2].len(), 0);
        assert_eq!(m[&3].len(), 0);
        assert_eq!(m[&4].len(), 0);
    }

    #[test]
    fn dipairs() {
        let m = experiment::recipients_dipairs(3);
        assert_eq!(m.len(), 6);
        assert_eq!(m[&0].len(), 1);
        assert_eq!(m[&1].len(), 0);
        assert_eq!(m[&2].len(), 1);
        assert_eq!(m[&3].len(), 0);
        assert_eq!(m[&4].len(), 1);
        assert_eq!(m[&5].len(), 0);
    }

    #[test]
    fn bipairs() {
        let m = experiment::recipients_bipairs(3);
        assert_eq!(m.len(), 6);
        assert_eq!(m[&0].len(), 1);
        assert_eq!(m[&1].len(), 1);
        assert_eq!(m[&2].len(), 1);
        assert_eq!(m[&3].len(), 1);
        assert_eq!(m[&4].len(), 1);
        assert_eq!(m[&5].len(), 1);
    }
}
