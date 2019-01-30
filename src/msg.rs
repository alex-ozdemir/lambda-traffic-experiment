#![allow(dead_code)]

use std::net::SocketAddr;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LambdaSenderStart {
    pub local_addr: SocketAddr,
    pub plan: experiment::ExperimentPlan,
    pub exp_id: u32,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LambdaReceiverStart {
    pub local_addr: SocketAddr,
    pub sender_addr: SocketAddr,
    pub exp_id: u32,
    pub dummy_id: Option<u16>,
}

pub mod experiment {
    use std::ops::Add;
    use std::time::Duration;
    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct RoundPlan {
        pub sleep_period: Duration,
        pub packets_per_ms: u16,
        pub duration: Duration,
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct RoundSenderResults {
        pub plan: RoundPlan,
        pub first_packet_id: u64,
        pub packets_sent: u64,
        pub bytes_sent: u64,
        pub errors: u64,
        pub would_blocks: u64,
        pub sleep_time: Duration,
        pub write_time: Duration,
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct ExperimentPlan {
        pub rounds: Vec<RoundPlan>,
    }

    impl ExperimentPlan {
        pub fn with_varying_counts(
            sleep_period: Duration,
            duration: Duration,
            packet_rates_per_ms: impl Iterator<Item = u16>,
        ) -> Self {
            ExperimentPlan {
                rounds: packet_rates_per_ms.map(|c| RoundPlan {
                    packets_per_ms: c,
                    duration: duration.clone(),
                    sleep_period: sleep_period.clone(),
                }).collect(),
            }
        }
    }

    impl Add for ExperimentPlan {
        type Output = Self;
        fn add(mut self, other: Self) -> Self::Output {
            self.rounds.extend(other.rounds);
            self
        }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct LambdaResult {}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum LocalTCPMessage {
    SenderPing(SocketAddr,String),
    ReceiverPing(SocketAddr,String),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum LocalMessage {
    DummyPing(SocketAddr,u16,String),
    ReceiverStats {
        packet_count: u64,
        byte_count: u64,
        errors: u64,
    },
    StartRound(experiment::RoundPlan),
    FinishRound(experiment::RoundSenderResults),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum SenderMessage {
    Die,
    ReceiverPing,
    ReceiverAddr(SocketAddr),
}
