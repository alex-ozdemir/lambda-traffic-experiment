#![allow(dead_code)]

use std::net::SocketAddr;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LambdaSenderStart {
    pub local_addr: SocketAddr,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LambdaReceiverStart {
    pub local_addr: SocketAddr,
    pub sender_addr: SocketAddr,
}

pub mod experiment {
    use std::time::Duration;
    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct RoundPlan {
        pub round_index: u16,
        pub burst_period: Duration,
        pub packets_per_burst: u16,
        pub duration: Duration,
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct RoundSenderResults {
        pub plan: RoundPlan,
        pub first_packet_id: u64,
        pub packets_sent: u64,
        pub bytes_sent: u64,
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct ExperimentReceiverResults {
        pub packets_recieved: Vec<u64>,
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct ExperimentPlan {
        pub rounds: Vec<RoundPlan>,
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct LambdaResult {}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum LocalMessage {
    SenderPing(SocketAddr),
    ReceiverPing(SocketAddr),
    StartRound(experiment::RoundPlan),
    FinishRound,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum SenderMessage {
    ReceiverPing,
    ReceiverAddr(SocketAddr),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum ReceiverMessage {
}
