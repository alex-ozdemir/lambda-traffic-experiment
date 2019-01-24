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
        pub round_i: u16,
        pub interval: Duration,
        pub packets_per_interval: u16,
        pub duration: Duration,
    }

    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct RoundSenderResults {
        pub plan: RoundPlan,
        pub first_packet_id: usize,
        pub packets_sent: usize,
    }
    #[derive(Deserialize, Serialize, Debug, Clone)]
    pub struct ExperimentReceiverResults {
        pub packets_recieved: Vec<usize>,
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
    ReceiverAddr(SocketAddr),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum ReceiverMessage {
}
