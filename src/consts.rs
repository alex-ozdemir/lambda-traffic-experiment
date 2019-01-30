#![allow(dead_code)]

use std::time::Duration;

pub const SENDER_FUNCTION_NAME: &str = "rust-test-sender";
pub const RECEIVER_FUNCTION_NAME: &str = "rust-test-receiver";

pub const S3_BUCKET: &str = "rust-test-2";
pub const UDP_PAYLOAD_BYTES: usize = 1400;

pub const FIREWALL_HEAL_TIME: Duration = Duration::from_secs(3);
