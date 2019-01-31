use docopt::Docopt;

const USAGE: &'static str = "
Lambda Traffic Experiment

Usage:
  local [options] <senders> <receivers>
  local (-h | --help)

Arguments:
  senders             the number of senders to create
  receivers           the number of receivers to create

Options:
  -h --help         Show this screen.
  --rounds <arg>    the number of rounds to run in this experiment [default: 6]
  --packets <arg>   the maximum rate of packets that should be sent by one
                    sender in one round, packets/ms [default: 50]
  --sleep <arg>     how long the senders sleep when they're caught up, ms
                    [default: 5]
  --duration <arg>  how long each round should last, seconds [default: 8]
";

#[derive(Debug, Deserialize)]
pub struct Args {
    pub arg_senders: u8,
    pub arg_receivers: u8,
    pub flag_rounds: u8,
    pub flag_packets: u16,
    pub flag_sleep: u16,
    pub flag_duration: u16,
}

pub fn parse_args() -> Args {
    Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit())
}
