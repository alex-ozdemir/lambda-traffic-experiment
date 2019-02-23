use docopt::Docopt;

const USAGE: &'static str = "
Complete Lambda Traffic Experiment

Usage:
  local [options] <remotes> <exp-name>
  local (-h | --help)

Arguments:
  remotes             the number of remotes to create
  exp-name            the name of this experiment

Options:
  -h --help         Show this screen.
  --rounds <arg>    the number of rounds to run in this experiment [default: 1]
  --packets <arg>   the maximum rate of packets that should be sent by one
                    sender in one round, packets/ms [default: 5]
  --sleep <arg>     how long the senders sleep between rounds [default: 2]
  --duration <arg>  how long each round should last, seconds [default: 8]
  --port <arg>      which port to use for UDP communication [default: 50000]
";

#[derive(Debug, Deserialize)]
pub struct Args {
    pub arg_remotes: u16,
    pub arg_exp_name: String,
    pub flag_rounds: u16,
    pub flag_packets: u16,
    pub flag_sleep: u16,
    pub flag_duration: u16,
    pub flag_port: u16,
}

pub fn parse_args() -> Args {
    Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit())
}
