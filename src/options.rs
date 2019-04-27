use docopt::Docopt;

const USAGE: &'static str = "
Complete Lambda Traffic Experiment

Usage:
  local bipartite [options] <senders> <receivers> <exp-name>
  local complete [options] <remotes> <exp-name>
  local dipairs [options] <pairs> <exp-name>
  local bipairs [options] <pairs> <exp-name>
  local (-h | --help)

Arguments:
  remotes           the number of remotes to create
  senders           the number of remotes that should send
  receivers         the number of remotes that should receive
  pairs             the number of pairs to create
  exp-name          the name of this experiment

Options:
  -h --help         Show this screen.
  --rounds <arg>    the number of rounds to run in this experiment [default: 1]
  --packets <arg>   the maximum rate of packets that should be sent by one
                    sender in one round, packets/ms [default: 5]
  --sleep <arg>     how long the senders sleep between rounds [default: 2]
  --duration <arg>  how long each round should last, seconds [default: 8]
  --port <arg>      which port to use for UDP communication [default: 50000]
  --extra <arg>     start this many extra workers, which do nothing [default: 0]
";

#[derive(Debug, Deserialize)]
pub struct Args {
    pub cmd_bipartite: bool,
    pub cmd_complete: bool,
    pub cmd_dipairs: bool,
    pub cmd_bipairs: bool,
    pub arg_remotes: Option<u16>,
    pub arg_senders: Option<u16>,
    pub arg_receivers: Option<u16>,
    pub arg_pairs: Option<u16>,
    pub arg_exp_name: String,
    pub flag_rounds: u16,
    pub flag_packets: u16,
    pub flag_sleep: u16,
    pub flag_duration: u16,
    pub flag_port: u16,
    pub flag_extra: u16,
}

pub fn parse_args() -> Args {
    Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit())
}
