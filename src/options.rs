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
  --packets <arg>   the rate of packets (p/ms) sent by the each sender in the
                    final round. The prior rounds uses linearly interpolated rates
                    [default: 5]
  --sleep <arg>     how long the senders sleep between rounds [default: 2]
  --duration <arg>  how long each round should last, seconds [default: 8]
  --port <arg>      which port to use for UDP communication [default: 50000]
  --extra <arg>     start this many extra workers, which do nothing [default: 0]

Examples:
  local complete --rounds 4 --packets 20 --extra 10 5 myexp

  Tests a complete topology on 5 nodes, where there are 10 extra dummy nodes
  and 4 rounds during which packets are sent at 5, 10, 15, and 20 packets per
  ms.
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
