# Network Experiments

This directory contains data from 2 experiments:
  1. **dipair-50**
     50 (sender, receiver) pairs were created (randomly, with 50 dummies
     sprinkled in) and then tried to to send 1, 2, 3, 4, 5, 6, 7, and 8 packets
     per ms. Each sender only send packets to its designated receiver.
  2. **bipartite-50**
     50 senders and 50 receivers were created (randomly, with 50 dummies
     sprinkled in) and then the senders tried to send 1, 2, 3, 4, 5, and 6
     packets per ms. Each sender send to all receivers in a round-robin.

The network can be represented by a directed graph where an edge (u, v)
represents "u sent packets to v".

## Data

There are two data files for each experiment.

### Links

Rows are keyed by (from: NodeId, to: NodeId, round: RoundId). They describe the
data transfered along an edge of a the graph during a round.

Fields:
* duration: duration of round in seconds
* packets_per_ms: number of packets that each sender tried to send per second
* bytes_s: number of bytes sent on that link
* bytes_r: number of bytes received on that link
* packets_s: number of packets sent on that link
* packets_r: number of packets received on that link
* senders: always 1
* receivers: always 1

### Net

Rows are keyed by (round: RoundId). They describe the average traffic during a
round.
* duration: duration of round in seconds
* packets_per_ms: number of packets that each sender tried to send per second
* rate_r: number of Mb/s per receiver (on average)
* rate_s: number of Mb/s per sender (on average)
* % sent: what % of the target send rate was acheived? Should be near 100%
* % loss: what % of the sent data was lost? Hopefully near 0.
