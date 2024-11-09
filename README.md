# Leader Lease
## Introduction
This assignment focuses on implementing a modified Raft system, similar to those used by geo-distributed database clusters such as CockroachDB or YugabyteDB. Raft is a consensus algorithm designed for distributed systems to ensure fault tolerance and consistency. It operates through leader election, log replication, and the commitment of entries across a cluster of nodes.

The goal is to build a database that stores key-value pairs, mapping strings (keys) to strings (values). The Raft cluster maintains this database and ensures fault tolerance and strong consistency. The client reliably requests the server to perform operations on this database.

## Raft Modification (for Faster Reads)
Traditionally, Raft requires the leader to exchange a heartbeat with a majority of peers before responding to a read request. If there are `n` nodes in the cluster, each read operation costs O(n). By requiring these heartbeats, Raft introduces a network hop between peers in a read operation, leading to higher read latencies, especially in multi-region geo-distributed databases where nodes are physically far apart.

### Leader Lease
A time-based “lease” for Raft leadership can be used, propagated through the heartbeat mechanism. With well-synchronized clocks, linearizable reads can be achieved without the round-trip latency penalty. This is made possible by the concept of **Leases**.

_This animation shows how leader leases work._

**What is a Leader Lease?**

Leases can be considered as tokens valid for a specific period, known as the lease duration. A node can serve read and write requests from the client only if it holds a valid token or lease.

In Raft, there is the possibility of multiple leaders (which is why even read requests require a quorum to fetch the latest value), but leases are designed so that only one lease exists at any time. Only leader nodes can acquire a lease, hence the term **Leader Lease**.

## Behavior of Leader & Follower

- The **leader** acquires and renews the lease through its heartbeat mechanism. When the leader acquires or renews its lease, it initiates a countdown for the lease duration. If the leader cannot renew its lease within this countdown, it steps down from its leadership role.
- The leader also propagates the end time of the acquired lease in its heartbeat. All **follower nodes** track this leader lease timeout, using this information in the next election process.

## Leader Election
During a leader election, a voter must propagate the old leader’s lease timeout (known to that voter) to the new candidate it is voting for. Upon receiving a majority of votes, the new leader must wait until the longest old leader’s lease duration expires before acquiring its own lease. The old leader steps down and no longer functions as a leader once its leader lease expires.
