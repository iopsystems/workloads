# async-queues

Simple workload for Rust async queues, covering both MPMC and broadcast types.

This workload consists of one or more publishers writing timestamped messages
into an async queue. One or more subscribers read messages from the queue,
calculate the amount of time from publish to receive, and update the stats
accordingly.

At the end of the run, basic metrics are reported on the command line.
Optionally, a histogram containing the full distribution of latencies can be
written out to a file.

This benchmark can be used to compare different queue implementations and how
they perform with varying numbers of publishers, subscribers, and message sizes.
It can also be used to see how the Tokio runtime configuration, such as the
number of threads and the global queue and event intervals impact the
performance.
