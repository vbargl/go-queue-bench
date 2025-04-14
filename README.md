# Queue Benchmark for Go

Simple benchmark to find out performance comparison between zmq and mangos.

## Dependencies

Following are the software and library requirements for running these
benchmarks. Code block below serves as example how to download dependencies serves
using [yay](https://github.com/Jguer/yay) on Arch Linux.

$ yay -Sy zeromq go [1]

### Message queues in comparison

1. [pebbe/zmq4](https://github.com/pebbe/zmq4)
2. [mangos/v3](https://go.nanomsg.org/mangos/v3)

## Running

To run the test simply run `go run ./bench/ -{zmq,mangos}`

## Honorable mentions

- @gdamore - [His comment](https://github.com/nanomsg/mangos/issues/76#issuecomment-278133543) helped me to lay things out.
- @eelcocramer - [His forked repository](https://github.com/eelcocramer/two-queues) also helped me to lay things out.
