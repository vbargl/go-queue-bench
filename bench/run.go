package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"taleoftwoqueues.bench/brokers"

	_ "go.nanomsg.org/mangos/v3/transport/all"
)

var (
	// tcp
	clientEndpoint = "tcp://127.0.0.1:4455"
	brokerEndpoint = "tcp://127.0.0.1:4456"

	// inproc
	// clientEndpoint = "inproc://127.0.0.1:4455"
	// brokerEndpoint = "inproc://127.0.0.1:4456"

	// ipc
	// clientEndpoint = "ipc://./output/p4455.sock"
	// brokerEndpoint = "ipc://./output/p4456.sock"
)

type ClientBuilder func(senderEndpoint, receiverEndpoint string, quite bool) (brokers.Client, error)

func client(clientBuilder ClientBuilder, loops int64) {
	c, err := clientBuilder(clientEndpoint, brokerEndpoint, true)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	if err := c.Subscribe("rtt.resp"); err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	for i := int64(0); i < loops; i++ {
		if err = c.Publish("rtt", strconv.FormatInt(i, 10)); err != nil {
			panic(err.Error())
		}
		if _, err = c.Receive(); err != nil {
			panic(err.Error())
		}
	}
	end := time.Now()

	delta := float64(end.Sub(start)) / float64(time.Second)

	fmt.Printf("Client %d RTTs in %f secs (%f rtt/sec)\n",
		loops, delta, float64(loops)/delta)
}

type BrokerBuilder func(senderEndpoint, receiverEndpoint string, quiet bool) (brokers.Broker, error)

func server(brokerBuilder BrokerBuilder) brokers.Broker {
	b, err := brokerBuilder(clientEndpoint, brokerEndpoint, true)
	if err != nil {
		panic(err)
	}

	if err := b.Handle("rtt"); err != nil {
		panic(err)
	}

	go func() {
		for {
			msg, e := b.Receive()
			if e != nil {
				fmt.Println(e.Error())
				return
			}

			parts := strings.Split(msg, " ")

			e = b.Publish("rtt.resp " + parts[1])
			if e != nil {
				fmt.Println(e.Error())
				return
			}
		}
	}()

	return b
}

var (
	clients   = flag.Int("clients", 32, "Number of clients")
	loops     = flag.Int("loops", 50000, "Number of loops")
	useZmq    = flag.Bool("zmq", false, "Use zmq broker")
	useMangos = flag.Bool("mangos", false, "Use mangos broker")
)

func main() {
	var (
		clientBuilder ClientBuilder
		brokerBuilder BrokerBuilder
	)

	if strings.HasPrefix(clientEndpoint, "ipc") {
		os.Remove(clientEndpoint[4:])
	}
	if strings.HasPrefix(brokerEndpoint, "ipc") {
		os.Remove(brokerEndpoint[4:])
	}

	switch flag.Parse(); {
	case *useZmq:
		clientBuilder = brokers.NewZMQClient
		brokerBuilder = brokers.NewZMQBroker
	case *useMangos:
		clientBuilder = brokers.NewMangosClient
		brokerBuilder = brokers.NewMangosBroker
	default:
		flag.Usage()
		return
	}

	b := server(brokerBuilder)
	defer b.Close()

	wg := sync.WaitGroup{}
	wg.Add(*clients)

	for i := 0; i < *clients; i++ {
		go func() {
			defer wg.Done()
			client(clientBuilder, int64(*loops))
		}()
	}

	wg.Wait()
}
