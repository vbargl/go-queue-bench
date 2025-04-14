package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"taleoftwoqueues.bench/protocols/pubsub"

	_ "go.nanomsg.org/mangos/v3/transport/all"
)

var (
	tcpClientEndpoint = "tcp://127.0.0.1:4455"
	tcpBrokerEndpoint = "tcp://127.0.0.1:4456"

	inprocClientEndpoint = "inproc://127.0.0.1:4455"
	inprocBrokerEndpoint = "inproc://127.0.0.1:4456"

	// ipc
	ipcClientEndpoint = "ipc://output/p4455.sock"
	ipcBrokerEndpoint = "ipc://output/p4456.sock"
)

type ClientBuilder func(clientEndpoint, brokerEndpoint string, quite bool) (pubsub.Client, error)

func client(clientEndpoint, brokerEndpoint string, clientBuilder ClientBuilder, loops int64) {
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

type BrokerBuilder func(clientEndpoint, serverEndpoint string, quiet bool) (pubsub.Broker, error)

func server(clientEndpoint, brokerEndpoint string, brokerBuilder BrokerBuilder) pubsub.Broker {
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
	clients = flag.Int("clients", 8, "Number of clients")
	loops   = flag.Int("loops", 50000, "Number of loops")

	// implementation
	useZmq    = flag.Bool("zmq", false, "Use zmq broker")
	useMangos = flag.Bool("mangos", false, "Use mangos broker")

	// protocols
	useIPC    = flag.Bool("ipc", false, "Use IPC")
	useTCP    = flag.Bool("tcp", false, "Use TCP")
	useInproc = flag.Bool("inproc", false, "Use Inproc")
)

func main() {
	var (
		clientBuilder ClientBuilder
		brokerBuilder BrokerBuilder

		clientEndpoint, brokerEndpoint string
	)

	_ = os.RemoveAll("./output")
	_ = os.Mkdir("./output", os.FileMode(0755))

	switch flag.Parse(); {
	case *useZmq:
		clientBuilder = pubsub.NewZMQClient
		brokerBuilder = pubsub.NewZMQBroker
	case *useMangos:
		clientBuilder = pubsub.NewMangosClient
		brokerBuilder = pubsub.NewMangosBroker
	default:
		flag.Usage()
		return
	}

	switch {
	case *useIPC:
		clientEndpoint, brokerEndpoint = ipcClientEndpoint, ipcBrokerEndpoint
	case *useTCP:
		clientEndpoint, brokerEndpoint = tcpClientEndpoint, tcpBrokerEndpoint
	case *useInproc:
		clientEndpoint, brokerEndpoint = inprocClientEndpoint, inprocBrokerEndpoint
	default:
		flag.Usage()
		return
	}

	b := server(clientEndpoint, brokerEndpoint, brokerBuilder)
	defer b.Close()

	wg := sync.WaitGroup{}
	wg.Add(*clients)

	for i := 0; i < *clients; i++ {
		go func() {
			defer wg.Done()
			client(clientEndpoint, brokerEndpoint, clientBuilder, int64(*loops))
		}()
	}

	wg.Wait()
}
