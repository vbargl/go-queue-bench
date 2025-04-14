package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"taleoftwoqueues.bench/protocols/reqrep"

	_ "go.nanomsg.org/mangos/v3/transport/all"
)

var (
	tcpServerEndpoint    = "tcp://127.0.0.1:4456"
	inprocServerEndpoint = "inproc://127.0.0.1:4456"
	ipcServerEndpoint    = "ipc://./output/p4455.sock"
)

type ClientBuilder func(serverEndpoint string) (reqrep.Client, error)

func client(serverEndpoint string, clientBuilder ClientBuilder, loops int64) {
	c, err := clientBuilder(serverEndpoint)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	for i := int64(0); i < loops; i++ {
		if err = c.Publish("ping " + strconv.FormatInt(i, 10)); err != nil {
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

type BrokerBuilder func(serverEndpoint string) (reqrep.Broker, error)

func server(serverEndpoint string, brokerBuilder BrokerBuilder) reqrep.Broker {
	b, err := brokerBuilder(serverEndpoint)
	if err != nil {
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

			e = b.Publish("pong " + parts[1])
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

	// protocol
	useIPC    = flag.Bool("ipc", false, "Use IPC protocol")
	useTCP    = flag.Bool("tcp", false, "Use TCP protocol")
	useInproc = flag.Bool("inproc", false, "Use Inproc protocol")
)

func main() {
	var (
		clientBuilder ClientBuilder
		brokerBuilder BrokerBuilder

		serverEndpoint string
	)

	_ = os.RemoveAll("./output")
	_ = os.Mkdir("./output", os.FileMode(0755))

	flag.Parse()

	switch {
	case *useZmq:
		clientBuilder = reqrep.NewZMQClient
		brokerBuilder = reqrep.NewZMQBroker
	case *useMangos:
		clientBuilder = reqrep.NewMangosClient
		brokerBuilder = reqrep.NewMangosBroker
	default:
		flag.Usage()
		return
	}

	switch {
	case *useIPC:
		serverEndpoint = ipcServerEndpoint
	case *useTCP:
		serverEndpoint = tcpServerEndpoint
	case *useInproc:
		serverEndpoint = inprocServerEndpoint
	default:
		flag.Usage()
		return
	}

	b := server(serverEndpoint, brokerBuilder)
	defer b.Close()

	wg := sync.WaitGroup{}
	wg.Add(*clients)

	for i := 0; i < *clients; i++ {
		go func() {
			defer wg.Done()
			client(serverEndpoint, clientBuilder, int64(*loops))
		}()
	}

	wg.Wait()
}
