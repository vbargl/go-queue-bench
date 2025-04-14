package reqrep

import (
	"errors"

	zmq "github.com/pebbe/zmq4"
)

type ZMQBroker struct {
	sock *zmq.Socket
}

func NewZMQBroker(serverEndpoint string) (Broker, error) {
	sock, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		return nil, err
	}
	if err := sock.Bind(serverEndpoint); err != nil {
		return nil, err
	}

	return &ZMQBroker{sock}, nil
}

func (broker *ZMQBroker) Receive() (string, error) {
	message, err := broker.sock.Recv(0)
	if err != nil {
		return "", err
	}
	return string(message), nil
}

func (broker *ZMQBroker) Publish(message string) error {
	_, err := broker.sock.Send(message, 0)
	return err
}

func (broker *ZMQBroker) Close() error {
	return errors.Join(
		broker.sock.Close(),
	)
}

type ZMQClient struct {
	sock *zmq.Socket
}

func NewZMQClient(serverEndpoint string) (Client, error) {
	sock, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		return nil, err
	}
	if err := sock.Connect(serverEndpoint); err != nil {
		return nil, err
	}

	return &ZMQClient{sock}, nil
}

func (client *ZMQClient) Publish(message string) error {
	_, err := client.sock.Send(message, 0)
	return err
}

func (client *ZMQClient) Receive() (string, error) {
	return client.sock.Recv(0)
}

func (client *ZMQClient) Close() error {
	return errors.Join(
		client.sock.Close(),
	)
}
