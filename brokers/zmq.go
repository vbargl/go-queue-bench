package brokers

import (
	"errors"

	zmq "github.com/pebbe/zmq4"
)

type ZMQBroker struct {
	pub *zmq.Socket
	sub *zmq.Socket
}

func NewZMQBroker(clientEndpoint, brokerEndpoint string, quiet bool) (Broker, error) {
	receiver, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		return nil, err
	}
	if err := receiver.Bind(clientEndpoint); err != nil {
		return nil, err
	}

	sender, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		return nil, err
	}
	if err := sender.Bind(brokerEndpoint); err != nil {
		return nil, err
	}

	return &ZMQBroker{sender, receiver}, nil
}

func (broker *ZMQBroker) Receive() (string, error) {
	message, err := broker.sub.Recv(0)
	if err != nil {
		return "", err
	}
	return string(message), nil
}

func (broker *ZMQBroker) Publish(message string) error {
	_, err := broker.pub.Send(message, 0)
	return err
}

func (broker *ZMQBroker) Handle(channels ...string) error {
	for _, channel := range channels {
		if err := broker.sub.SetSubscribe(channel); err != nil {
			return err
		}
	}
	return nil
}

func (broker *ZMQBroker) Close() error {
	return errors.Join(
		broker.pub.Close(),
		broker.sub.Close(),
	)
}

type ZMQClient struct {
	pub *zmq.Socket
	sub *zmq.Socket
}

func NewZMQClient(clientEndpoint, brokerEndpoint string, quite bool) (Client, error) {
	sender, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		return nil, err
	}
	if err := sender.Connect(clientEndpoint); err != nil {
		return nil, err
	}

	receiver, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		return nil, err
	}
	if err := receiver.Connect(brokerEndpoint); err != nil {
		return nil, err
	}

	return &ZMQClient{sender, receiver}, nil
}

func (client *ZMQClient) Subscribe(channels ...string) error {
	for _, channel := range channels {
		if err := client.sub.SetSubscribe(channel); err != nil {
			return err
		}
	}
	return nil
}

func (client *ZMQClient) Unsubscribe(channels ...string) error {
	for _, channel := range channels {
		if err := client.sub.SetUnsubscribe(channel); err != nil {
			return err
		}
	}
	return nil
}

func (client *ZMQClient) Publish(channel, message string) error {
	msg := channel + " " + message
	_, err := client.pub.Send(msg, 0)
	return err
}

func (client *ZMQClient) Receive() (string, error) {
	return client.sub.Recv(0)
}

func (client *ZMQClient) Close() error {
	return errors.Join(
		client.pub.Close(),
		client.sub.Close(),
	)
}
