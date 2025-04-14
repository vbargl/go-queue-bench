package brokers

import (
	"errors"

	"go.nanomsg.org/mangos/v3"
	pub "go.nanomsg.org/mangos/v3/protocol/pub"
	sub "go.nanomsg.org/mangos/v3/protocol/sub"

	_ "go.nanomsg.org/mangos/v3/transport/inproc"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

type MangosBroker struct {
	pub mangos.Socket
	sub mangos.Socket
}

func NewMangosBroker(clientEndpoint, brokerEndpoint string, quiet bool) (Broker, error) {
	receiver, err := sub.NewSocket()
	if err != nil {
		return nil, err
	}
	if err := receiver.Listen(brokerEndpoint); err != nil {
		return nil, err
	}

	sender, err := pub.NewSocket()
	if err != nil {
		return nil, err
	}
	if err := sender.Listen(clientEndpoint); err != nil {
		return nil, err
	}

	return &MangosBroker{sender, receiver}, nil
}

func (broker *MangosBroker) Receive() (string, error) {
	message, err := broker.sub.Recv()
	return string(message), err
}

func (broker *MangosBroker) Publish(message string) error {
	msg := []byte(message)
	return broker.pub.Send(msg)
}

func (broker *MangosBroker) Handle(channels ...string) error {
	for _, channel := range channels {
		if err := broker.sub.SetOption(mangos.OptionSubscribe, channel); err != nil {
			return err
		}
	}
	return nil
}

func (broker *MangosBroker) Close() error {
	return errors.Join(
		broker.pub.Close(),
		broker.sub.Close(),
	)
}

type MangosClient struct {
	pub mangos.Socket
	sub mangos.Socket
}

func NewMangosClient(clientEndpoint, brokerEndpoint string, quite bool) (Client, error) {
	sender, err := pub.NewSocket()
	if err != nil {
		return nil, err
	}
	if err := sender.Dial(brokerEndpoint); err != nil {
		return nil, err
	}

	receiver, err := sub.NewSocket()
	if err != nil {
		return nil, err
	}
	if err := receiver.Dial(clientEndpoint); err != nil {
		return nil, err
	}

	return &MangosClient{sender, receiver}, nil
}

func (client *MangosClient) Subscribe(channels ...string) error {
	for _, channel := range channels {
		if err := client.sub.SetOption(mangos.OptionSubscribe, channel); err != nil {
			return err
		}
	}
	return nil
}

func (client *MangosClient) Unsubscribe(channels ...string) error {
	for _, channel := range channels {
		if err := client.sub.SetOption(mangos.OptionUnsubscribe, channel); err != nil {
			return err
		}
	}
	return nil
}

func (client *MangosClient) Publish(channel, message string) error {
	msg := []byte(channel + " " + message)
	err := client.pub.Send(msg)
	return err
}

func (client *MangosClient) Receive() (string, error) {
	message, err := client.sub.Recv()
	return string(message), err
}

func (client *MangosClient) Close() error {
	return errors.Join(
		client.pub.Close(),
		client.sub.Close(),
	)
}
