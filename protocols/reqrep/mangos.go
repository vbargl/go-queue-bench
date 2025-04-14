package reqrep

import (
	"errors"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	"go.nanomsg.org/mangos/v3/protocol/req"

	_ "go.nanomsg.org/mangos/v3/transport/inproc"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

type MangosServer struct {
	sock mangos.Socket
}

func NewMangosBroker(serverEndpoint string) (Broker, error) {
	sock, err := rep.NewSocket()
	if err != nil {
		return nil, err
	}
	if err := sock.Listen(serverEndpoint); err != nil {
		return nil, err
	}

	return &MangosServer{sock}, nil
}

func (broker *MangosServer) Receive() (string, error) {
	message, err := broker.sock.Recv()
	return string(message), err
}

func (broker *MangosServer) Publish(message string) error {
	msg := []byte(message)
	return broker.sock.Send(msg)
}

func (broker *MangosServer) Close() error {
	return errors.Join(
		broker.sock.Close(),
	)
}

type MangosClient struct {
	sock mangos.Socket
}

func NewMangosClient(serverEndpoint string) (Client, error) {
	sock, err := req.NewSocket()
	if err != nil {
		return nil, err
	}
	if err := sock.Dial(serverEndpoint); err != nil {
		return nil, err
	}

	return &MangosClient{sock}, nil
}

func (client *MangosClient) Publish(message string) error {
	msg := []byte(message)
	err := client.sock.Send(msg)
	return err
}

func (client *MangosClient) Receive() (string, error) {
	message, err := client.sock.Recv()
	return string(message), err
}

func (client *MangosClient) Close() error {
	return errors.Join(
		client.sock.Close(),
	)
}
