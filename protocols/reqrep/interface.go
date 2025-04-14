package reqrep

type Client interface {
	Publish(message string) error
	Receive() (string, error)
	Close() error
}

type Broker interface {
	Receive() (string, error)
	Publish(message string) error
	Close() error
}
