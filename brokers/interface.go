package brokers

type Client interface {
	Subscribe(topics ...string) (err error)
	Unsubscribe(topics ...string) (err error)
	Publish(topic string, message string) error
	Receive() (string, error)
	Close() error
}

type Broker interface {
	Receive() (string, error)
	Publish(message string) error
	Handle(topic ...string) error
	Close() error
}
