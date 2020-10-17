package gonsumer

import (
	"fmt"

	"github.com/streadway/amqp"
)

type Connection struct {
	UserName string
	Password string
	HostName string
}

func (c Connection) getHost() string {
	return fmt.Sprintf(
		"amqp://%s:%s@%s:5672/",
		c.UserName,
		c.Password,
		c.HostName,
	)
}

func (c Connection) getConnection() (*amqp.Connection, error) {
	conn, err := amqp.Dial(c.getHost())
	if err != nil {
		return nil, err
	}

	return conn, nil
}
