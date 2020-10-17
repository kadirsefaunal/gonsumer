package gonsumer

import "github.com/streadway/amqp"

type Gonsumer struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	settings ConsumerSettings
}

type ConsumerSettings struct {
	ExchangeName string
	ExchangeType string
	QueueName    string
	RoutingKey   string
}

func New(conn Connection, c ConsumerSettings) (*Gonsumer, error) {
	connection, err := conn.getConnection()
	if err != nil {
		return nil, err
	}

	ch, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	g := &Gonsumer{
		conn:     connection,
		channel:  ch,
		settings: c,
	}

	err = g.exchangeDeclare()
	if err != nil {
		return nil, err
	}

	err = g.queueDeclare()
	if err != nil {
		return nil, err
	}

	err = g.queueBind()
	if err != nil {
		return nil, err
	}

	return g, nil
}

func (g *Gonsumer) Close() {
	g.channel.Close()
	g.conn.Close()
}

func (g *Gonsumer) exchangeDeclare() error {
	return g.channel.ExchangeDeclare(
		g.settings.ExchangeName, // name
		g.settings.ExchangeType, // type
		true,                    // durable
		false,                   // auto-deleted
		false,                   // internal
		false,                   // no-wait
		nil,                     // arguments
	)
}

func (g *Gonsumer) queueDeclare() error {
	_, err := g.channel.QueueDeclare(
		g.settings.QueueName, // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)

	return err
}

func (g *Gonsumer) queueBind() error {
	return g.channel.QueueBind(
		g.settings.QueueName,    // queue name
		g.settings.RoutingKey,   // routing key
		g.settings.ExchangeName, // exchange
		false,
		nil,
	)
}

func (g *Gonsumer) Consume() (<-chan amqp.Delivery, error) {
	return g.channel.Consume(
		g.settings.QueueName, // name
		"",                   // consumerTag,
		true,                 // noAck
		false,                // exclusive
		false,                // noLocal
		false,                // noWait
		nil,                  // arguments
	)
}
