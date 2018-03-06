package main

import (
	"github.com/streadway/amqp"
	"strconv"
	"time"
)

type AmqSender struct {
	Host       string
	Port       int
	User       string
	Password   string
	QueueName  string
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
}

func NewAmqSender(host string, port int, user string, pass string, queue string) (*AmqSender, error) {
	var err error
	a := &AmqSender{
		Host:      host,
		Port:      port,
		User:      user,
		Password:  pass,
		QueueName: queue,
	}

	url := "amqp://" + user + ":" + pass + "@" + host + ":" + strconv.Itoa(port) + "/"
	//"amqp://sensu:cisco123@127.0.0.1:5672/"
	a.connection, err = amqp.Dial(url)
	if err != nil {
		logger.Printf("Failed to connect to RabbitMQ")
		return nil, err
	}

	a.channel, err = a.connection.Channel()
	if err != nil {
		logger.Printf("Failed to open a channel")
		return nil, err
	}

	a.queue, err = a.channel.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		logger.Printf("Failed to declare a queue")
		return nil, err
	}

	return a, err
}

func (a *AmqSender) Close() {
	if a.connection != nil {
		a.connection.Close()
	}
	if a.channel != nil {
		a.channel.Close()
	}
}

func (a *AmqSender) Send(message string) error {
	err := a.channel.Publish(
		"",           // exchange
		a.queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(message),
		})
	return err
}

func (a *AmqSender) SendBatch(count int, period time.Duration) error {
	var msg string

	msggen, err := NewMsgGenerator()
	if err != nil {
		return err
	}
	defer timeTrack(time.Now(), "AmqSender.SendBatch()")

	for i := 0; i < count; i++ {
		msg, err = msggen.GetMessage()
		if err != nil {
			return err
		}
		err = a.Send(msg)
		if err != nil {
			return err
		}
		if period > 0 {
			time.Sleep(time.Duration(period))
		}
	}

	return err
}
