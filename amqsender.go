package main

import (
	"github.com/streadway/amqp"
	"time"
)

type AmqSender struct {
	Uri        *amqp.URI
	QueueName  string
	connection *amqp.Connection
	channel    *amqp.Channel
}

func NewAmqSender(host string, port int, user string, pass string, queue string, vhost string) (*AmqSender, error) {

	uri := &amqp.URI{
		Scheme:   "amqp",
		Host:     host,
		Port:     port,
		Username: user,
		Password: pass,
		Vhost:    vhost,
	}

	connection, channel, err := AmqpSetup(uri.String(), queue)
	if err != nil {
		logger.Printf("Failed to setup amqp connection.")
		return nil, err
	}

	a := &AmqSender{
		Uri:        uri,
		QueueName:  queue,
		connection: connection,
		channel:    channel,
	}

	a.registerReconnect()

	return a, nil
}

func (a *AmqSender) reconnect() {
	var err error
	backofftime := 1 * time.Second

	logger.Printf("AmqSender -> reconnect()")
	a.connection, a.channel, err = AmqpSetup(a.Uri.String(), a.QueueName)
	for err != nil {
		logger.Printf("Failed to setup amqp connection.")
		time.Sleep(backofftime)
		backofftime = backofftime * 2
		a.connection, a.channel, err = AmqpSetup(a.Uri.String(), a.QueueName)
	}

	a.registerReconnect()
}

func (a *AmqSender) registerReconnect() {
	notifyClose := make(chan *amqp.Error)
	a.channel.NotifyClose(notifyClose)
	go func() {
		err := <-notifyClose
		if err != nil {
			logger.Printf("Connection closed %v", err)
			a.reconnect()
		}
		//logger.Printf("Bye notifyClose")
	}()

	notifyFlow := make(chan bool)
	a.channel.NotifyFlow(notifyFlow)
	go func() {
		for flow := range notifyFlow {
			logger.Printf("Received flow %b", flow)
		}
		//logger.Printf("Bye notifyFlow")
	}()

	notifyReturn := make(chan amqp.Return)
	a.channel.NotifyReturn(notifyReturn)
	go func() {
		for returnedMsg := range notifyReturn {
			logger.Printf("Received notifyReturn %v", returnedMsg.Body)
		}
		//logger.Printf("Bye notifyReturn")
	}()
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
		"",          // exchange
		a.QueueName, // routing key
		false,       // mandatory
		false,       // immediate
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
		for err != nil {
			logger.Printf("(Retry) Failed to send with error %v", err)
			time.Sleep(time.Duration(period))
			err = a.Send(msg)
		}
		if period > 0 {
			time.Sleep(time.Duration(period))
		}
	}

	return err
}
