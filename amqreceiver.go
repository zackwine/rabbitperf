package main

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"time"
)

type AmqReceiver struct {
	Uri             *amqp.URI
	QueueName       string
	connection      *amqp.Connection
	channel         *amqp.Channel
	lastSeqNum      int
	ReceivedCount   int
	Discontinuities int
	ErrorCount      int
	receiving       bool
}

type AmqMessage struct {
	UUID      string
	SeqNum    string
	Timestamp string
}

func NewAmqReceiver(host string, port int, user string, pass string, queue string, vhost string) (*AmqReceiver, error) {

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

	a := &AmqReceiver{
		Uri:        uri,
		QueueName:  queue,
		connection: connection,
		channel:    channel,
		receiving:  false,
	}

	a.registerReconnect()

	return a, nil
}

func (a *AmqReceiver) reconnect() {
	var err error
	backofftime := 1 * time.Second

	a.connection, a.channel, err = AmqpSetup(a.Uri.String(), a.QueueName)
	for err != nil {
		logger.Printf("Failed to setup amqp connection.")
		time.Sleep(backofftime)
		backofftime = backofftime * 2
		a.connection, a.channel, err = AmqpSetup(a.Uri.String(), a.QueueName)
	}

	a.registerReconnect()

	if a.receiving {
		a.Receive()
	}
}

func (a *AmqReceiver) registerReconnect() {
	notifyClose := make(chan *amqp.Error)
	a.channel.NotifyClose(notifyClose)

	go func() {
		err := <-notifyClose
		if err != nil {
			logger.Printf("Connection closed %v", err)
			a.reconnect()
		}
	}()
}

func (a *AmqReceiver) Close() {
	if a.connection != nil {
		a.connection.Close()
	}
	if a.channel != nil {
		a.channel.Close()
	}
}

func (a *AmqReceiver) parseMessage(message []byte) {
	var m AmqMessage
	var err error

	err = json.Unmarshal(message, &m)
	if err == nil {
		a.ReceivedCount++
		//log.Printf("Parsed: %v", m)
		seqNum, err := strconv.Atoi(m.SeqNum)
		if err == nil {
			if seqNum != 0 && seqNum != (a.lastSeqNum+1) {
				a.Discontinuities++
			}
			a.lastSeqNum = seqNum
		} else {
			a.ErrorCount++
		}

	} else {
		a.ErrorCount++
		log.Printf("Failed to parse message: %v", err)
	}
}

func (a *AmqReceiver) Receive() error {

	// Indicate we only want 1 message to acknowledge at a time.
	if err := a.channel.Qos(1, 0, false); err != nil {
		log.Printf("Failed to set Qos on channel.")
		return err
	}

	msgs, err := a.channel.Consume(
		a.QueueName, // queue
		"",          // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		log.Printf("Failed to get consumer channel.")
		return err
	}

	go func() {
		a.receiving = true
		defer func() {
			a.receiving = false
		}()
		for d := range msgs {
			//log.Printf("Received a message: %s", d.Body)
			a.parseMessage(d.Body)
		}
	}()

	return err
}

func (a *AmqReceiver) Wait(count int, timeout time.Duration) {
	finished := make(chan bool, 1)
	go func() {
		for (a.lastSeqNum + 1) < count {
			time.Sleep(50 * time.Millisecond)
		}
		finished <- true
	}()

	select {
	case <-finished:
		//log.Printf("Received all messages.")
		return
	case <-time.After(timeout):
		log.Printf("Timeout waiting on all messages.")
	}
}
