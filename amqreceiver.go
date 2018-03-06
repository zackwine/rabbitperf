package main

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"time"
)

type AmqReceiver struct {
	Host            string
	Port            int
	User            string
	Password        string
	QueueName       string
	connection      *amqp.Connection
	channel         *amqp.Channel
	queue           amqp.Queue
	lastSeqNum      int
	ReceivedCount   int
	Discontinuities int
	ErrorCount      int
}

type AmqMessage struct {
	UUID      string
	SeqNum    string
	Timestamp string
}

func NewAmqReceiver(host string, port int, user string, pass string, queue string) (*AmqReceiver, error) {
	var err error
	a := &AmqReceiver{
		Host:      host,
		Port:      port,
		User:      user,
		Password:  pass,
		QueueName: queue,
	}

	defer func() {
		if err != nil {
			a.Close()
		}
	}()

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

	msgs, err := a.channel.Consume(
		a.queue.Name, // queue
		"",           // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		log.Printf("Failed to get consumer channel.")
		return err
	}

	go func() {
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
