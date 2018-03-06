package main

import (
	"fmt"
	"time"
)

type RabbitPerf struct {
	conf     RabbitPerfCfg
	receiver *AmqReceiver
	sender   *AmqSender
}

func runConfigFile(conffile string) error {
	configs, err := NewRabbitPerfCfg(conffile)

	if err != nil {
		logger.Printf("error: %v", err)
		return err
	}
	//logger.Printf("Parsed configs: %v", configs)

	done := make(chan *RabbitPerf)

	for index, config := range configs.Configs {
		logger.Println("Running test", index, config)
		rabbitperf := NewRabbitPerf(config)
		if err != nil {
			logger.Printf("Error creating rabbitmqperf %v", err)
			return err
		}
		go rabbitperf.run(done)
	}

	for index, _ := range configs.Configs {
		perf := <-done
		logger.Println("Finished test", index, perf.conf)
	}

	return nil
}

func NewRabbitPerf(conf RabbitPerfCfg) *RabbitPerf {
	r := &RabbitPerf{
		conf: conf,
	}
	return r
}

func (r *RabbitPerf) run(done chan *RabbitPerf) error {
	var err error

	defer func() {
		done <- r
	}()

	r.receiver, err = NewAmqReceiver(r.conf.Host, r.conf.Port, r.conf.User,
		r.conf.Pass, r.conf.Queue)
	if err != nil {
		logger.Printf("Failed create receiver: %s", err)
		return err
	}
	defer r.receiver.Close()
	r.receiver.Receive()

	r.sender, err = NewAmqSender(r.conf.Host, r.conf.Port, r.conf.User,
		r.conf.Pass, r.conf.Queue)
	if err != nil {
		logger.Printf("Failed create sender: %s", err)
		return err
	}
	defer r.sender.Close()

	err = r.sender.SendBatch(r.conf.MsgCount, time.Duration(r.conf.MsgInterval)*time.Microsecond)
	if err != nil {
		logger.Printf("Failed send: %s", err)
		panic(fmt.Sprintf("Failed send: %s", err))
	}

	r.receiver.Wait(r.conf.MsgCount, 2*time.Second)
	logger.Printf("ReceivedCount: %d, Discontinuities: %d, ErrorCount: %d", r.receiver.ReceivedCount, r.receiver.Discontinuities, r.receiver.ErrorCount)

	return err
}
