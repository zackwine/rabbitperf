package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type RabbitPerfCfg struct {
	Host        string
	Port        int
	User        string
	Pass        string
	Queue       string
	Vhost       string
	MsgCount    int
	MsgInterval int
}

type RabbitPerfCfgs struct {
	Configs []RabbitPerfCfg `tests`
}

func NewRabbitPerfCfg(filename string) (*RabbitPerfCfgs, error) {
	t := &RabbitPerfCfgs{}
	source, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(source, t)
	if err != nil {
		fmt.Printf("error: %v", err)
		return nil, err
	}
	return t, err
}
