package main

import (
	"encoding/json"
	"strconv"
	"time"
)

type MsgGenerator struct {
	msgMap map[string]string
	seqNum int
}

func NewMsgGenerator() (*MsgGenerator, error) {
	jsonMap := make(map[string]string)
	uuid, err := getUUID()
	if err != nil {
		return nil, err
	}

	jsonMap = make(map[string]string)
	jsonMap["uuid"] = uuid

	m := &MsgGenerator{
		msgMap: jsonMap,
	}

	return m, err
}

func (m *MsgGenerator) SetField(field string, value string) {
	m.msgMap[field] = value
}

func (m *MsgGenerator) ResetSeqNum() {
	m.seqNum = 0
}

func (m *MsgGenerator) GetMessage() (string, error) {

	m.msgMap["seqNum"] = strconv.Itoa(m.seqNum)
	m.seqNum++
	m.msgMap["timestamp"] = time.Now().Format(time.RFC3339)

	messagebytes, err := json.Marshal(m.msgMap)
	if err != nil {
		logger.Println(err)
		return "", err
	}

	return string(messagebytes[:]), err
}
