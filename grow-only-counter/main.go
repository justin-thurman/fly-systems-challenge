package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	manager := New(n)

	n.Handle("add", manager.HandleAdd)
	n.Handle("read", manager.HandleRead)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type MessageManager struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
}

func New(n *maelstrom.Node) MessageManager {
	manager := MessageManager{n: n, kv: maelstrom.NewSeqKV(n)}
	manager.kv.Write // TODO: set up context stuff; probably need a Background() context to kick things off
}

type MsgType string

const (
	Add    MsgType = "add"
	AddOk  MsgType = "add_ok"
	Read   MsgType = "read"
	ReadOk MsgType = "read_ok"
)

type AddBody struct {
	Type  MsgType `json:"type"`
	Delta int     `json:"delta"`
}

type AddOkBody struct {
	Type MsgType `json:"type"`
}

type ReadOkBody struct {
	Type  MsgType `json:"type"`
	Value int     `json:"value"`
}

func (m *MessageManager) HandleAdd(msg maelstrom.Message) error {
	body := &AddBody{}
	if err := json.Unmarshal(msg.Body, body); err != nil {
		return err
	}
	return nil
}

func (m *MessageManager) HandleRead(msg maelstrom.Message) error {
	return nil
}
