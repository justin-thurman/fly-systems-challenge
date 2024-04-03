package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	manager := New(n)

	n.Handle("init", manager.HandleInit)
	n.Handle("add", manager.HandleAdd)
	n.Handle("read", manager.HandleRead)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type MessageManager struct {
	n *maelstrom.Node
	*maelstrom.KV
	key        string
	currentVal int
}

func New(n *maelstrom.Node) *MessageManager {
	manager := &MessageManager{n: n, KV: maelstrom.NewSeqKV(n)}
	return manager
}

func (m *MessageManager) HandleInit(_ maelstrom.Message) error {
	m.key = "key"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.CompareAndSwap(ctx, m.key, 0, 0, true); err != nil {
		return nil
	}
	return nil
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

func (m *MessageManager) readValue() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	val, err := m.ReadInt(ctx, m.key)
	if err != nil {
		log.Fatal(err)
	}
	return val, nil
}

func (m *MessageManager) HandleAdd(msg maelstrom.Message) error {
	body := &AddBody{}
	if err := json.Unmarshal(msg.Body, body); err != nil {
		return err
	}
	if body.Delta == 0 {
		return m.n.Reply(msg, AddOkBody{Type: AddOk})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		currentVal, err := m.readValue()
		if err != nil {
			return err
		}
		updatedVal := currentVal + body.Delta
		err = m.CompareAndSwap(ctx, m.key, currentVal, updatedVal, true)
		if err == nil {
			break
		}
	}
	return m.n.Reply(msg, AddOkBody{Type: AddOk})
}

func (m *MessageManager) HandleRead(msg maelstrom.Message) error {
	val, err := m.readValue()
	if err != nil {
		return err
	}
	// m.currentVal = val
	return m.n.Reply(msg, ReadOkBody{Type: ReadOk, Value: val})
}
