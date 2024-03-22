package main

import (
	"bytes"
	"encoding/json"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	manager := New(n)

	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "echo_ok"

		return n.Reply(msg, body)
	})

	n.Handle("generate", manager.HandleGenerateId)
	n.Handle("broadcast", manager.HandleBroadcast)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type MessageManager struct {
	n *maelstrom.Node
}

func New(n *maelstrom.Node) MessageManager {
	return MessageManager{n: n}
}

type MsgType string

const (
	Generate    MsgType = "generate"
	GenerateOk  MsgType = "generate_ok"
	Broadcast   MsgType = "broadcast"
	BroadcastOk MsgType = "broadcast_ok"
)

type GenerateMsgBody struct {
	MsgId int `json:"msg_id"`
}

type GenerateOkMsgBody struct {
	Type MsgType `json:"type"`
	Id   string  `json:"id"`
}

func (m *MessageManager) HandleGenerateId(msg maelstrom.Message) error {
	body := &GenerateMsgBody{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var Id bytes.Buffer
	Id.WriteString(msg.Src)
	Id.WriteString(strconv.Itoa(body.MsgId))

	replyBody := &GenerateOkMsgBody{
		Type: GenerateOk,
		Id:   Id.String(),
	}
	return m.n.Reply(msg, replyBody)
}

type BroadcastMsgBody struct {
	Msg int `json:"message"`
}

type BroadcastOkMsgBody struct {
	Type MsgType `json:"type"`
}

func (m *MessageManager) HandleBroadcast(msg maelstrom.Message) error {
	body := &BroadcastMsgBody{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	// TODO: store the value
	return m.n.Reply(msg, &BroadcastOkMsgBody{Type: BroadcastOk})
}
