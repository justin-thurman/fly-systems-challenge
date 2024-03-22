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

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type Generate string

const (
	GenerateIncoming Generate = "generate"
	GenerateReply    Generate = "generate_ok"
)

type GenerateMsgBody struct {
	Type  Generate `json:"type"`
	MsgId int64    `json:"msg_id"`
}

type ReplyGenerateMsgBody struct {
	Type Generate `json:"type"`
	Id   string   `json:"id"`
}

type MessageManager struct {
	n *maelstrom.Node
}

func New(n *maelstrom.Node) MessageManager {
	return MessageManager{n: n}
}

func (m *MessageManager) HandleGenerateId(msg maelstrom.Message) error {
	src := msg.Src
	body := &GenerateMsgBody{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var out bytes.Buffer
	out.WriteString(src)
	out.WriteString(strconv.Itoa(int(body.MsgId)))

	replyBody := &ReplyGenerateMsgBody{
		Type: GenerateReply,
		Id:   out.String(),
	}
	return m.n.Reply(msg, replyBody)
}
