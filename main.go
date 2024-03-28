package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

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
	n.Handle("broadcast_ok", manager.HandleBroadcastOk)
	n.Handle("read", manager.HandleRead)
	n.Handle("topology", manager.HandleTopology)
	n.Handle("gossip", manager.HandleGossip)

	go manager.Gossip()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type MessageManager struct {
	n         *maelstrom.Node
	seenMsgs  []int
	neighbors []string
}

func New(n *maelstrom.Node) MessageManager {
	return MessageManager{n: n, seenMsgs: make([]int, 0)}
}

type MsgType string

const (
	Generate    MsgType = "generate"
	GenerateOk  MsgType = "generate_ok"
	Broadcast   MsgType = "broadcast"
	BroadcastOk MsgType = "broadcast_ok"
	Read        MsgType = "read"
	ReadOk      MsgType = "read_ok"
	Topology    MsgType = "topology"
	TopologyOk  MsgType = "topology_ok"
	Gossip      MsgType = "gossip"
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
	Type string `json:"type"`
	Msg  int    `json:"message"`
}

type BroadcastOkMsgBody struct {
	Type MsgType `json:"type"`
}

func (m *MessageManager) HandleBroadcast(msg maelstrom.Message) error {
	body := &BroadcastMsgBody{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	for _, node := range m.neighbors {
		if node == msg.Src {
			continue
		}
		err := m.n.Send(node, body)
		if err != nil {
			return err
		}
	}
	m.seenMsgs = append(m.seenMsgs, body.Msg)
	return m.n.Reply(msg, &BroadcastOkMsgBody{Type: BroadcastOk})
}

func (m *MessageManager) propagateBroadcast(body *BroadcastMsgBody) error {
	for _, node := range m.neighbors {
		err := m.n.Send(node, body)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MessageManager) HandleBroadcastOk(msg maelstrom.Message) error {
	return nil
}

type ReadOkMsgBody struct {
	Type MsgType `json:"type"`
	Msgs []int   `json:"messages"`
}

func (m *MessageManager) HandleRead(msg maelstrom.Message) error {
	return m.n.Reply(msg, &ReadOkMsgBody{Type: ReadOk, Msgs: m.seenMsgs})
}

type TopologyBody struct {
	Topology map[string][]string `json:"topology"`
	Type     MsgType             `json:"type"`
}

type TopologyOkMsgBody struct {
	Type MsgType `json:"type"`
}

func (m *MessageManager) HandleTopology(msg maelstrom.Message) error {
	if len(m.neighbors) > 0 {
		// then we've already built the topology
		return m.n.Reply(msg, &TopologyOkMsgBody{Type: TopologyOk})
	}
	self := msg.Dest
	body := &TopologyBody{}
	if err := json.Unmarshal(msg.Body, body); err != nil {
		return err
	}
	for node, neighbors := range body.Topology {
		if node != self {
			continue
		}
		m.neighbors = neighbors
		break
	}
	return m.n.Reply(msg, &TopologyOkMsgBody{Type: TopologyOk})
}

// Implementing recurring gossip event
type GossipBody struct {
	Type MsgType `json:"type"`
	Msgs []int   `json:"msgs"`
}

func (m *MessageManager) Gossip() error {
	for {
		if m.n.ID() == "" {
			fmt.Fprintln(os.Stderr, "Node not initialized. Waiting...")
			time.Sleep(300 * time.Millisecond)
			continue
		}
		for _, node := range m.neighbors {
			err := m.n.Send(node, &GossipBody{Type: Gossip, Msgs: m.seenMsgs})
			if err != nil {
				return err
			}
		}
	}
}

func (m *MessageManager) HandleGossip(msg maelstrom.Message) error {
	return nil
}
