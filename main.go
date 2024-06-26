package main

import (
	"bytes"
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"sync"
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

	// n.Handle("generate", manager.HandleGenerateId)
	n.Handle("broadcast", manager.HandleBroadcast)
	n.Handle("read", manager.HandleRead)
	n.Handle("topology", manager.HandleTopology)
	n.Handle("gossip", manager.HandleGossip)

	go manager.Gossip()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type Set map[int]struct{}

func NewSet() Set { return make(map[int]struct{}) }

type MessageManager struct {
	n                 *maelstrom.Node
	seenMsgs          Set
	neighborsSeenMsgs map[string]Set
	rwLock            sync.RWMutex
}

func New(n *maelstrom.Node) MessageManager {
	return MessageManager{n: n, seenMsgs: NewSet(), neighborsSeenMsgs: make(map[string]Set)}
}

func (m *MessageManager) GetAllMessages() []int {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	msgs := make([]int, 0, len(m.seenMsgs))
	for msg := range m.seenMsgs {
		msgs = append(msgs, msg)
	}
	return msgs
}

func (m *MessageManager) GetMessages(nodesNeighborSeen Set) []int {
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()
	msgs := make([]int, 0, len(m.seenMsgs))
	for msg := range m.seenMsgs {
		_, neighborHasSeen := nodesNeighborSeen[msg]
		shouldSendMsg := !neighborHasSeen
		if neighborHasSeen {
			randInt := rand.Intn(10)
			if randInt > 6 {
				// Send 30% of all seen messages
				shouldSendMsg = true
			}
		}
		if shouldSendMsg {
			msgs = append(msgs, msg)
		}
	}
	return msgs
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
	m.rwLock.RLock()
	if _, exists := m.seenMsgs[body.Msg]; exists {
		m.rwLock.RUnlock()
		return m.n.Reply(msg, &BroadcastOkMsgBody{Type: BroadcastOk})
	}
	m.rwLock.RUnlock()
	m.rwLock.Lock()
	m.seenMsgs[body.Msg] = struct{}{}
	m.rwLock.Unlock()
	return m.n.Reply(msg, &BroadcastOkMsgBody{Type: BroadcastOk})
}

type ReadOkMsgBody struct {
	Type MsgType `json:"type"`
	Msgs []int   `json:"messages"`
}

func (m *MessageManager) HandleRead(msg maelstrom.Message) error {
	return m.n.Reply(msg, &ReadOkMsgBody{Type: ReadOk, Msgs: m.GetAllMessages()})
}

type TopologyBody struct {
	Topology map[string][]string `json:"topology"`
	Type     MsgType             `json:"type"`
}

type TopologyOkMsgBody struct {
	Type MsgType `json:"type"`
}

func (m *MessageManager) HandleTopology(msg maelstrom.Message) error {
	if len(m.neighborsSeenMsgs) > 0 {
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
		for _, neighbor := range neighbors {
			m.neighborsSeenMsgs[neighbor] = NewSet()
		}
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
		for node, nodesNeighborSeen := range m.neighborsSeenMsgs {
			err := m.n.Send(node, &GossipBody{Type: Gossip, Msgs: m.GetMessages(nodesNeighborSeen)})
			if err != nil {
				return err
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (m *MessageManager) HandleGossip(msg maelstrom.Message) error {
	body := &GossipBody{}
	if err := json.Unmarshal(msg.Body, body); err != nil {
		return err
	}
	m.rwLock.Lock()
	defer m.rwLock.Unlock()
	for _, msgVal := range body.Msgs {
		m.seenMsgs[msgVal] = struct{}{}
		m.neighborsSeenMsgs[msg.Src][msgVal] = struct{}{}
	}
	return nil
}
