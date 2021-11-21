package pubsub

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"math"
	"sort"
	"time"
)

const (
	JmpSubID = protocol.ID("/jmpsub/1.0.0")
)

const (
	PUSH uint64 = iota
	PULL

	UPDATED bool = true
)

var (
	JmpMinFan                = 6
	JmpMaxFan                = 6
	JmpMaxMsgBuf             = 30
	JmpMaxHistory            = 120 // gossipsub = 5000
	JMPMaxGenerateMsg        = 5000
	JmpInitialDelay          = 100 * time.Millisecond
	JmpCycleInterval         = 1000 * time.Millisecond // gossipsub = 1 * sec
	JmpSentPeerMaintainCycle = 4
)

type JmpSubParams struct {
	MinFan                int
	MaxFan                int
	MaxMsgBuf             int
	MaxHistoryBuf         int
	MaxGenerateMsg        int
	InitialDelay          time.Duration
	CycleInterval         time.Duration
	SentPeerMaintainCycle int
}

type JamMaxPair struct {
	jam int
	max int
}

type JmpMessage struct {
	*pb.Message
	source    peer.ID
	sender    peer.ID
	msgNumber int
}

type JmpMsgBuf struct {
	msgBuf []*JmpMessage
	msgJmp *JamMaxPair
	source peer.ID
}

func NewJmpSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	params := DefaultJmpSubParams()
	rt := &JmpSubRouter{
		peers:        make(map[peer.ID]protocol.ID),
		historyJMP:   make(map[peer.ID]*JamMaxPair),
		gossipJMP:    make(map[peer.ID]*JamMaxPair),
		history:      make(map[peer.ID][]*JmpMessage),
		msgBuf:       make(map[peer.ID]*JmpMsgBuf),
		myMsg:        make([]*JmpMessage, 0, params.MaxGenerateMsg),
		isMsgUpdated: make(chan bool, params.MaxFan),
		mcache:       NewMessageCache(params.MinFan, params.MaxHistoryBuf),
		protos:       []protocol.ID{JmpSubID},
		params:       params,
	}

	rt.membershipRouter.rt = rt

	// opts = append(opts, )
	return NewPubSub(ctx, h, rt, opts...)
}

func DefaultJmpSubParams() JmpSubParams {
	return JmpSubParams{
		MinFan:                JmpMinFan,
		MaxFan:                JmpMaxFan,
		MaxMsgBuf:             JmpMaxMsgBuf,
		MaxHistoryBuf:         JmpMaxHistory,
		MaxGenerateMsg:        JMPMaxGenerateMsg,
		InitialDelay:          JmpInitialDelay,
		CycleInterval:         JmpCycleInterval,
		SentPeerMaintainCycle: JmpSentPeerMaintainCycle,
	}
}

type JmpSubRouter struct {
	// PubSub 과 상호참조
	p      *PubSub
	peers  map[peer.ID]protocol.ID
	protos []protocol.ID

	// Jam Max Pair
	historyJMP map[peer.ID]*JamMaxPair
	gossipJMP  map[peer.ID]*JamMaxPair

	// for msg caching
	history map[peer.ID][]*JmpMessage
	msgBuf  map[peer.ID]*JmpMsgBuf

	// Messages I made
	myMsg []*JmpMessage

	// If you have accepted the message, then true
	isMsgUpdated chan bool

	mcache *MessageCache

	// for track
	tracer *pubsubTracer

	params JmpSubParams

	membershipRouter
}

func (js *JmpSubRouter) Protocols() []protocol.ID {
	return js.protos
}

func (js *JmpSubRouter) Attach(p *PubSub) {
	// init func
	js.p = p
	js.tracer = p.tracer

	// start using the same msg ID function as PubSub for caching messages.
	js.mcache.SetMsgIdFn(p.msgID)

	// start the cycle
	go js.nextCycle()

	// set JMP
	if js.gossipJMP[js.p.signID] == nil {
		js.gossipJMP[js.p.signID] = &JamMaxPair{jam: 0, max: 0}
	}
	if js.historyJMP[js.p.signID] == nil {
		js.historyJMP[js.p.signID] = &JamMaxPair{jam: 0, max: 0}
	}
}

func (js *JmpSubRouter) AddPeer(p peer.ID, proto protocol.ID) {
	//js.membershipRouter.AddPeer(p, proto)

	log.Debugf("PEERUP: Add new peer %s using %s", p, proto)
	js.tracer.AddPeer(p, proto)
	js.peers[p] = proto

}

func (js *JmpSubRouter) RemovePeer(p peer.ID) {
	//js.membershipRouter.RemovePeer(p)

	log.Debugf("PEERDOWN: Remove disconnected peer %s", p)
	js.tracer.RemovePeer(p)
	delete(js.peers, p)

	// something to delete
}

func (js *JmpSubRouter) EnoughPeers(topic string, suggested int) bool {
	// check all peers in the topic
	tmap, ok := js.p.topics[topic]
	if !ok {
		return false
	}

	if suggested == 0 {
		suggested = JmpMinFan
	}

	if len(tmap) >= suggested {
		return true
	}

	return false
}

func (js *JmpSubRouter) AcceptFrom(p peer.ID) AcceptStatus {
	return AcceptAll
}

func (js *JmpSubRouter) HandleRPC(rpc *RPC) {
	sender := peer.ID(rpc.GetSender())
	jmpRPCs := rpc.GetJmpRPC()
	if jmpRPCs == nil {
		return
	}

	// do receive func
	// msgs 중에서 중복 제외하고 내 msg 버퍼에 추가
	forDuplicatedCheck := js.HandelJmpRPC(sender, jmpRPCs)

	if *rpc.JmpMode == PUSH {
		// 4a) loading pull gossip
		pullBuf := js.loadPullGossip(forDuplicatedCheck)
		out := js.rpcWithMsgBufs(pullBuf, PULL)

		if out != nil {
			js.sendRPC(sender, out)
		}
	}
}

func (js *JmpSubRouter) loadPullGossip(recv map[peer.ID][]*JmpMessage) map[peer.ID]*JmpMsgBuf {
	pullBuf := make(map[peer.ID]*JmpMsgBuf)

	for src, gjmp := range js.gossipJMP {
		tempJmp := gjmp

		history := js.history[src]
		if history == nil {
			pullBuf[src] = &JmpMsgBuf{msgJmp: &JamMaxPair{jam: 0, max: tempJmp.max}, msgBuf: nil, source: src}
			continue
		}

		msgBuf := make([]*JmpMessage, 0, js.params.MaxMsgBuf)
		for _, msg := range history {
			if tempJmp.jam < msg.msgNumber && msg.msgNumber <= tempJmp.max {
				msgBuf = append(msgBuf, msg)
			}
		}

		// find diff history & receive msgBuf
		tempMap := make(map[*JmpMessage]struct{}, len(recv[src]))
		for _, except := range recv[src] {
			tempMap[except] = struct{}{}
		}
		var diff []*JmpMessage
		for _, include := range msgBuf {
			if _, found := tempMap[include]; !found {
				diff = append(diff, include)
			}
		}

		tempJmp.jam = js.historyJMP[src].jam
		tempJmp.max = js.gossipJMP[src].max

		pullBuf[src] = &JmpMsgBuf{msgJmp: tempJmp, msgBuf: diff, source: src}
	}

	return pullBuf
}

func (js *JmpSubRouter) HandelJmpRPC(sender peer.ID, jmpRPCs []*pb.JmpMsgRPC) map[peer.ID][]*JmpMessage {
	forDuplicatedCheck := make(map[peer.ID][]*JmpMessage)

	for _, jmpRPC := range jmpRPCs {
		recvJmpMsgs := jmpRPC.JmpMsgs
		recvJMP := jmpRPC.MsgJamMaxPair
		recvSrc := peer.ID(jmpRPC.Source)

		var jmpMsgs []*JmpMessage
		for _, msg := range recvJmpMsgs {
			jmpMsgs = append(jmpMsgs, &JmpMessage{
				Message:   msg.MsgBuf,
				source:    recvSrc,
				msgNumber: int(*msg.MsgNumber),
				sender:    sender,
			})
		}

		// 3a)
		js.putHistory(jmpMsgs...)
		forDuplicatedCheck[recvSrc] = jmpMsgs

		// update gossipJmp
		if js.gossipJMP[recvSrc] == nil {
			js.gossipJMP[recvSrc] = &JamMaxPair{jam: 0, max: 0}
		}

		js.gossipJMP[recvSrc].jam = int(math.Min(float64(js.gossipJMP[recvSrc].jam), float64(int(*recvJMP.Jam))))
		js.gossipJMP[recvSrc].max = int(math.Max(float64(js.gossipJMP[recvSrc].max), float64(int(*recvJMP.Max))))
	}

	return forDuplicatedCheck
}

// Publish 여기서는 처음 msg 가 생성 될 때 호출 되는 함수
func (js *JmpSubRouter) Publish(msg *Message) {
	// 본인이 생성한 msg 인 경우에만 numbering 동작
	if msg.GetFrom() == js.p.host.ID() {
		// numbering 과 history 관리
		// publish 할 때는 본인이 생성한 msg 가 아닌 경우 history 에 담지 않음
		// 다른 사람이 보낸 msg 를 history 에 담는 것은 handleRPC 에서
		jmpmsg := js.numbering(msg)
		js.putHistory(jmpmsg)

		// set gossipJMP
		myID := js.p.host.ID()
		js.gossipJMP[myID].max = js.historyJMP[myID].max
	}
}

func (js *JmpSubRouter) nextCycle() {
	time.Sleep(js.params.InitialDelay)
	select {
	case js.p.eval <- js.gossip:
	case <-js.p.ctx.Done():
		return
	}

	ticker := time.NewTicker(js.params.CycleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-js.isMsgUpdated:
			select {
			case js.p.eval <- js.gossip:
			case <-js.p.ctx.Done():
				return
			}
		case <-ticker.C:
			select {
			case js.p.eval <- js.gossip:
			case <-js.p.ctx.Done():
				return
			}
		case <-js.p.ctx.Done():
			return
		}
	}
}

func (js *JmpSubRouter) gossip() {
	// 1a) loading push gossip
	pushBuf := js.loadPushGossip()

	for topic, _ := range js.p.topics {
		// subscribe 하고 있는 topic 을 대상으로 msg 를 전달
		// topic 안의 peer 중 보낼 peer 를 fanout 개수만큼 선택
		toSend := js.selectPeerWithFanout(topic)

		// 해당 topic 에 참여하고 있는 peer 가 보낸 msg 만 out 에 담음
		out := js.rpcWithMsgBufs(pushBuf, PUSH)

		if out != nil {
			for pid := range toSend {
				js.sendRPC(pid, out)
			}
		}
	}
}

func (js *JmpSubRouter) loadPushGossip() map[peer.ID]*JmpMsgBuf {
	// 각 msg src 마다 gossip js 를 기준으로 history 에 있는 msg 들을 js.msgBuf[src] 에 msg 를 담음
	pushBuf := make(map[peer.ID]*JmpMsgBuf)

	for src, gjmp := range js.gossipJMP {
		tempJmp := gjmp

		history := js.history[src]
		if history == nil {
			pushBuf[src] = &JmpMsgBuf{msgJmp: &JamMaxPair{jam: 0, max: tempJmp.max}, msgBuf: nil, source: src}
			continue
		}

		msgBuf := make([]*JmpMessage, 0, js.params.MaxMsgBuf)

		for _, msg := range history {
			if tempJmp.jam < msg.msgNumber && msg.msgNumber <= tempJmp.max {
				msgBuf = append(msgBuf, msg)
			}
		}

		tempJmp.jam = js.historyJMP[src].jam
		tempJmp.max = js.gossipJMP[src].max

		pushBuf[src] = &JmpMsgBuf{msgJmp: tempJmp, msgBuf: msgBuf, source: src}
	}

	return pushBuf
}

func (js *JmpSubRouter) rpcWithMsgBufs(msgBufs map[peer.ID]*JmpMsgBuf, mode uint64) *RPC {
	if len(msgBufs) == 0 {
		return nil
	}

	// msgBufs 에 담긴 msg 와 js 를 rpc 에 담아서 return
	var msgJmp []*pb.JmpMsgRPC

	for src, buf := range msgBufs {
		if buf == nil {
			continue
		}

		// 보낼 msg 를 rpc 에 담음
		var jmpMsgs []*pb.JmpMsgRPC_JmpMsg

		for _, msg := range buf.msgBuf {
			num := int64(msg.msgNumber)
			jmpMsgs = append(jmpMsgs, &pb.JmpMsgRPC_JmpMsg{
				MsgBuf:    msg.Message,
				MsgNumber: &num,
			})
		}

		// msgJmp 를 설정
		msgJam := int64(buf.msgJmp.jam)
		msgMax := int64(buf.msgJmp.max)

		msgJmp = append(msgJmp, &pb.JmpMsgRPC{
			JmpMsgs:       jmpMsgs,
			MsgJamMaxPair: &pb.JmpMsgRPC_JamMaxPair{Jam: &msgJam, Max: &msgMax},
			Source:        []byte(src), // src == buf.source
		})

		// 2a) After sending push-(a)
		// 5a) After sending pull-(a)
		if len(jmpMsgs) > 0 {
			js.gossipJMP[src].jam = int(*jmpMsgs[len(jmpMsgs)-1].MsgNumber)
		}
	}

	return &RPC{RPC: pb.RPC{JmpRPC: msgJmp, JmpMode: &mode, Sender: []byte(js.p.signID)}}
}

func (js *JmpSubRouter) Join(topic string) {
	//js.membershipRouter.Join(topic)

	log.Debugf("JOIN %s", topic)
	js.tracer.Join(topic)
}

func (js *JmpSubRouter) Leave(topic string) {
	//js.membershipRouter.Leave(topic)

	log.Debugf("LEAVE %s", topic)
	js.tracer.Leave(topic)
}

func (js *JmpSubRouter) numbering(msg *Message) *JmpMessage {
	jmpmsg := &JmpMessage{
		Message: msg.Message,
		source:  js.p.signID,
		sender:  js.p.signID,
	}

	if len(js.myMsg) > js.params.MaxGenerateMsg {
		js.myMsg = js.myMsg[1:]
	}

	if len(js.myMsg) == 0 {
		jmpmsg.msgNumber = 1
		js.myMsg = append(js.myMsg, jmpmsg)
	} else {
		last := js.myMsg[len(js.myMsg)-1].msgNumber
		jmpmsg.msgNumber = last + 1
		js.myMsg = append(js.myMsg, jmpmsg)
	}

	return jmpmsg
}

func (js *JmpSubRouter) putHistory(jmpMsgs ...*JmpMessage) {
	if jmpMsgs == nil {
		return
	}

	isUpdate := false

loop:
	for _, msg := range jmpMsgs {
		src := msg.source

		if his, ok := js.history[src]; ok {
			// history buf 사이즈의 최대에 도달 했을 때...
			if len(his) > js.params.MaxHistoryBuf {
				// 가장 앞의 msg 를 제거
				js.history[src] = his[1:]
			}

			// msg 가 중복되어 저장 되지 않도록 함
			for _, exist := range his {
				if msg.msgNumber == exist.msgNumber {
					js.tracer.DuplicateMessage(&Message{Message: msg.Message, ReceivedFrom: src})
					continue loop
				}
			}
		}

		if src != js.p.host.ID() {
			js.tracer.HitMessage(&Message{Message: msg.Message, ReceivedFrom: src})
		}

		js.history[src] = append(js.history[src], msg)

		// sort history -> 이걸 매번 해주는게 나을까...?
		sort.Slice(js.history[src], func(i, j int) bool {
			return js.history[src][i].msgNumber < js.history[src][j].msgNumber
		})

		js.updateHistoryJMP(src)
		isUpdate = true
	}

	if isUpdate {
		js.isMsgUpdated <- UPDATED
	}
}

func (js *JmpSubRouter) updateHistoryJMP(src peer.ID) {
	// update historyJmp
	// 앞의 부분을 못 받은 경우에도 받을 수 있도록 0 으로 setting
	js.historyJMP[src] = &JamMaxPair{jam: 0, max: 0}
	history := js.history[src]

	if len(history) > 1 {
		for i := 0; i < len(history)-1; i++ {
			// msg number 가 연속일 때, 마지막으로 연속된 number 를 jam 으로 설정
			if history[i+1].msgNumber == history[i].msgNumber+1 {
				js.historyJMP[src].jam = js.history[src][i+1].msgNumber
			} else {
				break
			}
		}
	} else {
		// msg가 하나뿐이 없을 때
		if history[0].msgNumber == 1 {
			js.historyJMP[src].jam = 1
		}
	}

	js.historyJMP[src].max = history[len(history)-1].msgNumber
}

func (js *JmpSubRouter) sendRPC(pid peer.ID, out *RPC) {
	// 받은 사람의 msg channel 을 확인
	mch, ok := js.p.peers[pid]
	if !ok {
		return
	}

	// If we're below the max message size, go ahead and send
	if out.Size() < js.p.maxMessageSize {
		js.doSendRPC(out, pid, mch)
		return
	}

	// If we're too big, fragment into multiple RPCs and send each sequentially
	outRPCs, err := fragmentRPC(out, js.p.maxMessageSize)
	if err != nil {
		js.doDropRPC(out, pid, fmt.Sprintf("unable to fragment RPC: %s", err))
		return
	}

	for _, rpc := range outRPCs {
		js.doSendRPC(rpc, pid, mch)
	}
}

func (js *JmpSubRouter) doSendRPC(rpc *RPC, pid peer.ID, mch chan *RPC) {
	select {
	case mch <- rpc:
		js.tracer.SendRPC(rpc, pid)
	default:
		js.doDropRPC(rpc, pid, "queue full")
	}
}

func (js *JmpSubRouter) doDropRPC(rpc *RPC, pid peer.ID, reason string) {
	log.Debugf("dropping message to peer %s: %s", pid.Pretty(), reason)
	js.tracer.DropRPC(rpc, pid)
}

func (js *JmpSubRouter) selectPeerWithFanout(topic string) map[peer.ID]struct{} {
	fanout := js.params.MaxFan
	toSend := make(map[peer.ID]struct{})

	tmap, ok := js.p.topics[topic]

	if !ok {
		return nil
	} else {
		if len(tmap) > fanout {
			randomPeers := js.getRandomPeersWithFanout(tmap, fanout)

			if len(randomPeers) > 0 {
				tmap = peerListToMap(randomPeers)
			}
		}
	}

	for p := range tmap {
		toSend[p] = struct{}{}
	}

	return toSend
}

func (js *JmpSubRouter) getRandomPeersWithFanout(pmap map[peer.ID]struct{}, fanout int) []peer.ID {
	peers := peerMapToList(pmap)

	shufflePeers(peers)

	if fanout > 0 && len(peers) > fanout {
		peers = peers[:fanout]
	}

	return peers
}
