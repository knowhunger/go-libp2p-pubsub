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

var (
	JmpMinFan                = 6
	JmpMaxFan                = 6
	JmpMaxMsgBuf             = 30
	JmpMaxHistory            = 120 // gossipsub = 5000
	JMPMaxGenerateMsg        = 5000
	JmpInitialDelay          = 100 * time.Millisecond
	JmpCycleInterval         = 250 * time.Millisecond
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

type JmpMsg struct {
	*pb.Message
	msgSrc    peer.ID
	msgSender peer.ID
	msgNumber int
}

type JmpMsgBuf struct {
	msgBuf []*JmpMsg
	msgJmp *JamMaxPair
	source peer.ID
}

func NewJmpSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	params := DefaultJmpSubParams()
	rt := &JmpSubRouter{
		peers:      make(map[peer.ID]protocol.ID),
		historyJMP: make(map[peer.ID]*JamMaxPair),
		gossipJMP:  make(map[peer.ID]*JamMaxPair),
		history:    make(map[peer.ID][]*JmpMsg),
		msgBuf:     make(map[peer.ID]*JmpMsgBuf),
		myMsg:      make([]*JmpMsg, 0, params.MaxGenerateMsg),
		sentPeer:   make(map[peer.ID]int),
		mcache:     NewMessageCache(params.MinFan, params.MaxHistoryBuf),
		protos:     []protocol.ID{JmpSubID},
		params:     params,
	}

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
	p     *PubSub
	peers map[peer.ID]protocol.ID

	historyJMP map[peer.ID]*JamMaxPair
	gossipJMP  map[peer.ID]*JamMaxPair

	history map[peer.ID][]*JmpMsg
	msgBuf  map[peer.ID]*JmpMsgBuf

	myMsg    []*JmpMsg
	sentPeer map[peer.ID]int

	protos []protocol.ID

	mcache *MessageCache
	tracer *pubsubTracer

	params JmpSubParams
}

func (jmp *JmpSubRouter) Protocols() []protocol.ID {
	return jmp.protos
}

func (jmp *JmpSubRouter) Attach(p *PubSub) {
	// init func
	jmp.p = p
	jmp.tracer = p.tracer

	// start using the same msg ID function as PubSub for caching messages.
	jmp.mcache.SetMsgIdFn(p.msgID)

	// start the cycle
	go jmp.nextCycle()

	// set JMP
	if jmp.gossipJMP[jmp.p.signID] == nil {
		jmp.gossipJMP[jmp.p.signID] = &JamMaxPair{jam: 0, max: 0}
	}
	if jmp.historyJMP[jmp.p.signID] == nil {
		jmp.historyJMP[jmp.p.signID] = &JamMaxPair{jam: 0, max: 0}
	}
}

func (jmp *JmpSubRouter) AddPeer(p peer.ID, proto protocol.ID) {
	log.Debugf("PEERUP: Add new peer %s using %s", p, proto)
	jmp.tracer.AddPeer(p, proto)
	jmp.peers[p] = proto

}

func (jmp *JmpSubRouter) RemovePeer(p peer.ID) {
	log.Debugf("PEERDOWN: Remove disconnected peer %s", p)
	jmp.tracer.RemovePeer(p)
	delete(jmp.peers, p)
	// something to delete
}

func (jmp *JmpSubRouter) EnoughPeers(topic string, suggested int) bool {
	// check all peers in the topic
	tmap, ok := jmp.p.topics[topic]
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

func (jmp *JmpSubRouter) AcceptFrom(p peer.ID) AcceptStatus {
	return AcceptAll
}

func (jmp *JmpSubRouter) HandleRPC(rpc *RPC) {
	sender := rpc.GetSender()
	jmpRPCs := rpc.GetJmpRPC()
	if jmpRPCs == nil {
		return
	}

	pullBuf := make(map[peer.ID]*JmpMsgBuf)

	// do receive func
	// msgs 중에서 중복 제외하고 내 msg 버퍼에 추가
	for _, jmpRPC := range jmpRPCs {
		recvJmpMsgs := jmpRPC.JmpMsgs
		recvJMP := jmpRPC.MsgJamMaxPair
		recvSrc := peer.ID(jmpRPC.Source)

		var jmpMsgs []*JmpMsg
		for _, msg := range recvJmpMsgs {
			jmpMsgs = append(jmpMsgs, &JmpMsg{
				Message:   msg.MsgBuf,
				msgSrc:    recvSrc,
				msgNumber: int(*msg.MsgNumber),
				msgSender: peer.ID(sender),
			})
		}

		// 3a)
		jmp.putHistory(jmpMsgs...)

		// update gossipJmp
		if jmp.gossipJMP[recvSrc] == nil {
			jmp.gossipJMP[recvSrc] = &JamMaxPair{jam: 0, max: 0}
		}

		jmp.gossipJMP[recvSrc].jam = int(math.Min(float64(jmp.gossipJMP[recvSrc].jam), float64(int(*recvJMP.Jam))))
		jmp.gossipJMP[recvSrc].max = int(math.Max(float64(jmp.gossipJMP[recvSrc].max), float64(int(*recvJMP.Max))))

		//fmt.Println("gjmp.jam: ", jmp.gossipJMP[recvSrc].jam, "recvJam: ", int(*recvJMP.Jam))

		if *rpc.JmpMode == "PUSH" {
			pullBuf[recvSrc] = jmp.loadPullGossip(recvSrc, jmpMsgs)
		}
	}

	if *rpc.JmpMode == "PUSH" {
		out := jmp.rpcWithMsgBufs(pullBuf, "PULL")

		if out != nil {
			jmp.sendRPC(peer.ID(sender), out)
		}
	}
}

func (jmp *JmpSubRouter) loadPullGossip(src peer.ID, jmpMsgs []*JmpMsg) *JmpMsgBuf {
	tempJmp := jmp.gossipJMP[src]
	history := jmp.history[src]
	if history == nil {
		return nil
	}

	msgBuf := make([]*JmpMsg, 0, jmp.params.MaxMsgBuf)
	for _, msg := range history {
		if tempJmp.jam < msg.msgNumber && msg.msgNumber <= tempJmp.max {
			msgBuf = append(msgBuf, msg)
		}
	}

	// find diff history & receive msgBuf
	tempMap := make(map[*JmpMsg]struct{}, len(jmpMsgs))
	for _, except := range jmpMsgs {
		tempMap[except] = struct{}{}
	}
	var diff []*JmpMsg
	for _, include := range msgBuf {
		if _, found := tempMap[include]; !found {
			diff = append(diff, include)
		}
	}

	tempJmp.jam = jmp.historyJMP[src].jam
	tempJmp.max = jmp.gossipJMP[src].max

	return &JmpMsgBuf{msgJmp: tempJmp, msgBuf: diff, source: src}
}

// Publish 여기서는 처음 msg 가 생성 될 때 호출 되는 함수
func (jmp *JmpSubRouter) Publish(msg *Message) {
	// 본인이 생성한 msg 인 경우에만 numbering 동작
	if msg.GetFrom() == jmp.p.signID {
		// numbering 과 history 관리
		// publish 할 때는 본인이 생성한 msg 가 아닌 경우 history 에 담지 않음
		// 다른 사람이 보낸 msg 를 history 에 담는 것은 handleRPC 에서
		jmpmsg := jmp.numbering(msg)
		jmp.putHistory(jmpmsg)

		// set gossipJMP
		myID := jmp.p.signID
		jmp.gossipJMP[myID].max = jmp.historyJMP[myID].max
	}
}

func (jmp *JmpSubRouter) nextCycle() {
	time.Sleep(jmp.params.InitialDelay)
	select {
	case jmp.p.eval <- jmp.gossip:
	case <-jmp.p.ctx.Done():
		return
	}

	ticker := time.NewTicker(jmp.params.CycleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case jmp.p.eval <- jmp.gossip:
			case <-jmp.p.ctx.Done():
				return
			}
		case <-jmp.p.ctx.Done():
			return
		}
	}
}

func (jmp *JmpSubRouter) gossip() {
	// 1a) When load push gossip 준비
	pushBuf := jmp.loadPushGossip()

	for topic, _ := range jmp.p.topics {
		// subscribe 하고 있는 topic 을 대상으로 msg 를 전달
		// topic 안의 peer 중 보낼 peer 를 fanout 개수만큼 선택
		toSend := jmp.choiceFanout(topic)

		// 해당 topic 에 참여하고 있는 peer 가 보낸 msg 만 out 에 담음
		out := jmp.rpcWithMsgBufs(pushBuf, "PUSH")

		if out != nil {
			//fmt.Println(out)
			for pid := range toSend {
				jmp.sendRPC(pid, out)
			}
		}
	}

	isSentAll := false
	for _, cnt := range jmp.sentPeer {
		if cnt == 0 {
			isSentAll = false
			break
		}
		isSentAll = true
		//if cnt > 0 {
		//	jmp.sentPeer[id] += 1
		//}
		//if cnt >= jmp.params.SentPeerMaintainCycle {
		//	jmp.sentPeer[id] = 0
		//}
	}

	if isSentAll {
		for id, _ := range jmp.sentPeer {
			jmp.sentPeer[id] = 0
		}
	}

	// msgBuf clear
	jmp.msgBuf = make(map[peer.ID]*JmpMsgBuf)
}

func (jmp *JmpSubRouter) loadPushGossip() map[peer.ID]*JmpMsgBuf {
	// 각 msg src 마다 gossip jmp 를 기준으로 history 에 있는 msg 들을 jmp.msgBuf[src] 에 msg 를 담음
	pushBuf := make(map[peer.ID]*JmpMsgBuf)

	for src, gjmp := range jmp.gossipJMP {
		tempJmp := gjmp

		history := jmp.history[src]
		if history == nil {
			continue
		}

		//fmt.Println("loadPushGossip jmp: ", tempJmp.jam, tempJmp.max, src)
		msgBuf := make([]*JmpMsg, 0, jmp.params.MaxMsgBuf)

		for _, msg := range history {
			if tempJmp.jam < msg.msgNumber && msg.msgNumber <= tempJmp.max {
				msgBuf = append(msgBuf, msg)
			}
		}

		tempJmp.jam = jmp.historyJMP[src].jam
		tempJmp.max = jmp.gossipJMP[src].max

		//fmt.Println("load push gossip msgJMP: ", tempJmp.jam, tempJmp.max)
		//if len(msgBuf) > 0 {
		pushBuf[src] = &JmpMsgBuf{msgJmp: tempJmp, msgBuf: msgBuf, source: src}
		//}
	}

	return pushBuf
}

func (jmp *JmpSubRouter) rpcWithMsgBufs(msgBufs map[peer.ID]*JmpMsgBuf, mode string) *RPC {
	if len(msgBufs) == 0 {
		return nil
	}

	// msgBufs 에 담긴 msg 와 jmp 를 rpc 에 담아서 return
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
		jam := int64(buf.msgJmp.jam)
		max := int64(buf.msgJmp.max)

		msgJmp = append(msgJmp, &pb.JmpMsgRPC{
			JmpMsgs:       jmpMsgs,
			MsgJamMaxPair: &pb.JmpMsgRPC_JamMaxPair{Jam: &jam, Max: &max},
			Source:        []byte(src), // src == buf.source
		})

		// 2a) After sending push-(a)
		// 5a) After sending pull-(a)
		if len(jmpMsgs) > 0 {
			jmp.gossipJMP[src].jam = int(*jmpMsgs[len(jmpMsgs)-1].MsgNumber)
		}
	}

	return &RPC{RPC: pb.RPC{JmpRPC: msgJmp, JmpMode: &mode, Sender: []byte(jmp.p.signID)}}
}

func (jmp *JmpSubRouter) Join(topic string) {
	jmp.tracer.Join(topic)
}

func (jmp *JmpSubRouter) Leave(topic string) {
	jmp.tracer.Leave(topic)
}

func (jmp *JmpSubRouter) numbering(msg *Message) *JmpMsg {
	jmpmsg := &JmpMsg{
		Message:   msg.Message,
		msgSrc:    jmp.p.signID,
		msgSender: jmp.p.signID,
	}

	if len(jmp.myMsg) > jmp.params.MaxGenerateMsg {
		jmp.myMsg = jmp.myMsg[1:]
	}

	if len(jmp.myMsg) == 0 {
		jmpmsg.msgNumber = 1
		jmp.myMsg = append(jmp.myMsg, jmpmsg)
	} else {
		last := jmp.myMsg[len(jmp.myMsg)-1].msgNumber
		jmpmsg.msgNumber = last + 1
		jmp.myMsg = append(jmp.myMsg, jmpmsg)
	}

	return jmpmsg
}

func (jmp *JmpSubRouter) putHistory(jmpMsgs ...*JmpMsg) {
loop:
	for _, msg := range jmpMsgs {
		src := msg.msgSrc

		if his, ok := jmp.history[src]; ok {
			// history buf 사이즈의 최대에 도달 했을 때...
			if len(his) > jmp.params.MaxHistoryBuf {
				// 가장 앞의 msg 를 제거
				jmp.history[src] = his[1:]
			}

			// msg 가 중복되어 저장 되지 않도록 함
			for _, exist := range his {
				if msg.msgNumber == exist.msgNumber {
					continue loop
				}
			}
		}

		jmp.history[src] = append(jmp.history[src], msg)

		// sort history -> 이걸 매번 해주는게 나을까...?
		sort.Slice(jmp.history[src], func(i, j int) bool {
			return jmp.history[src][i].msgNumber < jmp.history[src][j].msgNumber
		})

		jmp.updateHistoryJMP(src)
	}
}

func (jmp *JmpSubRouter) updateHistoryJMP(src peer.ID) {
	// update historyJmp
	// 앞의 부분을 못 받은 경우에도 받을 수 있도록 0 으로 setting
	jmp.historyJMP[src] = &JamMaxPair{jam: 0, max: 0}
	history := jmp.history[src]

	if len(history) > 1 {
		for i := 0; i < len(history)-1; i++ {
			if history[i+1].msgNumber == history[i].msgNumber+1 {
				jmp.historyJMP[src].jam = jmp.history[src][i+1].msgNumber
			} else {
				break
			}
		}
	}

	jmp.historyJMP[src].max = history[len(history)-1].msgNumber
}

func (jmp *JmpSubRouter) sendRPC(pid peer.ID, out *RPC) {
	// 받은 사람의 msg channel 을 확인
	mch, ok := jmp.p.peers[pid]
	if !ok {
		return
	}

	// If we're below the max message size, go ahead and send
	if out.Size() < jmp.p.maxMessageSize {
		jmp.doSendRPC(out, pid, mch)
		return
	}

	// If we're too big, fragment into multiple RPCs and send each sequentially
	outRPCs, err := fragmentRPC(out, jmp.p.maxMessageSize)
	if err != nil {
		jmp.doDropRPC(out, pid, fmt.Sprintf("unable to fragment RPC: %s", err))
		return
	}

	for _, rpc := range outRPCs {
		jmp.doSendRPC(rpc, pid, mch)
	}
}

func (jmp *JmpSubRouter) doSendRPC(rpc *RPC, pid peer.ID, mch chan *RPC) {
	select {
	case mch <- rpc:
		jmp.tracer.SendRPC(rpc, pid)
	default:
		jmp.doDropRPC(rpc, pid, "queue full")
	}
}

func (jmp *JmpSubRouter) doDropRPC(rpc *RPC, pid peer.ID, reason string) {
	log.Debugf("dropping message to peer %s: %s", pid.Pretty(), reason)
	jmp.tracer.DropRPC(rpc, pid)
}

func (jmp *JmpSubRouter) choiceFanout(topic string) map[peer.ID]struct{} {
	fanout := jmp.params.MaxFan
	toSend := make(map[peer.ID]struct{})

	tmap, ok := jmp.p.topics[topic]

	if !ok {
		return nil
	} else {
		if len(tmap) > fanout {
			randomPeers := jmp.getRandomPeers(tmap, fanout)

			if len(randomPeers) > 0 {
				tmap = peerListToMap(randomPeers)
			}
		}
	}

	for p := range tmap {
		jmp.sentPeer[p] += 1
		toSend[p] = struct{}{}
	}

	return toSend
}

func (jmp *JmpSubRouter) getRandomPeers(pmap map[peer.ID]struct{}, fanout int) []peer.ID {
	peers := make([]peer.ID, 0, len(pmap))
	for p := range pmap {
		if jmp.sentPeer[p] == 0 {
			peers = append(peers, p)
		}
	}

	//fmt.Println(len(peers))
	shufflePeers(peers)

	if fanout > 0 && len(peers) > fanout {
		peers = peers[:fanout]
	}

	return peers
}
