package pubsub

import (
	"context"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"time"
)

var (
	JamMaxSubMinFan              = 3
	JamMaxSubMaxFan              = 6
	JamMaxSubMaxSendBuf          = 30
	JamMaxSubMaxHistoryBuf       = 120
	JamMaxSubGossipInitialDelay  = 200 * time.Millisecond
	JamMaxSubGossipCycleInterval = 1 * time.Second
)

type JamMaxSubParams struct {
	MinFan              int
	MaxFan              int
	MaxSendBuf          int
	MaxHistoryBuf       int
	GossipInitialDelay  time.Duration
	GossipCycleInterval time.Duration
}

func DefaultJamMaxSubParams() JamMaxSubParams {
	return JamMaxSubParams{
		MinFan:              JamMaxSubMinFan,
		MaxFan:              JamMaxSubMaxFan,
		MaxSendBuf:          JamMaxSubMaxSendBuf,
		MaxHistoryBuf:       JamMaxSubMaxHistoryBuf,
		GossipInitialDelay:  JamMaxSubGossipInitialDelay,
		GossipCycleInterval: JamMaxSubGossipCycleInterval,
	}
}

type JamMaxMessage struct {
	*pb.Message // data, mid, topic, timestamp
	source      peer.ID
	sender      peer.ID
	msgNumber   int
}

type JamMaxSubRouter struct {
	// for PubSub peer
	p      *PubSub
	peers  map[peer.ID]protocol.ID
	protos []protocol.ID
	myID   peer.ID

	// peer selection
	fanout map[string]map[peer.ID]struct{} // select peer to send

	// jam-max pair
	hJMP map[string]*JamMaxPair
	gJMP map[string]*JamMaxPair

	// history buf
	history map[string][]*JamMaxMessage
	mcache  *MessageCache

	// tracer
	tracer *pubsubTracer

	// parameter
	params JamMaxSubParams
}

func NewJamMaxSubRouter(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	params := DefaultJamMaxSubParams()
	rt := &JamMaxSubRouter{
		peers:   make(map[peer.ID]protocol.ID),
		protos:  GossipSubDefaultProtocols,
		fanout:  make(map[string]map[peer.ID]struct{}),
		hJMP:    make(map[string]*JamMaxPair),
		gJMP:    make(map[string]*JamMaxPair),
		history: make(map[string][]*JamMaxMessage),

		mcache: NewMessageCache(params.MinFan, params.MaxFan),
		params: params,
	}

	return NewPubSub(ctx, h, rt, opts...)
}

func (jms *JamMaxSubRouter) Protocols() []protocol.ID {
	return jms.protos
}

func (jms *JamMaxSubRouter) Attach(p *PubSub) {
	jms.p = p
	jms.tracer = p.tracer
	jms.myID = p.host.ID()

	// temp msg cache entry... 이걸 쓸지, history 를 쓸지
	// start using the same msg ID function as PubSub for caching messages.
	jms.mcache.SetMsgIdFn(p.msgID)

	// something to start

	// go heartbeat

	// connect direct peer

}

func (jms *JamMaxSubRouter) makeMyMap() {
	//jms.history[]
}

func (jms *JamMaxSubRouter) AddPeer(pid peer.ID, proto protocol.ID) {
	log.Debugf("PEERUP: Add new peer %s using %s", pid, proto)
	jms.tracer.AddPeer(pid, proto)
	jms.peers[pid] = proto

	// connect peer
}

func (jms *JamMaxSubRouter) RemovePeer(id peer.ID) {
	panic("implement me")
}

func (jms *JamMaxSubRouter) EnoughPeers(topic string, suggested int) bool {
	panic("implement me")
}

func (jms *JamMaxSubRouter) AcceptFrom(pid peer.ID) AcceptStatus {
	return AcceptAll
}

func (jms *JamMaxSubRouter) HandleRPC(rpc *RPC) {
	panic("implement me")
}

func (jms *JamMaxSubRouter) Publish(msg *Message) {
	if jms.myID != msg.GetFrom() && jms.myID != msg.ReceivedFrom {
		hop := msg.GetHop()
		hop++
		msg.Hop = &hop
	}

	// put in history || msg cache
	jms.mcache.Put(msg.Message)

	//from := msg.ReceivedFrom
	//topic := msg.GetTopic()

	// any peers in the topic?
	//tmap, ok := jms.p.topics[topic]
	//if !ok {
	//	return
	//}

	// select peer to send
}

func (jms *JamMaxSubRouter) numbering(msg *Message) *JmpMessage {
	return nil
}

func (jms *JamMaxSubRouter) putHistory(msg *pb.Message) {
	// duplicated check

	// put

	// sort by msg number
}

func (jms *JamMaxSubRouter) Join(topic string) {
	fmap, ok := jms.fanout[topic]
	if ok {
		return
	} else {
		log.Debugf("JOIN %s", topic)
		jms.tracer.Join(topic)

		peers := jms.getPeersWithFanout(topic, jms.params.MaxFan)
		fmap = peerListToMap(peers)
		jms.fanout[topic] = fmap
	}

	// send graft ctrl with fmap
}

func (jms *JamMaxSubRouter) Leave(topic string) {
	_, ok := jms.fanout[topic]
	if !ok {
		return
	}

	log.Debugf("LEAVE %s", topic)
	jms.tracer.Leave(topic)

	delete(jms.fanout, topic)

	// send prune ctrl with fmap
}

func (jms *JamMaxSubRouter) getPeersWithFanout(topic string, fanout int) []peer.ID {
	tmap, ok := jms.p.topics[topic]
	if !ok {
		return nil
	}

	peers := make([]peer.ID, 0, len(tmap))
	for p := range tmap {
		// something to check peer condition
		peers = append(peers, p)
	}

	shufflePeers(peers)

	if fanout > 0 && len(peers) > fanout {
		peers = peers[:fanout]
	}

	return peers
}
