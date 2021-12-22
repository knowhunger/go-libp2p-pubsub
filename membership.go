package pubsub

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

type membershipRouter struct {
	p  *PubSub
	rt PubSubRouter

	peers map[peer.ID]protocol.ID

	tracer *pubsubTracer
}

func NewMembershipRouter(router PubSubRouter) *membershipRouter {
	return &membershipRouter{
		rt:    router,
		peers: make(map[peer.ID]protocol.ID),
	}
}

func (mr *membershipRouter) Attach(p *PubSub) {
	mr.p = p
	mr.tracer = p.tracer

}

func (mr *membershipRouter) AddPeer(p peer.ID, proto protocol.ID) {
	log.Debugf("PEERUP: Add new peer %s using %s", p, proto)
	mr.tracer.AddPeer(p, proto)
	mr.peers[p] = proto
}

func (mr *membershipRouter) RemovePeer(p peer.ID) {
	log.Debugf("PEERDOWN: Remove disconnected peer %s", p)
	mr.tracer.RemovePeer(p)
	delete(mr.peers, p)
}

func (mr *membershipRouter) Join(topic string) {
	log.Debugf("JOIN %s", topic)
	mr.tracer.Join(topic)
}

func (mr *membershipRouter) Leave(topic string) {
	log.Debugf("LEAVE %s", topic)
	mr.tracer.Leave(topic)
}
