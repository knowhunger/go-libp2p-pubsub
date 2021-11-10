package pubsub

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

type membershipRouter struct {
	rt PubSubRouter
}

func NewMembershipRouter(router PubSubRouter) *membershipRouter {
	return &membershipRouter{
		rt: router,
	}
}

func (mr *membershipRouter) AddPeer(p peer.ID, proto protocol.ID) {
	log.Debugf("PEERUP: Add new peer %s using %s", p, proto)
	if gs, ok := mr.rt.(*GossipSubRouter); ok {
		gs.tracer.AddPeer(p, proto)
		gs.peers[p] = proto
	}

	if js, ok := mr.rt.(*JmpSubRouter); ok {
		js.tracer.AddPeer(p, proto)
		js.peers[p] = proto
	}
}

func (mr *membershipRouter) RemovePeer(p peer.ID) {
	log.Debugf("PEERDOWN: Remove disconnected peer %s", p)

	if gs, ok := mr.rt.(*GossipSubRouter); ok {
		gs.tracer.RemovePeer(p)
		delete(gs.peers, p)
	}

	if js, ok := mr.rt.(*JmpSubRouter); ok {
		js.tracer.RemovePeer(p)
		delete(js.peers, p)
	}
}

func (mr *membershipRouter) Join(topic string) {
	log.Debugf("JOIN %s", topic)

	if gs, ok := mr.rt.(*GossipSubRouter); ok {
		gs.tracer.Join(topic)
	}
	if js, ok := mr.rt.(*JmpSubRouter); ok {
		js.tracer.Join(topic)
	}
}

func (mr *membershipRouter) Leave(topic string) {
	log.Debugf("LEAVE %s", topic)

	if gs, ok := mr.rt.(*GossipSubRouter); ok {
		gs.tracer.Leave(topic)
	}
	if js, ok := mr.rt.(*JmpSubRouter); ok {
		js.tracer.Leave(topic)
	}
}
