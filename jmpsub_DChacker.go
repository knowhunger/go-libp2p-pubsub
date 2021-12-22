package pubsub

//
//import "github.com/libp2p/go-libp2p-core/peer"
//
//type JmpDFliter struct {
//	//unrecv map[JmpMsgID]struct{}
//	jmpMap map[peer.ID]JamMaxPair
//}
//
//func (jd *JmpDFliter) isDuplicated(msg *JmpMessage) bool {
//	mid := msg.mid
//	jmp, ok := jd.jmpMap[mid.pid]
//	if !ok {
//		jmp = JamMaxPair{jam: 0, max: 0}
//		jd.jmpMap[mid.pid] = jmp
//		jd.unrecv[mid] = struct{}{}
//	}
//
//	if _, ok := jd.unrecv[mid]; !ok {
//		return false
//	}
//	if mid.seq <= jmp.jam {
//		return false
//	}
//	if mid.seq == jmp.max {
//		return false
//	}
//
//	if mid.seq == jmp.jam+1 {
//		jmp.jam = mid.seq
//	}
//	if mid.seq > jmp.max {
//		//unrecv, ok := jd.unrecv[msg.mid]
//		//if !ok {
//		//	unrecv =
//		//}
//		//for (long l = recvjm.max + 1; l < msgseq; l++) {
//		//	BCMsgID mid = new BCMsgID(msgsid, l);
//		//	ts.add(mid);
//		//}
//		//recvjm.max = msgseq;
//	}
//
//	return true
//}
