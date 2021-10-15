package pubsub

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/host"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func getJmpsub(ctx context.Context, h host.Host, opts ...Option) *PubSub {
	ps, err := NewJmpSub(ctx, h, opts...)
	if err != nil {
		panic(err)
	}
	return ps
}

func getJmpsubs(ctx context.Context, hs []host.Host, opts ...Option) []*PubSub {
	var psubs []*PubSub
	originOpts := opts
	for i, h := range hs {
		tracer, err := NewJSONTracer(fmt.Sprintf("./trace_out/tracer_%d.json", i))
		if err != nil {
			panic(err)
		}
		opts = append(opts, WithEventTracer(tracer))
		psubs = append(psubs, getJmpsub(ctx, h, opts...))
		opts = originOpts
	}
	return psubs
}

func TestJmpPublish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numHosts := 100
	numMsgs := 50

	hosts := getNetHosts(t, ctx, numHosts)
	psubs := getJmpsubs(ctx, hosts)
	topics := getTopics(psubs, "foobar")

	var msgs []*Subscription
	for _, tp := range topics {
		subch, err := tp.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	// full connect
	connectSome(t, hosts, numHosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	owners := make(map[int][]int)
	for i := 0; i < numMsgs; i++ {
		time.Sleep(time.Millisecond * 250)
		fmt.Println("start", i)
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		owner := rand.Intn(5)
		//owner := 0
		owners[owner] = append(owners[owner], i)

		err := topics[owner].Publish(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second * 5)

	// 각 owner 가 생성한 msg 개수
	// fmt.Println(owners)

	sum := 0
	// 각 owner 가 생성한 msg 개수 확인
	for ow, msgNum := range owners {
		sum += len(msgNum)
		// 각 pubsub peer 가 보유한 msg 개수 확인
		fmt.Println(ow, "sent a total of", len(msgNum), "msgs: \t", msgNum)

		for i, ps := range psubs {
			//assert.Equal(t, len(msgNum), len(ps.rt.(*JmpSubRouter).history[psubs[ow].signID]), fmt.Sprintf("%d peer msg loss", i))
			var recvMsgs []string
			for _, msg := range ps.rt.(*JmpSubRouter).history[psubs[ow].signID] {
				stringMsg := strings.Split(string(msg.Data), " ")
				recvMsgs = append(recvMsgs, stringMsg[0])
			}
			fmt.Println("\tpeer", i, "recv a total of", len(recvMsgs), "msgs: \t", recvMsgs)
			if len(recvMsgs) == 0 {
				fmt.Println("\tgossipJMP", ps.rt.(*JmpSubRouter).gossipJMP[psubs[ow].signID])
				fmt.Println("\thistoryJMP", ps.rt.(*JmpSubRouter).historyJMP[psubs[ow].signID])
			}
		}
		fmt.Println()
		fmt.Println()
	}
	//assert.Equal(t, sum, numMsgs, "total msg count is different")
}
