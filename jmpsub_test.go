package pubsub

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func BenchmarkShuffle(b *testing.B) {
	a := []int{}
	temp := []int{}
	for i := 0; i < 30; i++ {
		a = append(a, i)
	}

	for i := 0; i < b.N; i++ {
		temp = sp(a, 6)
	}

	fmt.Println(temp)
}

func BenchmarkShuffleRemove(b *testing.B) {
	a := []int{}
	temp := []int{}
	for i := 0; i < 30; i++ {
		a = append(a, i)
	}

	for i := 0; i < b.N; i++ {
		temp = spr(a, 6)
	}

	fmt.Println(temp)
}

func sp(s []int, l int) []int {
	temp := []int{}
	for _, v := range s {
		temp = append(temp, v)
	}
	for i := range temp {
		j := rand.Intn(i + 1)
		temp[i], temp[j] = temp[j], temp[i]
	}
	return temp[:l]
}

func spr(s []int, l int) []int {
	temp := []int{}
	for _, v := range s {
		temp = append(temp, v)
	}
	for len(temp) > l {
		r := rand.Intn(len(temp))
		temp[r] = temp[len(temp)-1]
		temp = temp[:len(temp)-1]
	}
	return temp
}

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
		tracer, err := NewJSONTracer(fmt.Sprintf("./trace_out_jmp/tracer_%d.json", i))
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

	numHosts := 50
	numMsgs := 100

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
	connectAll(t, hosts)
	//connectSome(t, hosts, 20)
	//denseConnect(t, hosts)
	//sparseConnect(t, hosts)

	//for i, ps := range psubs {
	//	fmt.Println(i, "'s peer", len(ps.topics["foobar"]))
	//}

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	owners := make(map[int][]int)
	for i := 0; i < numMsgs; i++ {
		time.Sleep(time.Millisecond * 50)

		if i%10 == 0 {
			fmt.Println("publishing", i)
			// time.Sleep(time.Millisecond * 1000)
		}
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		//owner := i % len(psubs)
		owner := rand.Intn(len(psubs))

		//owner := 0
		owners[owner] = append(owners[owner], i)

		err := topics[owner].Publish(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}

		//if i%10 == 0 {
		//	for i, ps := range psubs {
		//		fmt.Println(i, "'s peer", len(ps.topics["foobar"]))
		//	}
		//}
	}

	time.Sleep(time.Second * 2)

	// 각 owner 가 생성한 msg 개수
	// fmt.Println(owners)

	sum := 0
	// 각 owner 가 생성한 msg 개수 확인
	for ow, msgNum := range owners {
		sum += len(msgNum)
		// 각 pubsub peer 가 보유한 msg 개수 확인
		//fmt.Println(ow, "sent a total of", len(msgNum), "msgs: \t", msgNum)

		for i, ps := range psubs {
			var recvMsgs []string
			for _, msg := range ps.rt.(*JmpSubRouter).history[psubs[ow].signID] {
				stringMsg := strings.Split(string(msg.Data), " ")
				recvMsgs = append(recvMsgs, stringMsg[0])
			}
			//fmt.Println("\tpeer", i, "hit a total of", len(recvMsgs), "msgs: \t", recvMsgs)
			isRecvAll := assert.Equal(t, len(msgNum), len(ps.rt.(*JmpSubRouter).history[psubs[ow].signID]), fmt.Sprintf("%d peer msg loss", i))
			if !isRecvAll {
				fmt.Println("\tgossipJMP", ps.rt.(*JmpSubRouter).gossipJMP[psubs[ow].signID])
				fmt.Println("\thistoryJMP", ps.rt.(*JmpSubRouter).historyJMP[psubs[ow].signID])
			}
		}
		//fmt.Println()
		//fmt.Println()
	}

	//assert.Equal(t, sum, numMsgs, "total msg count is different")

	printStat(psubs, "jmp")
}

func TestJmpPublishJoinLater(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numHosts := 50
	numMsgs := 100

	hosts := getNetHosts(t, ctx, 30)
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
	connectAll(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	owners := make(map[int][]int)
	for i := 0; i < numMsgs/2; i++ {
		time.Sleep(time.Millisecond * 200)
		if i%10 == 0 {
			fmt.Println("publishing", i)
		}
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		//owner := i % len(psubs)
		owner := rand.Intn(len(topics))
		//owner := 0
		owners[owner] = append(owners[owner], i)

		err := topics[owner].Publish(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second * 2)

	hostsLater := getNetHosts(t, ctx, numHosts-30)
	psubsLater := getJmpsubs(ctx, hostsLater)
	topicsLater := getTopics(psubsLater, "foobar")
	hosts = append(hosts, hostsLater...)
	psubs = append(psubs, psubsLater...)
	topics = append(topics, topicsLater...)

	connectAll(t, hosts)

	for _, tpl := range topicsLater {
		subch, err := tpl.Subscribe()
		if err != nil {
			t.Fatal(err)
		}

		msgs = append(msgs, subch)
	}

	for i := 50; i < numMsgs; i++ {
		time.Sleep(time.Millisecond * 200)
		if i%10 == 0 {
			fmt.Println("publishing with later peer", i)
		}
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		//owner := i % len(psubs)
		owner := rand.Intn(len(topics))
		//owner := 0
		owners[owner] = append(owners[owner], i)

		err := topics[owner].Publish(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second * 2)

	sum := 0
	// 각 owner 가 생성한 msg 개수 확인
	for ow, msgNum := range owners {
		sum += len(msgNum)
		// 각 pubsub peer 가 보유한 msg 개수 확인
		fmt.Println(ow, "sent a total of", len(msgNum), "msgs: \t", msgNum)

		for i, ps := range psubs {
			assert.Equal(t, len(msgNum), len(ps.rt.(*JmpSubRouter).history[psubs[ow].signID]), fmt.Sprintf("%d peer msg loss", i))
			var recvMsgs []string
			for _, msg := range ps.rt.(*JmpSubRouter).history[psubs[ow].signID] {
				stringMsg := strings.Split(string(msg.Data), " ")
				recvMsgs = append(recvMsgs, stringMsg[0])
			}
			fmt.Println("\tpeer", i, "hit a total of", len(recvMsgs), "msgs: \t", recvMsgs)
			if len(recvMsgs) == 0 {
				fmt.Println("\tgossipJMP", ps.rt.(*JmpSubRouter).gossipJMP[psubs[ow].signID])
				fmt.Println("\thistoryJMP", ps.rt.(*JmpSubRouter).historyJMP[psubs[ow].signID])
			}
		}
		fmt.Println()
		fmt.Println()
	}

	//assert.Equal(t, sum, numMsgs, "total msg count is different")

	printStat(psubs, "jmp")
}

func TestJmpFanoutBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	JmpMinFan = 3
	JmpMaxFan = 12

	defer func() {
		JmpMinFan = 3
		JmpMaxFan = 6
	}()

	numHosts := 50
	numMsgs := 100

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

	denseConnect(t, hosts)

	// wait for heartbeats to build mesh
	time.Sleep(time.Second * 2)

	owners := make(map[int][]int)
	for i := 0; i < numMsgs; i++ {
		time.Sleep(time.Millisecond * 200)
		if i%10 == 0 {
			fmt.Println("publishing", i)
		}
		msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))

		//owner := i % len(psubs)
		owner := rand.Intn(len(psubs))
		//owner := 0
		owners[owner] = append(owners[owner], i)

		err := topics[owner].Publish(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second * 2)

	sum := 0
	// 각 owner 가 생성한 msg 개수 확인
	for ow, msgNum := range owners {
		sum += len(msgNum)
		// 각 pubsub peer 가 보유한 msg 개수 확인
		fmt.Println(ow, "sent a total of", len(msgNum), "msgs: \t", msgNum)

		for i, ps := range psubs {
			assert.Equal(t, len(msgNum), len(ps.rt.(*JmpSubRouter).history[psubs[ow].signID]), fmt.Sprintf("%d peer msg loss", i))
			var recvMsgs []string
			for _, msg := range ps.rt.(*JmpSubRouter).history[psubs[ow].signID] {
				stringMsg := strings.Split(string(msg.Data), " ")
				recvMsgs = append(recvMsgs, stringMsg[0])
			}
			fmt.Println("\tpeer", i, "hit a total of", len(recvMsgs), "msgs: \t", recvMsgs)
			if len(recvMsgs) == 0 {
				fmt.Println("\tgossipJMP", ps.rt.(*JmpSubRouter).gossipJMP[psubs[ow].signID])
				fmt.Println("\thistoryJMP", ps.rt.(*JmpSubRouter).historyJMP[psubs[ow].signID])
			}
		}
		fmt.Println()
		fmt.Println()
	}

	//assert.Equal(t, sum, numMsgs, "total msg count is different")

	printStat(psubs, "jmp")
}
