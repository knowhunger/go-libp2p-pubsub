package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type something struct {
	ctx   context.Context
	intCh chan int
	strCh chan string
}

func NewSomething(ctx context.Context) *something {
	return &something{
		ctx:   ctx,
		intCh: make(chan int, 5),
		strCh: make(chan string),
	}
}

func TestSomething(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	//defer cancel()

	st := NewSomething(ctx)

	go st.doSomethingIterInt()
	go st.doSomethingIterStr()

	for i := 0; i < 5; i++ {
		st.intCh <- i
	}

	time.Sleep(10 * time.Second)
	cancel()
}

func (st *something) doSomethingIterInt() {
	for {
		select {
		case num := <-st.intCh:
			fmt.Println(num * num)
			time.Sleep(time.Second)
			st.strCh <- fmt.Sprintf("%d complete", num)
		case <-st.ctx.Done():
			return
		}
	}
}

func (st *something) doSomethingIterStr() {
	for {
		select {
		case str := <-st.strCh:
			fmt.Println(str)
			time.Sleep(time.Second)
		case <-st.ctx.Done():
			return
		}
	}
}

// difference returns the elements in `a` that aren't in `b`.
func difference(a, b []int) []int {
	mb := make(map[int]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []int
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}
