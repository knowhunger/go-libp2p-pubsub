package pubsub

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-gota/gota/dataframe"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/mr-tron/base58/base58"
	chart "github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"image/color"
	"io/fs"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type experimentsStats struct {
	gmsg, smsg, rmsg int
	hmsg             map[string]*hmsgInfo
	dmsg, hitmsg     int
	fanout           []int
	visual           *visualization
	startTime        int64
	multipleSend     int
}

type hmsgInfo struct {
	timestamp  int64
	delay, hop int
}

type visualization struct {
	hit  map[string][]int64
	send map[string][]int64
	recv map[string][]int64
}

func NewVisualization() *visualization {
	return &visualization{
		hit:  make(map[string][]int64),
		send: make(map[string][]int64),
		recv: make(map[string][]int64),
	}
}

func (es *experimentsStats) evaluateStat(evt *pb.TraceEvent) {
	rand.Seed(time.Now().UnixNano())
	min := 50
	max := 200

	switch evt.GetType() {
	case pb.TraceEvent_JOIN:
		es.startTime = evt.GetTimestamp() / 1000000
	case pb.TraceEvent_PUBLISH_MESSAGE:
		// check gmsg
		es.gmsg++
	case pb.TraceEvent_RECV_RPC:
		if len(evt.RecvRPC.Meta.Messages) > 0 {
			// check only msg rpc
			for _, msg := range evt.RecvRPC.Meta.Messages {
				// check rmsg
				es.rmsg++
				// check hmsg
				if _, ok := es.hmsg[string(msg.MessageID)]; !ok {
					networkDelay := 0
					hop := int(msg.GetHop())

					for i := 0; i < hop; i++ {
						networkDelay += rand.Intn(max-min+1) + min
					}

					es.hmsg[string(msg.MessageID)] = &hmsgInfo{
						timestamp: evt.GetTimestamp(),
						delay:     int((evt.GetTimestamp()-msg.GetCreateTime())/1000000) + networkDelay,
						hop:       hop,
					}

					senderID := base58.Encode(evt.PeerID)
					senderID = senderID[len(senderID)-6:]
					//recvTime := (evt.GetTimestamp() - msg.GetCreateTime()) / 1000000
					//es.visual.hit[senderID] = append(es.visual.hit[senderID], recvTime)
					es.visual.hit[senderID] = append(es.visual.hit[senderID], int64(hop))
				}
			}
		}
		if len(evt.RecvRPC.Meta.Jmp) > 0 {
			for _, jmp := range evt.RecvRPC.Meta.Jmp {
				es.rmsg += len(jmp.JmpMsgs)
				//if len(jmp.JmpMsgs) > 1 {
				//	es.multipleSend++
				//}
				if len(jmp.JmpMsgs) > 0 {
					senderID := base58.Encode(evt.PeerID)
					senderID = senderID[len(senderID)-6:]
					recvTime := evt.GetTimestamp() / 1000000
					es.visual.recv[senderID] = append(es.visual.recv[senderID], recvTime)
				}
			}
		}
	case pb.TraceEvent_SEND_RPC:
		if len(evt.SendRPC.Meta.Messages) > 0 {
			// check only msg rpc
			for _, msg := range evt.SendRPC.Meta.Messages {
				// check smsg
				es.smsg++
				es.fanout = append(es.fanout, int(msg.GetFanout()))

				senderID := base58.Encode(evt.SendRPC.SendTo)
				senderID = senderID[len(senderID)-6:]
				recvTime := evt.GetTimestamp()/1000000 - es.startTime
				es.visual.send[senderID] = append(es.visual.send[senderID], recvTime)
			}
		}
		if len(evt.SendRPC.Meta.Jmp) > 0 {
			for _, jmp := range evt.SendRPC.Meta.Jmp {
				es.smsg += len(jmp.JmpMsgs)
				es.fanout = append(es.fanout, int(jmp.GetFanout()))

				senderID := base58.Encode(evt.SendRPC.SendTo)
				senderID = senderID[len(senderID)-6:]
				recvTime := evt.GetTimestamp()/1000000 - es.startTime
				es.visual.send[senderID] = append(es.visual.send[senderID], recvTime)
			}
		}
	case pb.TraceEvent_DUPLICATE_MESSAGE:
		es.dmsg++
	case pb.TraceEvent_HIT_MESSAGE:
		es.hitmsg++
		msg := evt.HitMessage
		if _, ok := es.hmsg[string(msg.MessageID)]; !ok {
			hop := int(evt.HitMessage.GetHop())
			networkDelay := 0

			for i := 0; i < hop; i++ {
				networkDelay += rand.Intn(max-min+1) + min
			}

			es.hmsg[string(msg.MessageID)] = &hmsgInfo{
				timestamp: evt.GetTimestamp(),
				delay:     int((evt.GetTimestamp()-evt.HitMessage.GetCreateTime())/1000000) + networkDelay,
				// + networkDelay,
				hop: hop,
			}

			senderID := base58.Encode(evt.PeerID)
			senderID = senderID[len(senderID)-6:]
			recvTime := (evt.GetTimestamp() - evt.HitMessage.GetCreateTime()) / 1000000
			es.visual.hit[senderID] = append(es.visual.hit[senderID], recvTime) //+int64(networkDelay)
			//es.visual.hit[senderID] = append(es.visual.hit[senderID], int64(hop)) //+int64(networkDelay)
		}

	}
}

func opsPublish(ctx context.Context, tp *Topic, msgs []*Subscription, fileInfo fs.FileInfo) {
	rand.Seed(time.Now().UnixNano())
	targetDir := "./rgaops"
	ops := readOpFile(targetDir, fileInfo)
	//min := 50
	//max := 200

	for i, op := range ops {
		time.Sleep(100 * time.Millisecond)
		//networkDelay := rand.Intn(max - min + 1) + min
		//time.Sleep(time.Millisecond * time.Duration(networkDelay))

		//msg := []byte(fmt.Sprintf("%d it's not a floooooood %d", i, i))
		msg := []byte(op[2])

		//owner := rand.Intn(len(psubs))
		//owner := 0 	// publish only peer 0
		//owner, _ := strconv.Atoi(strings.Split(op[1], ",")[1])

		err := tp.Publish(ctx, msg)
		if err != nil {
			panic(err)
		}

		//for _, sub := range msgs {
		//	_, err := sub.Next(ctx)
		//	if err != nil {
		//		panic(sub.err)
		//	}
		//	//if !bytes.Equal(msg, got.Data) {
		//	//	fmt.Println(string(msg))
		//	//	fmt.Println(string(got.Data))
		//	//	panic("got wrong message!")
		//	//}
		//}
		if i%100 == 0 {
			fmt.Println("send msg", i, "번 째", len(ops))
		}
		if i > 1000 {
			break
		}
	}
	//ElapsedTime(operationTime, owner, "msg publish")
	time.Sleep(5 * time.Second)
}

type IncrementalStats struct {
	id       string
	seq      int
	min, max int
	avg      float64
	slice    []int
	units    string
}

func (is IncrementalStats) String() string {
	is.CalculateStats()
	return fmt.Sprintf("Min: %6d %s,\t\t Max: %6d %s,\t\t Avg: %6.4f %s",
		is.min, is.units,
		is.max, is.units,
		is.avg, is.units)
}

func (is *IncrementalStats) Add(nums ...int) {
	if nums == nil {
		return
	}
	is.slice = append(is.slice, nums...)
}

func (is *IncrementalStats) AddWithIncrementalStats(iss IncrementalStats) {
	is.slice = append(is.slice, iss.slice...)
	is.units = iss.units
}

func (is *IncrementalStats) CalculateStats() {
	if is.slice == nil {
		return
	}

	sort.Slice(is.slice, func(i, j int) bool {
		return is.slice[i] < is.slice[j]
	})

	for _, v := range is.slice {
		is.avg += float64(v)
	}
	is.avg = is.avg / float64(len(is.slice))
	is.min = is.slice[0]
	is.max = is.slice[len(is.slice)-1]
}

func printStat(psubs []*PubSub, rtName string) {
	fmt.Println("printStat starts")
	type statGroup struct {
		gmsg, smsg, rmsg, hmsg int
		dmsg, hitmsg           int
		delay, hop, fanout     IncrementalStats
		recv                   map[string][]int64
		check                  int
	}

	var heatmap []IncrementalStats

	var wg sync.WaitGroup
	totalStat := &statGroup{recv: make(map[string][]int64)}
	totalStatChan := make(chan statGroup, len(psubs))

	//var splitPsubs [][]*PubSub
	//spliter := len(psubs) / 50
	//fmt.Println(spliter)
	//
	//for i := 0; i < spliter; i++ {
	//	splitPsubs = append(splitPsubs, psubs[i*50:(i+1)*50])
	//}

	//for _, split := range splitPsubs {
	pmap := make(map[string]int)
	for i := 0; i < len(psubs); i++ {
		senderID := base58.Encode([]byte(psubs[i].host.ID()))
		senderID = senderID[len(senderID)-6:]
		pmap[senderID] = i

		wg.Add(1)
		go func(i int, totalStatChan chan statGroup) {
			var delay, hop, fanout IncrementalStats
			var evt pb.TraceEvent
			stats := &experimentsStats{
				hmsg:   make(map[string]*hmsgInfo),
				visual: NewVisualization(),
			}
			delay.units = "ms"
			hop.units = "hops"
			fanout.units = "nodes"

			f, err := os.Open(fmt.Sprintf("./trace_out_%s/tracer_%d.json", rtName, i))
			if err != nil {
				panic(err)
			}
			defer f.Close()

			dec := json.NewDecoder(f)
			for {
				evt.Reset()
				err := dec.Decode(&evt)
				if err != nil {
					break
				}
				stats.evaluateStat(&evt)
			}

			for _, hm := range stats.hmsg {
				delay.Add(hm.delay)
				hop.Add(hm.hop)
			}
			fanout.Add(stats.fanout...)

			delay.id = senderID
			delay.seq = i

			//fmt.Println("peer", i, "'s Stat")
			//fmt.Println("multiple send: ", stats.multipleSend)
			//fmt.Println("gmsg cnt:", stats.gmsg)
			//fmt.Println("smsg cnt:", stats.smsg)
			//fmt.Println("rmsg cnt:", stats.rmsg)
			//fmt.Println("hmsg cnt:", len(stats.hmsg))
			//fmt.Println("dmsg cnt:", stats.dmsg)
			//fmt.Println("hitmsg cnt:", stats.hitmsg)
			//fmt.Println("delay:\n\t", delay)
			//fmt.Println("hop:\n\t", hop)
			//fmt.Println("fanout:\n\t", fanout)
			//fmt.Println()
			//fmt.Println(len(stats.visual.hit))
			//drawTimePlot(stats.visual.hit, i)
			//drawScatterPlot(stats.visual.hit, "hit", strconv.Itoa(i))
			//drawScatterPlot(stats.visual.send, "send", strconv.Itoa(i))

			totalStatChan <- statGroup{
				gmsg: stats.gmsg, smsg: stats.smsg, rmsg: stats.rmsg, hmsg: len(stats.hmsg),
				delay: delay, hop: hop, fanout: fanout,
				dmsg: stats.dmsg, hitmsg: stats.hitmsg,
				recv: stats.visual.hit, check: len(stats.visual.hit),
			}
			wg.Done()
		}(i, totalStatChan)
	}
	wg.Wait()
	//}
	close(totalStatChan)

	for c := range totalStatChan {
		totalStat.gmsg += c.gmsg
		totalStat.smsg += c.smsg
		totalStat.rmsg += c.rmsg
		totalStat.hmsg += c.hmsg
		totalStat.dmsg += c.dmsg
		totalStat.hitmsg += c.hitmsg
		totalStat.delay.AddWithIncrementalStats(c.delay)
		totalStat.hop.AddWithIncrementalStats(c.hop)
		totalStat.fanout.AddWithIncrementalStats(c.fanout)
		totalStat.check += c.check
		for k, v := range c.recv {
			totalStat.recv[k] = append(totalStat.recv[k], v...)
		}

		heatmap = append(heatmap, c.delay)
	}
	checkerRecvPeers := 0
	checkerMsgLen := 0
	for _, v := range totalStat.recv {
		checkerRecvPeers++
		checkerMsgLen += len(v)
	}

	sort.Slice(heatmap, func(i, j int) bool {
		return heatmap[i].seq < heatmap[j].seq
	})

	//fmt.Println(heatmap)
	//makeDataFrame(heatmap)

	fmt.Println("total hit data size with checkerMsgLen: ", checkerMsgLen)
	fmt.Println("total recved peers: ", len(totalStat.recv), checkerRecvPeers)
	drawScatterPlot(pmap, totalStat.recv, "hit", "total")

	fmt.Println("total gmsg: ", totalStat.gmsg)
	fmt.Println("total smsg: ", totalStat.smsg)
	fmt.Println("total rmsg: ", totalStat.rmsg)
	fmt.Println("total hmsg: ", totalStat.hmsg)
	fmt.Println("excepted hmsg: ", totalStat.gmsg*(len(psubs)-1))

	fmt.Println("total dmsg: ", totalStat.dmsg)
	fmt.Println("total hitmsg: ", totalStat.hitmsg)

	//var coverage float64
	coverage := float64(totalStat.hmsg) / (float64(totalStat.gmsg) * float64(len(psubs)-1))
	fmt.Println("final Coverage: ", coverage)

	hitCoverage := float64(totalStat.hitmsg) / (float64(totalStat.gmsg) * float64(len(psubs)-1))
	fmt.Println("final hit Coverage: ", hitCoverage)

	//var redundancy float64
	redundancy := float64(totalStat.smsg) / (float64(totalStat.gmsg) * float64(len(psubs)-1))
	fmt.Printf("final Redundancy: %6.4f\n", redundancy)

	//calculateCoverage(totalGmsg, hmsgPerUnitTime, endTime, len(psubs))

	fmt.Println("total Delay:\n\t", totalStat.delay)
	//fmt.Println("delays: \n\t", totalStat.delay.slice)
	fmt.Println("total Hop:\n\t", totalStat.hop)
	//fmt.Println("hops: \n\t", totalStat.hop.slice)
	fmt.Println("total Fanout:\n\t", totalStat.fanout)

	delayMap := dupCounter(totalStat.delay.slice)
	//fmt.Println(delayMap)
	drawCoveragePlot(delayMap, totalStat.gmsg*(len(psubs)-1), "coverage_per_delay")

	hopMap := dupCounter(totalStat.hop.slice)
	//fmt.Println(hopMap)
	drawCoveragePlot(hopMap, totalStat.gmsg*(len(psubs)-1), "coverage_per_hop")

	drawStackBar()
}

func dupCounter(list []int) map[int]int {
	counter := make(map[int]int)

	for _, item := range list {
		// check if the item/element exist in the duplicate_frequency map
		_, exist := counter[item]

		if exist {
			counter[item] += 1 // increase counter by 1 if already in the map
		} else {
			counter[item] = 1 // else start counting from 1
		}
	}
	return counter
}

func drawScatterPlot(pmap map[string]int, data map[string][]int64, dataMode string, peerNum string) {
	xValues := []float64{}
	yValues := []float64{}
	//stringToIndex := make(map[string]float64)
	for k, v := range data {
		//stringToIndex[k] = float64(pmap[k])
		for _, y := range v {
			xValues = append(xValues, float64(pmap[k]))
			yValues = append(yValues, float64(y))
		}
	}

	viridisByY := func(xr, yr chart.Range, index int, x, y float64) drawing.Color {
		return chart.Viridis(y, yr.GetMin(), yr.GetMax())
	}

	graph := chart.Chart{
		Series: []chart.Series{
			chart.ContinuousSeries{
				Style: chart.Style{
					StrokeWidth:      chart.Disabled,
					DotWidth:         4,
					DotColorProvider: viridisByY,
				},
				XValues: xValues,
				YValues: yValues,
				//XValues: chart.Seq{Sequence: chart.NewLinearSequence().WithStart(0).WithEnd(127)}.Values(),
				//YValues: chart.Seq{Sequence: chart.NewRandomSequence().WithLen(128).WithMin(0).WithMax(1024)}.Values(),
			},
		},
	}

	err := os.MkdirAll(fmt.Sprintf("./visualization_%s", dataMode), os.ModePerm)
	if err != nil {
		panic(err)
	}
	f, err := os.Create(fmt.Sprintf("./visualization_%s/output_%s.png", dataMode, peerNum))
	if err != nil {
		panic(err)
	}
	defer f.Close()

	graph.Render(chart.PNG, f)
}

func drawStackBar() {
	sbc := chart.StackedBarChart{
		Title: "Redundancy Chart Each Algorithm",
		Background: chart.Style{
			Padding: chart.Box{
				Top: 40,
			},
		},
		Height: 512,
		Bars: []chart.StackedBar{
			{
				Name: "jammaxsub",
				Values: []chart.Value{
					{Value: 5.4622, Label: "Duplicated Msg"},
					{Value: 1, Label: "Hit Msg"},
				},
			},
			{
				Name: "gossipsub",
				Values: []chart.Value{
					{Value: 4.9749, Label: "Duplicated Msg"},
					{Value: 1, Label: "Hit Msg"},
				},
			},
		},
	}

	err := os.MkdirAll(fmt.Sprintf("./visualization"), os.ModePerm)
	if err != nil {
		panic(err)
	}

	f, _ := os.Create("./visualization/redundancy_chart.png")
	defer f.Close()

	err = sbc.Render(chart.PNG, f)
	if err != nil {
		return
	}
}

func makeDataFrame(data []IncrementalStats) {
	df := dataframe.LoadStructs(data)
	fmt.Println(df)
}

func drawTimePlot(data map[string][]int64, peerNum int) {
	drawable := []chart.Value{}

	for k, v := range data {
		for _, y := range v {
			drawable = append(drawable, chart.Value{Value: float64(y), Label: k})
		}
	}

	graph := chart.BarChart{
		Title: "Test Bar Chart",
		Background: chart.Style{
			Padding: chart.Box{
				Top: 40,
			},
		},
		Height:   512,
		BarWidth: 60,
		Bars:     drawable,
	}

	f, _ := os.Create(fmt.Sprintf("./visualization/output_%d.png", peerNum))
	defer f.Close()

	graph.Render(chart.PNG, f)
}

func drawCoveragePlot(data map[int]int, denominator int, name string) {
	type kv struct {
		Key   int
		Value int
	}
	var sortMsg []kv
	var pts plotter.XYs

	for k, v := range data {
		sortMsg = append(sortMsg, kv{Key: k, Value: v})
	}
	sort.Slice(sortMsg, func(i, j int) bool {
		return sortMsg[i].Key < sortMsg[j].Key
	})

	total := 0
	pts = append(pts, plotter.XY{X: 0, Y: float64(total) / float64(denominator)})
	for _, m := range sortMsg {
		//fmt.Println(hm.Key, hm.Value)
		total += m.Value

		//coverage := float64(total) / (float64(denominator) * float64(lenPsubs-1))
		pts = append(pts, plotter.XY{X: float64(m.Key), Y: float64(total) / float64(denominator)})
		//pts[i].Y = coverage[i]
	}

	p := plot.New()

	splitName := strings.Split(name, "_")
	p.Title.Text = name
	p.X.Label.Text = splitName[len(splitName)-1]
	p.Y.Label.Text = "Coverage"

	p.Y.Min = 0.0
	p.Y.Max = 1.2

	plter, err := plotter.NewLine(pts)
	if err != nil {
		panic(err)
	}

	plter.Color = color.RGBA{R: 255, B: 255, A: 255}

	p.Add(plter)

	//err := plotutil.AddLinePoints(p, "Coverage", pts)
	//if err != nil {
	//	panic(err)
	//}

	// Save the plot to a PNG file.
	if err := p.Save(4*vg.Inch, 4*vg.Inch, "./figure/"+name+".png"); err != nil {
		panic(err)
	}
}

func ElapsedTime(start time.Time, num int, name string) {
	elapsed := time.Since(start)
	fmt.Printf("%d 's %s took  %s\n", num, name, elapsed)
}

func readOpFile(targetDir string, fileInfo fs.FileInfo) [][]string {
	var dataSlice [][]string

	file, err := os.Open(fmt.Sprintf("%v/%v", targetDir, fileInfo.Name()))
	defer file.Close()

	//handle errors while opening
	if err != nil {
		log.Fatalf("Error when opening file: %s", err)
	}

	fileScanner := bufio.NewScanner(file)

	// read line by line
	for fileScanner.Scan() {
		splitString := strings.Split(fileScanner.Text(), " ")
		splitString[1] = strings.Trim(splitString[1], "[]")
		dataSlice = append(dataSlice, splitString)
	}
	// handle first encountered error while reading
	if err := fileScanner.Err(); err != nil {
		log.Fatalf("Error while reading file: %s", err)
	}

	return dataSlice
}

func readOps(targetDir string) [][]string {
	var fileContents [][]string

	files, err := ioutil.ReadDir(targetDir)
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		fileContents = append(fileContents, readOpFile(targetDir, file)...)
	}

	sort.Slice(fileContents, func(i, j int) bool {
		left, _ := strconv.Atoi(fileContents[i][0])
		right, _ := strconv.Atoi(fileContents[j][0])
		return left < right
	})

	return fileContents
}
