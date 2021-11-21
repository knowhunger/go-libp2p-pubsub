package pubsub

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
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
}

type hmsgInfo struct {
	timestamp  int64
	delay, hop int
}

func (es *experimentsStats) evaluateStat(evt *pb.TraceEvent) {
	rand.Seed(time.Now().UnixNano())
	min := 50
	max := 200

	switch evt.GetType() {
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
					for i := 0; i < int(*evt.RecvRPC.Meta.Messages[0].Hop+1); i++ {
						networkDelay += rand.Intn(max-min+1) + min
					}
					es.hmsg[string(msg.MessageID)] = &hmsgInfo{
						timestamp: *evt.Timestamp,
						delay: int((*evt.Timestamp-*evt.RecvRPC.Meta.Messages[0].Createtime)/1000000) +
							networkDelay,
						hop: int(*evt.RecvRPC.Meta.Messages[0].Hop + 1),
					}
				}
			}
		}
		if len(evt.RecvRPC.Meta.Jmp) > 0 {
			for _, jmp := range evt.RecvRPC.Meta.Jmp {
				es.rmsg += len(jmp.JmpMsgs)
			}
		}
	case pb.TraceEvent_SEND_RPC:
		if len(evt.SendRPC.Meta.Messages) > 0 {
			// check only msg rpc
			for _ = range evt.SendRPC.Meta.Messages {
				// check smsg
				es.smsg++
			}
		}
		if len(evt.SendRPC.Meta.Jmp) > 0 {
			for _, jmp := range evt.SendRPC.Meta.Jmp {
				es.smsg += len(jmp.JmpMsgs)
			}
		}
	case pb.TraceEvent_DUPLICATE_MESSAGE:
		es.dmsg++
	case pb.TraceEvent_HIT_MESSAGE:
		es.hitmsg++
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

func printStat(psubs []*PubSub) {
	fmt.Println("printStat starts")
	type statGroup struct {
		gmsg, smsg, rmsg, hmsg int
		delay, hop             []int
		dmsg, hitmsg           int
	}

	var wg sync.WaitGroup
	totalStat := &statGroup{}
	totalStatChan := make(chan statGroup, len(psubs))
	for i := 0; i < len(psubs); i++ {
		wg.Add(1)
		go func(i int, totalStatChan chan statGroup) {
			//var gmsg, smsg, rmsg, hmsg int
			//var dmsg int
			var delay, hop []int
			var evt pb.TraceEvent
			stats := &experimentsStats{hmsg: make(map[string]*hmsgInfo)}

			f, err := os.Open(fmt.Sprintf("./trace_out/tracer_%d.json", i))
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

			fmt.Println("peer", i, "'s Stat")
			fmt.Println("gmsg cnt", stats.gmsg)
			fmt.Println("smsg cnt", stats.smsg)
			fmt.Println("rmsg cnt", stats.rmsg)
			fmt.Println("hmsg cnt", len(stats.hmsg))
			fmt.Println("dmsg cnt", stats.dmsg)
			fmt.Println("hitmsg cnt", stats.hitmsg)

			for _, hm := range stats.hmsg {
				delay = append(delay, hm.delay)
				hop = append(hop, hm.hop)
			}

			fmt.Println()

			totalStatChan <- statGroup{
				gmsg: stats.gmsg, smsg: stats.smsg, rmsg: stats.rmsg, hmsg: len(stats.hmsg),
				delay: delay, hop: hop,
				dmsg: stats.dmsg, hitmsg: stats.hitmsg}
			wg.Done()
		}(i, totalStatChan)
	}

	wg.Wait()
	close(totalStatChan)

	//cnt := 0
	for c := range totalStatChan {
		totalStat.gmsg += c.gmsg
		totalStat.smsg += c.smsg
		totalStat.rmsg += c.rmsg
		totalStat.hmsg += c.hmsg
		totalStat.delay = append(totalStat.delay, c.delay...)
		totalStat.hop = append(totalStat.hop, c.hop...)
		totalStat.dmsg += c.dmsg
		totalStat.hitmsg += c.hitmsg
		//cnt++
		//if cnt == len(psubs) {
		//	close(totalStatChan)
		//}
	}

	fmt.Println("total gmsg: ", totalStat.gmsg)
	fmt.Println("total smsg: ", totalStat.smsg)
	fmt.Println("total rmsg: ", totalStat.rmsg)
	fmt.Println("total hmsg: ", totalStat.hmsg)
	fmt.Println("excepted hmsg: ", totalStat.gmsg*(len(psubs)-1))

	fmt.Println("total dmsg", totalStat.dmsg)
	fmt.Println("total hitmsg", totalStat.hitmsg)

	//var coverage float64
	coverage := float64(totalStat.hmsg) / (float64(totalStat.gmsg) * float64(len(psubs)-1))
	fmt.Println("final Coverage: ", coverage)

	hitCoverage := float64(totalStat.hitmsg) / (float64(totalStat.gmsg) * float64(len(psubs)-1))
	fmt.Println("final hit Coverage: ", hitCoverage)

	//var redundancy float64
	redundancy := float64(totalStat.smsg) / (float64(totalStat.gmsg) * float64(len(psubs)-1))
	fmt.Println("final Redundancy: ", redundancy)

	//calculateCoverage(totalGmsg, hmsgPerUnitTime, endTime, len(psubs))

	delayMap := dupCounter(totalStat.delay)
	//fmt.Println(delayMap)
	drawCoveragePlot(delayMap, totalStat.gmsg*(len(psubs)-1), "coverage_per_delay")

	hopMap := dupCounter(totalStat.hop)
	//fmt.Println(hopMap)
	drawCoveragePlot(hopMap, totalStat.gmsg*(len(psubs)-1), "coverage_per_hop")
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
