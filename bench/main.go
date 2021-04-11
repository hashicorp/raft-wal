package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	raftwal "github.com/notnoop/raft-wal"
	chart "github.com/wcharczuk/go-chart/v2"
)

func main() {
	var logStore, chartFile string
	var n, size, bSize int
	var snapInterval time.Duration
	flag.StringVar(&logStore, "log-store", "bolt", "the LogStore implementation"+
		" to use, one of 'bolt', 'wal'")
	flag.IntVar(&n, "n", 100000, "number of log entries to write")
	flag.IntVar(&size, "size", 2*1024, "size (in bytes) of each log entry")
	flag.IntVar(&bSize, "batch-size", 1, "append batch size")
	flag.StringVar(&chartFile, "chart", "", "file to write chart PNG to (optional)")
	flag.DurationVar(&snapInterval, "snap-interval", 10*time.Second, "snaphost interval")

	flag.Parse()

	tmpDir, err := ioutil.TempDir("", "raft-wal")
	if err != nil {
		log.Fatalf("Failed creating tmp dir: %s", err)
	}

	var ls raft.LogStore
	switch logStore {
	case "bolt":
		ls, err = raftboltdb.NewBoltStore(tmpDir + "/bolt.db")
		if err != nil {
			log.Fatalf("Failed initializing boltdb: %s", err)
		}
	case "wal":
		ls, err = raftwal.NewWAL(tmpDir, raftwal.LogConfig{NoSync: false})
		if err != nil {
			log.Fatalf("Failed initializing wal: %s", err)
		}
	case "wal-nosync":
		ls, err = raftwal.NewWAL(tmpDir, raftwal.LogConfig{NoSync: true})
		if err != nil {
			log.Fatalf("Failed initializing wal: %s", err)
		}
	default:
		log.Fatalf("LogStore %s not implemented", logStore)
	}

	title := fmt.Sprintf("writeAndTruncate with store=%s, n=%d size=%d "+
		"batchSize=%d snapInterval=%s", logStore, n, size, bSize,
		snapInterval)

	fmt.Printf("==> Running %s\n", title)

	samples := writeAndTruncate(ls, n, size, bSize, snapInterval)

	if len(samples) > 0 && chartFile != "" {
		drawRateChart(title, chartFile, samples)
	}
}

func writeAndTruncate(ls raft.LogStore, n, size, bSize int, snapInterval time.Duration) []uint64 {
	// First create some random payloads to write outside the timed loop
	randEntries := randEntries(bSize, size)

	index := uint64(1)

	stop := make(chan struct{})
	sampleCh := make(chan []uint64)
	var wg sync.WaitGroup

	t0 := time.Now()

	// Start a background goroutine to collect samples every second
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(1 * time.Second)
		second := 0
		prev := uint64(0)
		s := make([]uint64, 0, 300)
		fmt.Println("Second, Entries Appended, Cumulative")
		s = append(s, 0)
		fmt.Printf("% 4d, % 8d, % 8d\n", 0, 0, 0)
		for {
			select {
			case <-stop:
				idx := atomic.LoadUint64(&index)
				second++
				s = append(s, idx-prev)
				sampleCh <- s
				fmt.Printf("% 4d, % 8d, % 8d\n", second, idx-prev, idx)
				elapsed := time.Since(t0)
				rate := float64(idx) / elapsed.Seconds()
				li, err := ls.LastIndex()
				if err != nil {
					fmt.Printf("Failed fetching last index: %s", err)
				}
				fi, err := ls.FirstIndex()
				if err != nil {
					fmt.Printf("Failed fetching first index: %s", err)
				}
				fmt.Printf("Logs remaining: %d [%d, %d]\n", li-fi, fi, li)
				fmt.Printf("Total: %d in %s (mean %0.1f per second)\n", idx,
					elapsed, rate)
				return
			case <-t.C:
				// Output for now
				idx := atomic.LoadUint64(&index)
				second++
				s = append(s, idx-prev)
				fmt.Printf("% 4d, % 8d, % 8d\n", second, idx-prev, idx)
				prev = idx
			}
		}
	}()

	// Start a background goroutine that truncates the log to the last 10k
	// entries every interval.
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(snapInterval)
		minIdx := uint64(0)
		truncate := func() {
			idx := atomic.LoadUint64(&index)
			if idx > 10000 {
				err := ls.DeleteRange(minIdx, idx-10000)
				if err != nil {
					log.Fatalf("Failed truncating log: %s", err)
				}
				// Truncate is inclusive
				minIdx = idx - 10000 + 1
			}
		}
		for {
			select {
			case <-stop:
				truncate()
				return
			case <-t.C:
				truncate()
			}
		}
	}()

	for {
		indexV := atomic.LoadUint64(&index)
		if index >= uint64(n) {
			break
		}

		logs := make([]*raft.Log, bSize)
		for i := range logs {
			logs[i] = &raft.Log{Index: indexV + uint64(i), Term: 1, Data: randEntries[i]}
		}
		err := ls.StoreLogs(logs)
		if err != nil {
			log.Fatalf("Failed storing logs: %s", err)
		}
		atomic.AddUint64(&index, uint64(bSize))
	}
	close(stop)
	samples := <-sampleCh
	wg.Wait()
	return samples
}

func randEntries(n, size int) [][]byte {
	logs := make([][]byte, n)
	for i := 0; i < n; i++ {
		logs[i] = make([]byte, size)
		_, err := rand.Read(logs[i])
		if err != nil {
			log.Fatalf("Failed generating random bytes: %s", err)
		}
	}
	return logs
}

func drawRateChart(title, file string, samples []uint64) {

	xs := make([]float64, len(samples))
	ys := make([]float64, len(samples))

	// x-axis ticks max of 15 ticks
	numTicks := 15
	len := len(samples)
	mult := 1
	for len/mult > numTicks {
		mult += 5
	}
	numTicks = len / mult
	ticks := make([]chart.Tick, numTicks)
	for t := 0; t < numTicks; t++ {
		ticks[t].Value = float64(t * mult)
		ticks[t].Label = strconv.Itoa(t * mult)
	}

	for i, s := range samples {
		xs[i] = float64(i)
		ys[i] = float64(s)
	}

	graph := chart.Chart{
		Title: title,
		TitleStyle: chart.Style{
			Hidden:   false,
			FontSize: 14,
		},
		Background: chart.Style{
			Padding: chart.Box{
				Top:    80,
				Left:   30,
				Bottom: 30,
				Right:  30,
			},
		},
		XAxis: chart.XAxis{
			Name:      "Elapsed Seconds",
			NameStyle: chart.Shown(),
			Style:     chart.Shown(),
			Ticks:     ticks,
		},
		YAxis: chart.YAxis{
			Name:      "Log Entries Appended Per Second",
			NameStyle: chart.Shown(),
			Style:     chart.Shown(),
			AxisType:  chart.YAxisSecondary,
			ValueFormatter: func(v interface{}) string {
				vFlt := v.(float64)
				return strconv.Itoa(int(vFlt))
			},
		},
		Series: []chart.Series{
			chart.ContinuousSeries{
				Style: chart.Style{
					Hidden:      false,
					StrokeColor: chart.GetDefaultColor(0).WithAlpha(64),
					FillColor:   chart.GetDefaultColor(0).WithAlpha(64),
				},
				XValues: xs,
				YValues: ys,
			},
		},
	}

	f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed opening chart file: %s", err)
	}
	defer f.Close()
	err = graph.Render(chart.PNG, f)
	if err != nil {
		log.Fatalf("Failed writing chart file: %s", err)
	}
}
