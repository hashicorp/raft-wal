package metrics

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAtomicCollector(t *testing.T) {
	defs := Definitions{
		Counters: []Descriptor{
			{
				Name: "c1",
				Desc: "counter one.",
			},
			{
				Name: "c2",
				Desc: "counter two.",
			},
		},
		Gauges: []Descriptor{
			{
				Name: "g1",
				Desc: "gauge one.",
			},
			{
				Name: "g2",
				Desc: "gauge two.",
			},
		},
	}

	c := NewAtomicCollector(defs)

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				c.IncrementCounter("c1", 1)
				c.IncrementCounter("c2", 2)
				c.SetGauge("g1", uint64(j))
				c.SetGauge("g2", uint64(j*2))
			}
		}()
	}

	wg.Wait()

	s := c.Summary()
	require.Equal(t, 100, int(s.Counters["c1"]))
	require.Equal(t, 200, int(s.Counters["c2"]))
	require.Equal(t, 9, int(s.Gauges["g1"]))
	require.Equal(t, 18, int(s.Gauges["g2"]))
}
