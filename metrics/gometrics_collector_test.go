// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package metrics

import (
	"testing"
	"time"

	gometrics "github.com/armon/go-metrics"
	"github.com/stretchr/testify/require"
)

func TestGoMetricsCollector(t *testing.T) {
	cfg := &gometrics.Config{
		EnableHostname:       false,
		EnableRuntimeMetrics: false,
		// FilterDefault is super weird and backwards but "true" means "don't
		// filter"!
		FilterDefault: true,
	}
	sink := gometrics.NewInmemSink(1*time.Second, 10*time.Second)
	gm, err := gometrics.New(cfg, sink)
	require.NoError(t, err)

	c := NewGoMetricsCollector(
		[]string{"myapp", "wal"},
		[]gometrics.Label{{Name: "label", Value: "foo"}},
		gm,
	)

	c.IncrementCounter("counter_one", 1)
	c.IncrementCounter("counter_one", 1)
	c.IncrementCounter("counter_two", 10)

	c.SetGauge("g1", 12345)

	summary := flattenData(sink.Data())

	require.Equal(t, 2, int(summary.Counters["myapp.wal.counter_one;label=foo"]))
	require.Equal(t, 10, int(summary.Counters["myapp.wal.counter_two;label=foo"]))

	require.Equal(t, 12345, int(summary.Gauges["myapp.wal.g1;label=foo"]))

}

func flattenData(ivs []*gometrics.IntervalMetrics) Summary {
	s := Summary{
		Counters: make(map[string]uint64),
		Gauges:   make(map[string]uint64),
	}
	for _, iv := range ivs {
		for name, v := range iv.Counters {
			s.Counters[name] += uint64(v.Sum)
		}
		for name, v := range iv.Gauges {
			s.Gauges[name] = uint64(v.Value)
		}
	}
	return s
}
