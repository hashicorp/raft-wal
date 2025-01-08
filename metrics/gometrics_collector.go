// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

// # Metrics Configuration
//
// The raft-wal library is instrumented to be able to use different metrics collectors. There are currently two implemented within this package:
//   - atomic
//   - go-metrics
//
// # go-metrics Compatibility
//
// This library can emit metrics using either github.com/armon/go-metrics or github.com/hashicorp/go-metrics. Choosing between the libraries is controlled via build tags.
//
// Build Tags:
//   - armonmetrics - Using this tag will cause metrics to be routed to armon/go-metrics
//   - hashicorpmetrics - Using this tag will cause all metrics to be routed to hashicorp/go-metrics
//
// If no build tag is specified, the default behavior is to use armon/go-metrics.
//
// # Deprecating armon/go-metrics
//
// Emitting metrics to armon/go-metrics is officially deprecated. Usage of armon/go-metrics will remain the default until mid-2025 with opt-in support continuing to the end of 2025.
//
// Migration:
// To migrate an application currently using the older armon/go-metrics to instead use hashicorp/go-metrics the following should be done.
//
//  1. Upgrade libraries using armon/go-metrics to consume hashicorp/go-metrics/compat instead. This should involve only changing import statements. All repositories within the hashicorp GitHub organization will be getting these updates in early 2025.
//
//  2. Update an applications library dependencies to those that have the compatibility layer configured.
//
//  3. Update the application to use hashicorp/go-metrics for configuring metrics export instead of armon/go-metrics
//
//     - Replace all application imports of github.com/armon/go-metrics with github.com/hashicorp/go-metrics
//
//     - Instrument your build system to build with the hashicorpmetrics tag.
//
// Eventually once the default behavior changes to use hashicorp/go-metrics by default (mid-2025), you can drop the hashicorpmetrics build tag.
package metrics

import gometrics "github.com/hashicorp/go-metrics/compat"

// GoMetricsCollector implements a Collector that passes through observations to
// a go-metrics instance. The zero value works, writing metrics to the default
// global instance however to set a prefix or a static set of labels to add to
// each metric observed, or to use a non-global metrics instance use
// NewGoMetricsCollector.
type GoMetricsCollector struct {
	gm     *gometrics.Metrics
	prefix []string
	labels []gometrics.Label
}

// NewGoMetricsCollector returns a GoMetricsCollector that will attach the
// specified name prefix and/or labels to each observation. If gm is nil the
// global metrics instance is used.
func NewGoMetricsCollector(prefix []string, labels []gometrics.Label, gm *gometrics.Metrics) *GoMetricsCollector {
	if gm == nil {
		gm = gometrics.Default()
	}
	return &GoMetricsCollector{
		gm:     gm,
		prefix: prefix,
		labels: labels,
	}
}

// IncrementCounter record val occurrences of the named event. Names will
// follow prometheus conventions with lower_case_and_underscores. We don't
// need any additional labels currently.
func (c *GoMetricsCollector) IncrementCounter(name string, delta uint64) {
	c.gm.IncrCounterWithLabels(c.name(name), float32(delta), c.labels)
}

// SetGauge sets the value of the named gauge overriding any previous value.
func (c *GoMetricsCollector) SetGauge(name string, val uint64) {
	c.gm.SetGaugeWithLabels(c.name(name), float32(val), c.labels)
}

// name returns the metric name as a slice we don't want to risk modifying the
// prefix slice backing array since this might be called concurrently so we
// always allocate a new slice.
func (c *GoMetricsCollector) name(name string) []string {
	var ss []string
	return append(append(ss, c.prefix...), name)
}
