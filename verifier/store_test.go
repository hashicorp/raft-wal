// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package verifier

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-wal/metrics"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	tcs := []struct {
		name  string
		steps []testStep
	}{
		{
			name: "basic verification",
			steps: newTestSteps(1234).
				AppendN(5).
				AssertCanRead("leader", LogRange{1234, 1234 + 5}).
				AppendCheckpoint().
				AssertReport("leader", LogRange{1234, 1234 + 5}, "", func(t *testing.T, r *VerificationReport) {
					require.Nil(t, r.SkippedRange)
				}).
				ReplicateTo("f1", 1239, 0).
				// Follower should report and match leader
				AssertReport("f1", LogRange{1234, 1234 + 5}, "").
				Steps(),
		},
		{
			name: "first index configuration is ignored",
			steps: newTestSteps(1).
				BootstrapNodes("leader", "f1").
				AppendN(5).
				AssertCanRead("leader", LogRange{1, 7}).
				AppendCheckpoint().
				AssertReport("leader", LogRange{1, 7}, "", func(t *testing.T, r *VerificationReport) {
					require.Nil(t, r.SkippedRange)
				}).
				ReplicateTo("f1", 7, 0).
				// Follower should report and match leader
				AssertReport("f1", LogRange{1, 7}, "").
				Steps(),
		},
		{
			name: "leader WAL corruption",
			steps: newTestSteps(1234).
				AppendN(5).
				AssertCanRead("leader", LogRange{1234, 1234 + 5}).
				CorruptWALRecord("leader", 1235).
				AppendCheckpoint().
				AssertReport("leader", LogRange{1234, 1234 + 5}, "storage corruption").
				Steps(),
		},
		{
			name: "in-flight corruption",
			steps: newTestSteps(1234).
				AppendN(5).
				AssertCanRead("leader", LogRange{1234, 1234 + 5}).
				AppendCheckpoint().
				AssertReport("leader", LogRange{1234, 1234 + 5}, "").
				ReplicateTo("f1", 1239, 1235).
				// Follower should report and detect in-flight corruption
				AssertReport("f1", LogRange{1234, 1234 + 5}, "in-flight corruption").
				AssertMetrics("f1", func(t *testing.T, s metrics.Summary) {
					require.Equal(t, 1, int(s.Counters["checkpoints_written"]))
					require.Equal(t, 1, int(s.Counters["write_checksum_failures"]))
					require.Equal(t, 0, int(s.Counters["read_checksum_failures"]))
				}).
				Steps(),
		},
		{
			name: "follower corruption",
			steps: newTestSteps(1234).
				AppendN(5).
				AssertCanRead("leader", LogRange{1234, 1234 + 5}).
				AppendCheckpoint().
				AssertReport("leader", LogRange{1234, 1234 + 5}, "").
				// Setup follower's WAL to corrupt record. We must do this before
				// replicating otherwise it's non-deterministic whether we manage to
				// mark it corrupt before the background verification is triggered by
				// replicating the checkpoint.
				CorruptWALRecord("f1", 1236).
				ReplicateTo("f1", 1239, 0).
				// Follower should report and detect storage corruption
				AssertReport("f1", LogRange{1234, 1234 + 5}, "storage corruption").
				AssertMetrics("f1", func(t *testing.T, s metrics.Summary) {
					require.Equal(t, 1, int(s.Counters["checkpoints_written"]))
					require.Equal(t, 0, int(s.Counters["write_checksum_failures"]))
					require.Equal(t, 1, int(s.Counters["read_checksum_failures"]))
				}).
				Steps(),
		},
		{
			name: "follower has partial checkpoint range",
			steps: newTestSteps(1234).
				AppendN(5).
				AssertCanRead("leader", LogRange{1234, 1234 + 5}).
				AppendCheckpoint().
				AssertReport("leader", LogRange{1234, 1234 + 5}, "").
				// Only replicate a subset of the range
				ReplicateRange("f1", 1236, 1239, 0).
				// Follower should report with special err type because it doesn't have
				// enough logs
				AssertReport("f1", LogRange{1234, 1234 + 5}, "range mismatch").
				// But next checkpoint around it should be fine
				AppendN(5).
				AppendCheckpoint().
				ReplicateTo("f1", 1234+5+5+2, 0).
				AssertReport("f1", LogRange{1239, 1239 + 5 + 1}, "").
				Steps(),
		},
		{
			name: "reportFn blocks",
			steps: newTestSteps(1).
				AppendN(5).
				AssertCanRead("leader", LogRange{1, 5}).
				BlockReporting("leader").
				AppendCheckpoint(). // CP 1 @idx=6
				// Wait for that checkpoint to trigger the report fn and that report fn
				// to block. This is important to prevent flakes. Before adding this the
				// test would pass fine locally on multiple hardware threads but fail
				// with GOMAXPROCS=1 because the current os thread would continue and
				// deliver more checkpoints before the background runVerifier goroutine
				// that is reading them from the verifyCh actually got scheduled,
				// breaking the assumptions below that the second checkpoint would be
				// buffered (since the first still was). This step ensures that we wait
				// until the runVerifier goroutine is actually processing the first one
				// (and so the buffer is free for the next) before sending that.
				WaitForReportFn("leader").
				// Deliver another three checkpoints. The first will be already done and
				// blocked on delivery so we'll get it eventually. The second will be
				// buffered on write ready for the first to finish. So will eventually
				// complete when we unblock. The third should be dropped because there
				// is no more buffer space left and we can't block writes. The fourth
				// (after unblocking) should complete but indicate the dropped range.
				AppendN(5).
				AppendCheckpoint(). // CP 2 @idx=12
				AppendN(5).
				AppendCheckpoint(). // CP 3 @idx=18 (dropped)
				AppendN(5).
				UnblockReporting("leader").
				// First report should be delivered now
				AssertReport("leader", LogRange{1, 6}, "").
				// As should second which was buffered
				AssertReport("leader", LogRange{6, 12}, "").
				AppendCheckpoint(). // CP 4 @idx=24
				// Next report should be the fourth one since third was skipped while
				// verifier was blocked.
				AssertReport("leader", LogRange{18, 24}, "", func(t *testing.T, r *VerificationReport) {
					require.Equal(t, &LogRange{12, 18}, r.SkippedRange)
				}).
				AssertMetrics("leader", func(t *testing.T, s metrics.Summary) {
					require.Equal(t, 1, int(s.Counters["dropped_reports"]))
					require.Equal(t, 3, int(s.Counters["ranges_verified"]))
				}).
				Steps(),
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			peers := newPeerSet()
			defer peers.Close()

			for _, step := range tc.steps {
				t.Logf(" -> test step %s", step)
				switch {
				case step.appendBatch != nil:
					// Append batch to leader's store
					ls := peers.logStore("leader")
					require.NoError(t, ls.StoreLogs(step.appendBatch))

				case len(step.bootstrapNodes) > 0:
					// When we bootstrap we write a new configuration into log index 1
					// This actually happens independently on all servers in Raft and so
					// the entry itself may not be byte-for-byte identical (although it
					// should be logically identical). For example in Consul this comes
					// from Serf gossip discovery and so the list of servers might be
					// different on each server. That means validating the first log entry
					// is likely to fail since it's legitimately different on each server.
					// To simulate this, write a different entry to each server as
					// "config".
					for _, n := range step.bootstrapNodes {
						// create the peer
						ls := peers.logStore(n)
						cfg := raft.Log{
							Index: 1,
							Type:  raft.LogConfiguration,
							Data:  []byte(fmt.Sprintf("config for %s", n)),
						}
						require.NoError(t, ls.StoreLog(&cfg))
					}

				case step.replicateMax != 0:
					leader := peers.logStore("leader")
					follow := peers.logStore(step.targetNode)
					replicate(t, leader, follow, step.replicateMin, step.replicateMax, step.corruptInFlight)

				case step.assertCanReadRange.Start != 0:
					assertCanRead(t, peers.logStore(step.targetNode),
						step.assertCanReadRange.Start, step.assertCanReadRange.End)

				case step.assertReportRange.Start != 0:
					r := assertReportDelivered(t, peers.reportCh(step.targetNode))
					t.Logf("Report: %#v", r)
					if step.wantError != "" {
						require.ErrorContains(t, r.Err, step.wantError)
					} else {
						require.NoError(t, r.Err)
					}
					require.Equal(t, step.assertReportRange, r.Range)
					for _, fn := range step.extraAssertions {
						fn(t, r)
					}

				case step.corruptWALIndex != 0:
					// Corrupt the "disk" log of a peer at this index
					ts := peers.testStore(step.targetNode)
					ts.Corrupt(step.corruptWALIndex)

				case step.blockReporting:
					peers.blockReportFn(step.targetNode)

				case step.unblockReporting:
					peers.unblockReportFn(step.targetNode)

				case step.waitForReportFn:
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					require.NoError(t, peers.waitForReportFn(ctx, step.targetNode))
					cancel()

				case step.assertMetrics != nil:
					ms := peers.metrics(step.targetNode)
					step.assertMetrics(t, ms)

				default:
					t.Fatalf("invalid testStep: %#v", step)
				}
			}

		})
	}
}

type reportBlocker struct {
	sync.Mutex

	blocked bool
	waiting bool
	waitErr error

	waitingSig   sync.Cond
	unblockedSig sync.Cond
}

func newReportBlocker() *reportBlocker {
	b := &reportBlocker{}
	b.waitingSig.L = &b.Mutex
	b.unblockedSig.L = &b.Mutex
	return b
}

func (b *reportBlocker) WaitBlock() {
	b.Lock()
	defer b.Unlock()

	first := true
	b.waiting = true

	for b.blocked {
		if first {
			// We need to wait. First signal that we are waiting so the test can be
			// deterministic about what state the reporting goroutine is before moving
			// on.
			b.waitingSig.Broadcast()
			first = false
		}

		// Now wait for unblock (and re-check!)
		b.unblockedSig.Wait()
	}

	b.waiting = false
}

func (b *reportBlocker) WaitForNextReportBlocked(ctx context.Context) error {
	b.Lock()
	defer b.Unlock()

	if !b.blocked {
		// We're not blocked so the reportFn is not going to wait
		return errors.New("reports are not blocked")
	}

	if b.waitErr != nil {
		return b.waitErr
	}

	if b.waiting {
		// Report fn is already waiting so it already fired the waitingSig
		return nil
	}

	// Unblock the waiter if context is cancelled
	go func() {
		<-ctx.Done()
		b.Lock()
		defer b.Unlock()

		b.waitErr = ctx.Err()
		b.waitingSig.Broadcast()
	}()

	b.waitingSig.Wait()
	return b.waitErr
}

func (b *reportBlocker) Block() {
	b.Lock()
	defer b.Unlock()

	b.blocked = true
}

func (b *reportBlocker) Unblock() {
	b.Lock()
	defer b.Unlock()

	b.blocked = false
	b.unblockedSig.Broadcast()
}

type peerSet struct {
	lss    map[string]*LogStore
	tss    map[string]*testStore
	chs    map[string]chan VerificationReport
	blocks map[string]*reportBlocker
	mcs    map[string]*metrics.AtomicCollector
}

func newPeerSet() *peerSet {
	return &peerSet{
		lss:    make(map[string]*LogStore),
		tss:    make(map[string]*testStore),
		chs:    make(map[string]chan VerificationReport),
		blocks: make(map[string]*reportBlocker),
		mcs:    make(map[string]*metrics.AtomicCollector),
	}
}

func (s *peerSet) Close() error {
	for node, ls := range s.lss {
		ls.Close()
		delete(s.lss, node)
		delete(s.tss, node)
		delete(s.chs, node)
		delete(s.mcs, node)
		// Don't close chans as it causes panics.
	}
	return nil
}

func cpFn(l *raft.Log) (bool, error) {
	return bytes.Equal(l.Data, []byte("CHECKPOINT")), nil
}

func (s *peerSet) init(node string) (*LogStore, *testStore, chan VerificationReport, *metrics.AtomicCollector) {
	ts := &testStore{}

	ch := make(chan VerificationReport, 20)
	block := newReportBlocker()
	reportFn := func(vr VerificationReport) {
		block.WaitBlock()
		ch <- vr
	}

	metrics := metrics.NewAtomicCollector(MetricDefinitions)
	ls := NewLogStore(ts, cpFn, reportFn, metrics)

	s.lss[node] = ls
	s.tss[node] = ts
	s.chs[node] = ch
	s.mcs[node] = metrics
	s.blocks[node] = block
	return ls, ts, ch, metrics
}

func (s *peerSet) logStore(node string) *LogStore {
	ls, ok := s.lss[node]
	if !ok {
		ls, _, _, _ = s.init(node)
	}
	return ls
}

func (s *peerSet) testStore(node string) *testStore {
	ts, ok := s.tss[node]
	if !ok {
		_, ts, _, _ = s.init(node)
	}
	return ts
}

func (s *peerSet) reportCh(node string) chan VerificationReport {
	ch, ok := s.chs[node]
	if !ok {
		_, _, ch, _ = s.init(node)
	}
	return ch
}

func (s *peerSet) metrics(node string) metrics.Summary {
	mc, ok := s.mcs[node]
	if !ok {
		_, _, _, mc = s.init(node)
	}
	return mc.Summary()
}

func (s *peerSet) waitForReportFn(ctx context.Context, node string) error {
	return s.blocks[node].WaitForNextReportBlocked(ctx)
}

func (s *peerSet) blockReportFn(node string) {
	s.blocks[node].Block()
}

func (s *peerSet) unblockReportFn(node string) {
	s.blocks[node].Unblock()
}

func assertReportDelivered(t *testing.T, ch <-chan VerificationReport) *VerificationReport {
	t.Helper()
	select {
	case r := <-ch:
		return &r

	case <-time.After(5 * time.Second):
		t.Fatalf("didn't get report after 5 seconds!")
	}
	return nil
}

func assertCanRead(t *testing.T, s raft.LogStore, start, end uint64) {
	t.Helper()
	var log raft.Log
	for idx := start; idx < end; idx++ {
		require.NoError(t, s.GetLog(idx, &log), "failed reading idx=%d", idx)
		require.Equal(t, int(idx), int(log.Index), "failed reading idx=%d, got idx=%d", idx, log.Index)
		if !bytes.Equal(log.Data, []byte("CHECKPOINT")) && log.Type != raft.LogConfiguration {
			require.Equal(t, fmt.Sprintf("LOG(%d)", idx), string(log.Data),
				"failed reading idx=%d", idx)
		}
	}
}

func replicate(t *testing.T, leader, follower raft.LogStore, start, end, corrupt uint64) {
	t.Helper()

	first, err := leader.FirstIndex()
	require.NoError(t, err)

	last, err := leader.LastIndex()
	require.NoError(t, err)

	if start < first {
		start = first
	}

	if end > last {
		end = last
	}

	for idx := start; idx <= end; idx++ {
		var log raft.Log
		require.NoError(t, leader.GetLog(idx, &log), "failed reading idx=%d", idx)
		if corrupt == idx {
			// Tamper the log "in flight"
			log.Data = []byte(fmt.Sprintf("CORRUPT_IN_FLIGHT(%d)", idx))
		}
		require.NoError(t, follower.StoreLog(&log), "failed writing idx=%d", idx)
	}
}

type testBuilder struct {
	nextIndex uint64
	steps     []testStep
	peers     map[string]uint64
}

type testStep struct {
	appendBatch    []*raft.Log
	bootstrapNodes []string
	checkPoint     bool

	targetNode string

	replicateMin    uint64
	replicateMax    uint64
	corruptInFlight uint64

	corruptWALIndex uint64

	assertReportRange LogRange
	wantError         string
	extraAssertions   []func(t *testing.T, r *VerificationReport)

	assertCanReadRange LogRange

	blockReporting   bool
	unblockReporting bool
	waitForReportFn  bool

	assertMetrics func(t *testing.T, s metrics.Summary)
}

func (s testStep) String() string {
	switch {
	case s.appendBatch != nil && !s.checkPoint:
		return fmt.Sprintf("append(%d)", len(s.appendBatch))

	case s.appendBatch != nil && s.checkPoint:
		return fmt.Sprintf("checkpoint()")

	case s.replicateMax != 0:
		corrupt := ""
		if s.corruptInFlight != 0 {
			corrupt = fmt.Sprintf(", corrupting=%d", s.corruptInFlight)
		}
		return fmt.Sprintf("replicate(to=%s, range=[%d, %d)%s)", s.targetNode,
			s.replicateMin, s.replicateMax, corrupt)

	case s.assertReportRange.Start != 0:
		extras := ""
		if len(s.extraAssertions) > 0 {
			extras = fmt.Sprintf(", extras[%d]", len(s.extraAssertions))
		}
		return fmt.Sprintf("assertReport(node=%s, range=%s, wantErr=%q%s)",
			s.targetNode, s.assertReportRange, s.wantError, extras)

	case s.assertCanReadRange.Start != 0:
		return fmt.Sprintf("assertCanRead(node=%s, range=%s)",
			s.targetNode, s.assertCanReadRange)

	case s.blockReporting:
		return fmt.Sprintf("blockReporting(%s)", s.targetNode)

	case s.unblockReporting:
		return fmt.Sprintf("unblockReporting(%s)", s.targetNode)

	case s.waitForReportFn:
		return fmt.Sprintf("waitForReportFn(%s)", s.targetNode)

	case s.assertMetrics != nil:
		return fmt.Sprintf("assertMetrics(%s)", s.targetNode)

	case s.corruptWALIndex != 0:
		return fmt.Sprintf("corruptWal(%s, %d)", s.targetNode, s.corruptWALIndex)

	default:
		return fmt.Sprintf("invalid step")
	}
}

func newTestSteps(startIdx uint64) *testBuilder {
	if startIdx == 0 {
		startIdx = 1
	}
	return &testBuilder{
		nextIndex: startIdx,
		peers:     make(map[string]uint64),
	}
}

func (b *testBuilder) AppendN(n int) *testBuilder {
	step := testStep{}
	for i := 0; i < n; i++ {
		step.appendBatch = append(step.appendBatch, &raft.Log{
			Index: b.nextIndex,
			Data:  []byte(fmt.Sprintf("LOG(%d)", b.nextIndex)),
		})
		b.nextIndex++
	}
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) BootstrapNodes(nodes ...string) *testBuilder {
	step := testStep{}
	// Bootstrap config must be index 1
	step.bootstrapNodes = nodes
	b.nextIndex = 2
	for _, n := range nodes {
		b.peers[n] = 1
	}
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) AppendCheckpoint() *testBuilder {
	step := testStep{
		appendBatch: []*raft.Log{{
			Index: b.nextIndex,
			Data:  []byte("CHECKPOINT"),
		}},
		checkPoint: true,
	}
	b.nextIndex++
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) ReplicateTo(node string, upTo, corruptInFlight uint64) *testBuilder {
	peerIdx := b.peers[node]
	step := testStep{
		targetNode:      node,
		replicateMin:    peerIdx + 1,
		replicateMax:    upTo,
		corruptInFlight: corruptInFlight,
	}
	b.peers[node] = upTo
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) ReplicateRange(node string, start, end uint64, corruptInFlight uint64) *testBuilder {
	step := testStep{
		targetNode:      node,
		replicateMin:    start,
		replicateMax:    end,
		corruptInFlight: corruptInFlight,
	}
	b.peers[node] = end
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) CorruptWALRecord(node string, idx uint64) *testBuilder {
	step := testStep{
		targetNode:      node,
		corruptWALIndex: idx,
	}
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) AssertReport(node string, over LogRange, wantError string, extras ...func(t *testing.T, r *VerificationReport)) *testBuilder {
	step := testStep{
		targetNode:        node,
		assertReportRange: over,
		wantError:         wantError,
		extraAssertions:   extras,
	}
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) AssertCanRead(node string, r LogRange) *testBuilder {
	step := testStep{
		targetNode:         node,
		assertCanReadRange: r,
	}
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) BlockReporting(node string) *testBuilder {
	step := testStep{
		targetNode:     node,
		blockReporting: true,
	}
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) UnblockReporting(node string) *testBuilder {
	step := testStep{
		targetNode:       node,
		unblockReporting: true,
	}
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) WaitForReportFn(node string) *testBuilder {
	step := testStep{
		targetNode:      node,
		waitForReportFn: true,
	}
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) AssertMetrics(node string, fn func(t *testing.T, m metrics.Summary)) *testBuilder {
	step := testStep{
		targetNode:    node,
		assertMetrics: fn,
	}
	b.steps = append(b.steps, step)
	return b
}

func (b *testBuilder) Steps() []testStep {
	return b.steps
}

type testStore struct {
	mu          sync.Mutex
	logs        []*raft.Log
	corruptions map[uint64]struct{}
}

func (s *testStore) Corrupt(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.corruptions == nil {
		s.corruptions = make(map[uint64]struct{})
	}
	s.corruptions[index] = struct{}{}
}

// FirstIndex returns the first index written. 0 for no entries.
func (s *testStore) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.logs) == 0 {
		return 0, nil
	}
	return s.logs[0].Index, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (s *testStore) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.lastIndexLocked(), nil
}

func (s *testStore) lastIndexLocked() uint64 {
	if len(s.logs) == 0 {
		return 0
	}
	return s.logs[len(s.logs)-1].Index
}

// GetLog gets a log entry at a given index.
func (s *testStore) GetLog(index uint64, log *raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, err := s.getLogLocked(index)
	if err != nil {
		return err
	}
	*log = *l

	if _, ok := s.corruptions[index]; ok {
		(*log).Data = []byte(fmt.Sprintf("CORRUPT(%d)", index))
	}

	return nil
}

func (s *testStore) getLogLocked(index uint64) (*raft.Log, error) {
	if len(s.logs) < 1 {
		return nil, raft.ErrLogNotFound
	}
	first := s.logs[0].Index
	if index < first {
		return nil, raft.ErrLogNotFound
	}
	delta := index - first
	if delta >= uint64(len(s.logs)) {
		return nil, raft.ErrLogNotFound
	}
	return s.logs[delta], nil
}

// StoreLog stores a log entry.
func (s *testStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (s *testStore) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastIdx := s.lastIndexLocked()
	for _, log := range logs {
		if lastIdx > 0 && lastIdx != (log.Index-1) {
			return fmt.Errorf("non monotonic indexes: %d after %d", log.Index, lastIdx)
		}
		s.logs = append(s.logs, log)
		lastIdx = log.Index
	}
	return nil
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (s *testStore) DeleteRange(min uint64, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.logs) < 1 {
		return nil
	}

	first, last := s.logs[0].Index, s.logs[len(s.logs)-1].Index
	if max < first || min > last || min > max {
		return nil
	}

	// Just copy out a new slice to keep it simple
	newLogs := make([]*raft.Log, 0, len(s.logs))
	for _, log := range s.logs {
		if log.Index < min || log.Index > max {
			newLogs = append(newLogs, log)
		}
	}
	s.logs = newLogs
	return nil
}
