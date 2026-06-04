package kgo

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestKeepFetchEOF verifies that, with the KeepFetchEOF option, a partition
// consumed up to its high watermark surfaces ErrFetchEOF exactly once, and that
// the signal re-arms after more records are produced.
func TestKeepFetchEOF(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopicPartitions(t, 1)
	defer cleanup()

	pcl, _ := newTestClient(DefaultProduceTopic(topic), UnknownTopicRetries(-1))
	defer pcl.Close()

	produce := func(n int) {
		t.Helper()
		recs := make([]*Record, n)
		for i := range recs {
			recs[i] = StringRecord("v")
		}
		if err := pcl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	produce(5)

	cl, _ := newTestClient(
		ConsumeTopics(topic),
		ConsumeResetOffset(NewOffset().AtStart()),
		KeepFetchEOF(),
		// Keep the poll snappy so reaching the end is observed quickly.
		FetchMaxWait(100*time.Millisecond),
	)
	defer cl.Close()

	// consumeToEOF polls until it observes ErrFetchEOF for the partition,
	// returning how many records were delivered before the EOF.
	consumeToEOF := func() int {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var got int
		for {
			fs := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				t.Fatalf("timed out waiting for EOF after %d records", got)
			}
			var sawEOF bool
			fs.EachPartition(func(p FetchTopicPartition) {
				// EOF rides on the fetch that catches us up to the high
				// watermark, so this partition may also carry that fetch's
				// final records.
				got += len(p.Records)
				if errors.Is(p.Err, ErrFetchEOF) {
					sawEOF = true
				}
			})
			if sawEOF {
				return got
			}
		}
	}

	// We must read every record before EOF is reported, and EOF must be
	// reported once we reach the high watermark.
	if got := consumeToEOF(); got != 5 {
		t.Fatalf("read %d records before EOF, want 5", got)
	}

	// Producing again re-arms the signal: consumption resumes and a fresh EOF
	// is reported once we reach the new high watermark.
	produce(3)
	if got := consumeToEOF(); got != 3 {
		t.Fatalf("read %d records before second EOF, want 3", got)
	}
}

// TestNoFetchEOFByDefault verifies that without KeepFetchEOF a caught-up
// partition produces no EOF (or any) error: the default behavior is unchanged
// and a poll past the end simply blocks until the context is canceled.
func TestNoFetchEOFByDefault(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopicPartitions(t, 1)
	defer cleanup()

	pcl, _ := newTestClient(DefaultProduceTopic(topic), UnknownTopicRetries(-1))
	defer pcl.Close()
	if err := pcl.ProduceSync(context.Background(), StringRecord("v"), StringRecord("v")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	cl, _ := newTestClient(
		ConsumeTopics(topic),
		ConsumeResetOffset(NewOffset().AtStart()),
		FetchMaxWait(100*time.Millisecond),
	)
	defer cl.Close()

	// Drain the two records.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var got int
	for got < 2 {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("only read %d of 2 records", got)
		}
		fs.EachError(func(_ string, _ int32, err error) {
			t.Errorf("unexpected fetch error: %v", err)
		})
		got += len(fs.Records())
	}

	// Now caught up. Without KeepFetchEOF, a poll must not surface any error
	// (no EOF) and no records; it just blocks until our short context expires.
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	fs := cl.PollFetches(ctx2)
	if err := fs.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error after catching up: %v", err)
	}
	if n := len(fs.Records()); n != 0 {
		t.Fatalf("got %d records after catching up, want 0", n)
	}
}
