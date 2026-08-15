package main

import (
	"context"
	"testing"
	"time"
)

func TestSinkWriterDropsOldestQueuedWriteWithoutBlocking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan int64, 3)
	release := make(chan struct{})
	writer := newSinkWriter(ctx, 2, func(request sinkWriteRequest) sinkWriteResult {
		started <- request.seq
		<-release
		return sinkWriteResult{request: request}
	}, nil)

	if _, accepted := writer.enqueue(sinkWriteRequest{seq: 1}); !accepted {
		t.Fatal("expected first write to be accepted")
	}
	select {
	case seq := <-started:
		if seq != 1 {
			t.Fatalf("expected first write to start, got seq=%d", seq)
		}
	case <-time.After(time.Second):
		t.Fatal("writer did not start first write")
	}

	writer.enqueue(sinkWriteRequest{seq: 2})
	writer.enqueue(sinkWriteRequest{seq: 3})
	dropped, accepted := writer.enqueue(sinkWriteRequest{seq: 4})
	if !accepted {
		t.Fatal("expected newest write to be accepted")
	}
	if dropped == nil || dropped.seq != 2 {
		t.Fatalf("expected oldest queued write seq=2 to be dropped, got %+v", dropped)
	}

	release <- struct{}{}
	if seq := <-started; seq != 3 {
		t.Fatalf("expected seq=3 after blocked write, got %d", seq)
	}
	release <- struct{}{}
	if seq := <-started; seq != 4 {
		t.Fatalf("expected newest seq=4 to be preserved, got %d", seq)
	}
	release <- struct{}{}
}

func TestSinkWriterStopsAcceptingAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	writer := newSinkWriter(ctx, 1, func(request sinkWriteRequest) sinkWriteResult {
		return sinkWriteResult{request: request}
	}, nil)
	cancel()

	select {
	case <-writer.done:
	case <-time.After(time.Second):
		t.Fatal("writer did not stop after cancellation")
	}

	if _, accepted := writer.enqueue(sinkWriteRequest{seq: 1}); accepted {
		t.Fatal("expected canceled writer to reject writes")
	}
}
