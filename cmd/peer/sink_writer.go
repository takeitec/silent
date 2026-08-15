package main

import (
	"context"
	"time"
)

type sinkWriteRequest struct {
	payload     []byte
	seq         int64
	source      string
	enqueuedAt  time.Time
	scheduledAt time.Time
	receivedAt  time.Time
	producedAt  time.Time
}

type sinkWriteResult struct {
	request   sinkWriteRequest
	startedAt time.Time
	duration  time.Duration
	err       error
}

type sinkWriter struct {
	queue   chan sinkWriteRequest
	results chan sinkWriteResult
	done    chan struct{}
}

func newSinkWriter(ctx context.Context, capacity int, write func(sinkWriteRequest) sinkWriteResult, closeSink func()) *sinkWriter {
	if capacity < 1 {
		capacity = 1
	}
	writer := &sinkWriter{
		queue:   make(chan sinkWriteRequest, capacity),
		results: make(chan sinkWriteResult, capacity+1),
		done:    make(chan struct{}),
	}
	go func() {
		defer close(writer.done)
		defer close(writer.results)
		if closeSink != nil {
			defer closeSink()
		}
		for {
			select {
			case <-ctx.Done():
				return
			case request, ok := <-writer.queue:
				if !ok {
					return
				}
				result := write(request)
				select {
				case writer.results <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return writer
}

func (w *sinkWriter) enqueue(request sinkWriteRequest) (dropped *sinkWriteRequest, accepted bool) {
	select {
	case <-w.done:
		return nil, false
	default:
	}

	select {
	case w.queue <- request:
		return nil, true
	default:
	}

	select {
	case oldest := <-w.queue:
		dropped = &oldest
	default:
	}

	select {
	case w.queue <- request:
		return dropped, true
	case <-w.done:
		return dropped, false
	}
}
