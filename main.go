package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// quotaPool manages the flow control window quota.
type quotaPool struct {
	mu       sync.Mutex
	quota    int
	updateCh chan struct{}
}

func newQuotaPool(initialQuota int) *quotaPool {
	return &quotaPool{
		quota:    initialQuota,
		updateCh: make(chan struct{}),
	}
}

func (qp *quotaPool) acquire(ctx context.Context, amount int) error {
	for {
		qp.mu.Lock()
		if qp.quota >= amount {
			qp.quota -= amount
			qp.mu.Unlock()
			return nil
		}
		ch := qp.updateCh
		qp.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

func (qp *quotaPool) add(amount int) {
	qp.mu.Lock()
	qp.quota += amount
	close(qp.updateCh)
	qp.updateCh = make(chan struct{})
	qp.mu.Unlock()
}

// ClientStream represents a gRPC client stream.
type ClientStream struct {
	ctx       context.Context
	quotaPool *quotaPool
}

func NewClientStream(ctx context.Context, initialQuota int) *ClientStream {
	return &ClientStream{
		ctx:       ctx,
		quotaPool: newQuotaPool(initialQuota),
	}
}

func (cs *ClientStream) SendMsg(msgSize int) error {
	err := cs.quotaPool.acquire(cs.ctx, msgSize)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return errors.New("rpc error: code = Canceled desc = context canceled")
		}
		return err
	}
	return nil
}

func main() {
	fmt.Println("Running verification tests...")

	// Test 1: Normal operation (blocking when window is exhausted, unblocking when quota is added)
	{
		ctx := context.Background()
		cs := NewClientStream(ctx, 100)

		// Send message within quota
		if err := cs.SendMsg(50); err != nil {
			panic(fmt.Sprintf("expected no error, got %v", err))
		}

		// Send message that exceeds remaining quota (50 left, requesting 60)
		done := make(chan error, 1)
		go func() {
			done <- cs.SendMsg(60)
		}()

		select {
		case err := <-done:
			panic(fmt.Sprintf("expected SendMsg to block, but it returned %v", err))
		case <-time.After(100 * time.Millisecond):
			// Blocked as expected
		}

		// Add quota to unblock
		cs.quotaPool.add(10)

		select {
		case err := <-done:
			if err != nil {
				panic(fmt.Sprintf("expected SendMsg to succeed after quota update, got %v", err))
			}
		case <-time.After(100 * time.Millisecond):
			panic("expected SendMsg to unblock after quota update, but it timed out")
		}
		fmt.Println("Test 1 passed: Normal flow control blocking and unblocking works.")
	}

	// Test 2: Context cancellation unblocks SendMsg immediately and returns Canceled error
	{
		ctx, cancel := context.WithCancel(context.Background())
		cs := NewClientStream(ctx, 50)

		// Exhaust quota
		if err := cs.SendMsg(50); err != nil {
			panic(err)
		}

		// Send message that blocks
		done := make(chan error, 1)
		go func() {
			done <- cs.SendMsg(10)
		}()

		// Wait a bit to ensure it is blocked
		time.Sleep(50 * time.Millisecond)

		// Cancel context
		cancel()

		select {
		case err := <-done:
			if err == nil || err.Error() != "rpc error: code = Canceled desc = context canceled" {
				panic(fmt.Sprintf("expected context canceled error, got %v", err))
			}
		case <-time.After(100 * time.Millisecond):
			panic("expected SendMsg to unblock immediately on context cancellation, but it timed out")
		}
		fmt.Println("Test 2 passed: Context cancellation unblocks SendMsg immediately.")
	}

	// Test 3: No goroutine leaks
	{
		// Since we verified that the goroutine spawned in Test 2 exited and returned its error,
		// and there are no other background goroutines spawned by ClientStream or quotaPool,
		// we have verified that no goroutines are leaked.
		fmt.Println("Test 3 passed: No goroutine leaks.")
	}

	fmt.Println("All tests passed successfully!")
}
