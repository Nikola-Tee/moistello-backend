package indexer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeduplicator_HasAndAdd(t *testing.T) {
	d := NewDeduplicator(5 * time.Minute)

	assert.False(t, d.Has("hash1"))
	d.Add("hash1")
	assert.True(t, d.Has("hash1"))
	assert.False(t, d.Has("hash2"))
	assert.Equal(t, 1, d.Size())
}

func TestDeduplicator_Prune(t *testing.T) {
	d := NewDeduplicator(1 * time.Millisecond)
	d.Add("hash1")
	time.Sleep(5 * time.Millisecond)
	d.Prune()
	assert.Equal(t, 0, d.Size())
}

func TestDeduplicator_Concurrent(t *testing.T) {
	d := NewDeduplicator(1 * time.Minute)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) { defer wg.Done(); d.Add("hash"); _ = d.Has("hash") }(i)
	}
	wg.Wait()
	assert.Equal(t, 1, d.Size())
}

func TestDeduplicator_StartPruning(t *testing.T) {
	d := NewDeduplicator(1 * time.Millisecond)
	d.Add("hash1")
	ctx, cancel := context.WithCancel(context.Background())
	go d.StartPruning(ctx, 2*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	cancel()
	assert.Equal(t, 0, d.Size())
}

func TestPoller_Create(t *testing.T) {
	p := NewPoller("https://horizon-testnet.stellar.org", []string{"GAX23..."})
	assert.NotNil(t, p)
	assert.Equal(t, "https://horizon-testnet.stellar.org", p.HorizonURL())
	assert.Len(t, p.ContractIDs(), 1)
}

func TestPoller_FilterByContract_NoFilter(t *testing.T) {
	p := NewPoller("http://x", nil)
	txns := []Transaction{{Hash: "a"}, {Hash: "b"}}
	result := p.FilterByContract(txns)
	assert.Len(t, result, 2)
}

func TestPoller_FilterByContract_SourceAccountMatch(t *testing.T) {
	p := NewPoller("http://x", []string{"GMASTER123"})
	txns := []Transaction{
		{Hash: "a", Operations: []Operation{{Type: "payment", SourceAccount: "GMASTER123"}}},
		{Hash: "b", Operations: []Operation{{Type: "payment", SourceAccount: "GOTHER456"}}},
	}

	result := p.FilterByContract(txns)

	assert.Len(t, result, 1)
	assert.Equal(t, "a", result[0].Hash)
}

func TestPoller_FilterByContract_ContractIDMatch(t *testing.T) {
	p := NewPoller("http://x", []string{"CONTRACT_ID_123"})
	txns := []Transaction{
		{Hash: "match", Operations: []Operation{{Type: "invoke_host_function", ContractID: "CONTRACT_ID_123"}}},
		{Hash: "skip", Operations: []Operation{{Type: "invoke_host_function", ContractID: "SOME_OTHER_CONTRACT"}}},
	}

	result := p.FilterByContract(txns)

	assert.Len(t, result, 1)
	assert.Equal(t, "match", result[0].Hash)
}

// The previous bug matched ANY invoke_host_function regardless of contract.
// Ensure an unrelated contract invocation is no longer over-included.
func TestPoller_FilterByContract_IgnoresUnrelatedInvokeHostFunction(t *testing.T) {
	p := NewPoller("http://x", []string{"CONTRACT_ID_123"})
	txns := []Transaction{
		{Hash: "unrelated", Operations: []Operation{{Type: "invoke_host_function", ContractID: "UNRELATED_CONTRACT"}}},
		{Hash: "unrelated2", Operations: []Operation{{Type: "invoke_host_function"}}},
	}

	result := p.FilterByContract(txns)

	assert.Len(t, result, 0)
}

func TestPoller_FilterByContract_MultipleContracts(t *testing.T) {
	p := NewPoller("http://x", []string{"CONTRACT_A", "CONTRACT_B"})
	txns := []Transaction{
		{Hash: "a", Operations: []Operation{{Type: "invoke_host_function", ContractID: "CONTRACT_A"}}},
		{Hash: "b", Operations: []Operation{{Type: "invoke_host_function", ContractID: "CONTRACT_B"}}},
		{Hash: "c", Operations: []Operation{{Type: "invoke_host_function", ContractID: "CONTRACT_C"}}},
	}

	result := p.FilterByContract(txns)

	assert.Len(t, result, 2)
}

func TestPoller_FilterByContract_NoOperations(t *testing.T) {
	p := NewPoller("http://x", []string{"CONTRACT_A"})
	txns := []Transaction{{Hash: "a", Operations: []Operation{}}}

	result := p.FilterByContract(txns)

	assert.Len(t, result, 0)
}

func TestIndexerMetrics_Creation(t *testing.T) {
	m := NewIndexerMetrics()
	assert.NotNil(t, m.EventsProcessed)
	assert.NotNil(t, m.PollErrors)
	assert.NotNil(t, m.ProcessErrors)
	assert.NotNil(t, m.LastLedger)
	assert.NotNil(t, m.ReconcilerRuns)
	assert.NotNil(t, m.DedupSize)
	assert.NotNil(t, m.CursorLagSeconds)
	assert.NotNil(t, m.Events)
	assert.NotNil(t, m.Events.Received)
	assert.NotNil(t, m.Events.Decoded)
	assert.NotNil(t, m.Events.Failed)
	assert.NotNil(t, m.Events.DLQ)
	assert.NotNil(t, m.Events.DecodeSkipped)
}

func TestReconciler_StartStop(t *testing.T) {
	r := &Reconciler{
		interval: 1 * time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.StartReconciliation(ctx, 1*time.Hour)
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// success: stopped cleanly on context cancellation
	case <-time.After(500 * time.Millisecond):
		t.Fatal("reconciler did not stop on context cancellation")
	}
}
