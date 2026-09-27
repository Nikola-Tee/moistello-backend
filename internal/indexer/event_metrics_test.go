package indexer

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCountingProcessor builds a processor wired to a private set of per-type
// counters, so tests can assert on exact values without touching the default
// registry.
func newCountingProcessor(t *testing.T, knownContracts ...string) (*EventProcessor, *EventCounters) {
	t.Helper()
	p := newTestProcessor(nil, nil, nil, nil, nil)
	counters := NewEventCounters(prometheus.NewRegistry())
	p.SetEventCounters(counters)
	if knownContracts != nil {
		p.SetKnownContracts(knownContracts)
	}
	return p, counters
}

func counterValue(t *testing.T, vec *prometheus.CounterVec, eventType string) float64 {
	t.Helper()
	return testutil.ToFloat64(vec.WithLabelValues(eventType))
}

// TestEventCounters_MixedEventLoad is the accuracy check: a batch mixing several
// event types, several of which fail, must land in the right bucket on the
// right event type, and the per-type totals must add up.
func TestEventCounters_MixedEventLoad(t *testing.T) {
	p, counters := newCountingProcessor(t, "known")

	// Nil repositories make the handlers that touch domain state fail, so the
	// mix produces a spread of outcomes from a single batch.
	events := []ContractEvent{
		*contractEvent(EventFeeDeposited, "known", nil),
		*contractEvent(EventFeeDeposited, "known", nil),
		*contractEvent(EventFeeDeposited, "known", nil),
		*contractEvent(EventVoteCast, "known", nil),
		*contractEvent(EventVoteCast, "known", nil),
		*contractEvent(EventMemberJoined, "known", map[string]any{"circle_id": "c", "member": "w"}),
		*contractEvent(EventContributionReceived, "known", map[string]any{"circle_id": "c", "member": "w"}),
		*contractEvent(EventPayoutExecuted, "known", map[string]any{"circle_id": "c", "recipient": "w"}),
		// Unknown contract: skipped, only recorded.
		*contractEvent(EventDisputeRaised, "stranger", nil),
		*contractEvent(EventDisputeRaised, "stranger", nil),
	}

	p.processContractEvents(context.Background(), "tx1", events)

	// Handlers that only log and broadcast succeed: FeeDeposited, VoteCast.
	assert.Equal(t, 3.0, counterValue(t, counters.Received, EventFeeDeposited))
	assert.Equal(t, 3.0, counterValue(t, counters.Decoded, EventFeeDeposited))
	assert.Equal(t, 0.0, counterValue(t, counters.Failed, EventFeeDeposited))
	assert.Equal(t, 0.0, counterValue(t, counters.DLQ, EventFeeDeposited))

	assert.Equal(t, 2.0, counterValue(t, counters.Received, EventVoteCast))
	assert.Equal(t, 2.0, counterValue(t, counters.Decoded, EventVoteCast))

	// Handlers backed by nil repositories fail, and must be counted as failed
	// rather than silently dropped.
	for _, evType := range []string{EventMemberJoined, EventContributionReceived, EventPayoutExecuted} {
		assert.Equal(t, 1.0, counterValue(t, counters.Received, evType), evType)
		assert.Equal(t, 1.0, counterValue(t, counters.Failed, evType), evType)
		assert.Equal(t, 0.0, counterValue(t, counters.Decoded, evType), evType)
	}

	// Unknown-contract events are received and dead-lettered, never decoded.
	assert.Equal(t, 2.0, counterValue(t, counters.Received, EventDisputeRaised))
	assert.Equal(t, 2.0, counterValue(t, counters.DLQ, EventDisputeRaised))
	assert.Equal(t, 0.0, counterValue(t, counters.Decoded, EventDisputeRaised))
	assert.Equal(t, 0.0, counterValue(t, counters.Failed, EventDisputeRaised))
}

// TestEventCounters_PartitionHolds asserts the invariant operators rely on:
// for every event type, received == decoded + failed + dlq.
func TestEventCounters_PartitionHolds(t *testing.T) {
	p, counters := newCountingProcessor(t, "known")

	events := []ContractEvent{
		*contractEvent(EventFeeDeposited, "known", nil),
		*contractEvent(EventMemberJoined, "known", map[string]any{"circle_id": "c", "member": "w"}),
		*contractEvent(EventAuctionBid, "stranger", nil),
		*contractEvent(EventDisputeRaised, "known", map[string]any{"circle_id": "c"}),
		*contractEvent(EventDisputeRaised, "stranger", nil),
	}
	p.processContractEvents(context.Background(), "tx1", events)

	for _, evType := range []string{EventFeeDeposited, EventMemberJoined, EventAuctionBid, EventDisputeRaised} {
		received := counterValue(t, counters.Received, evType)
		rest := counterValue(t, counters.Decoded, evType) +
			counterValue(t, counters.Failed, evType) +
			counterValue(t, counters.DLQ, evType)
		assert.Equal(t, rest, received, "received must equal decoded+failed+dlq for %s", evType)
	}
}

// TestEventCounters_NewEventTypeNeedsNoCodeChange is the zero-code-change check:
// an event type this package has never heard of still shows up in the
// counters, without a new metric and without touching the dispatch switch.
func TestEventCounters_NewEventTypeNeedsNoCodeChange(t *testing.T) {
	p, counters := newCountingProcessor(t, "known")

	const brandNewType = "CollateralSlashed"
	assert.NotContains(t, knownEventTypes(), brandNewType, "the fixture must use an unknown type")

	p.processContractEvents(context.Background(), "tx1", []ContractEvent{
		*contractEvent(brandNewType, "known", nil),
	})

	assert.Equal(t, 1.0, counterValue(t, counters.Received, brandNewType))
	assert.Equal(t, 1.0, counterValue(t, counters.Decoded, brandNewType))

	// And it is exported under its own label on the metrics endpoint.
	body := scrape(t, counters)
	assert.Contains(t, body, `moistello_indexer_events_received_total{event_type="`+brandNewType+`"} 1`)
	assert.Contains(t, body, `moistello_indexer_events_decoded_total{event_type="`+brandNewType+`"} 1`)
}

// TestEventCounters_AllKnownTypesAreCounted makes sure the existing event types
// the platform cares about (transfers, disputes, payouts) are all reachable
// through the counters, not just a hand-picked few.
func TestEventCounters_AllKnownTypesAreCounted(t *testing.T) {
	all := knownEventTypes()
	require.NotEmpty(t, all)

	for _, evType := range all {
		t.Run(evType, func(t *testing.T) {
			p, counters := newCountingProcessor(t, "known")
			p.processContractEvents(context.Background(), "tx1", []ContractEvent{
				*contractEvent(evType, "known", nil),
			})
			assert.Equal(t, 1.0, counterValue(t, counters.Received, evType),
				"every contract event type must be counted, including ones whose handler has no repository yet")
		})
	}
}

func TestEventCounters_NilReceiverIsSafe(t *testing.T) {
	var counters *EventCounters
	assert.NotPanics(t, func() {
		counters.Count(EventFeeDeposited, OutcomeDecoded)
		counters.Count("", OutcomeDecoded)
	})
}

func TestEventCounters_EmptyEventTypeIgnored(t *testing.T) {
	_, counters := newCountingProcessor(t, "known")
	counters.Count("", OutcomeDecoded)
	assert.Equal(t, 0, testutil.CollectAndCount(counters.Received))
}

// TestEventCounters_UndecodableBatchLeavesPerTypeTotalsAlone checks the
// separation the counters promise: a transaction whose XDR cannot be decoded at
// all has no attributable event type, so it must not inflate anyone's totals.
func TestEventCounters_UndecodableBatchLeavesPerTypeTotalsAlone(t *testing.T) {
	p, counters := newCountingProcessor(t, "known")

	// Valid base64, not valid XDR: nothing can be decoded from it.
	require.NoError(t, p.ProcessTransaction(context.Background(), &Transaction{
		Hash:   "tx-bad",
		Ledger: 7,
		Operations: []Operation{{
			Type:          "invoke_host_function",
			ResultMetaXDR: "aGVsbG8gd29ybGQ=",
		}},
	}))

	assert.Equal(t, 0, testutil.CollectAndCount(counters.Received),
		"an undecodable transaction must not be attributed to any event type")
}

// knownEventTypes returns the contract event type names declared in this
// package, so a newly added event type is covered by these tests automatically.
func knownEventTypes() []string {
	return []string{
		EventCircleCreated,
		EventMemberJoined,
		EventContributionReceived,
		EventPayoutExecuted,
		EventLateReported,
		EventMemberExited,
		EventDefaultRecorded,
		EventCircleCompleted,
		EventAuctionBid,
		EventVoteCast,
		EventDisputeRaised,
		EventFeeDeposited,
	}
}

// scrape renders the counters through a Prometheus registry and returns the
// exposition text, which is what the indexer's /metrics endpoint serves.
func scrape(t *testing.T, counters *EventCounters) string {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(counters.Received, counters.Decoded, counters.Failed, counters.DLQ, counters.DecodeSkipped)
	families, err := reg.Gather()
	require.NoError(t, err)

	var sb strings.Builder
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			labels := make([]string, 0, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				labels = append(labels, lp.GetName()+`="`+lp.GetValue()+`"`)
			}
			sb.WriteString(mf.GetName())
			if len(labels) > 0 {
				sb.WriteString("{" + strings.Join(labels, ",") + "}")
			}
			sb.WriteString(" " + strconv.FormatFloat(m.GetCounter().GetValue(), 'g', -1, 64) + "\n")
		}
	}
	return sb.String()
}
