package indexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseContractEvents_EmptyXDR(t *testing.T) {
	events, skipped, err := ParseContractEvents("hash1", 100, "")
	assert.NoError(t, err)
	assert.Empty(t, events)
	assert.Zero(t, skipped)
}

func TestParseContractEvents_InvalidBase64(t *testing.T) {
	events, skipped, err := ParseContractEvents("hash1", 100, "!!!not-valid-base64!!!")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "base64 decode")
	assert.Nil(t, events)
	assert.Zero(t, skipped)
}

func TestParseContractEvents_InvalidXDR(t *testing.T) {
	// Valid base64 but not valid XDR.
	events, skipped, err := ParseContractEvents("hash1", 100, "aGVsbG8gd29ybGQ=")
	assert.Error(t, err)
	assert.Nil(t, events)
	assert.Zero(t, skipped)
}

func TestScValToGo_Void(t *testing.T) {
	// Full round-trip XDR tests require Soroban testnet data and are covered
	// in integration tests. This file covers helper utilities only.
	t.Log("XDR round-trip tests require live testnet data — see xdr_parser integration tests")
}

func TestPayloadStr(t *testing.T) {
	p := map[string]any{"key": "value", "num": 42}
	assert.Equal(t, "value", payloadStr(p, "key"))
	assert.Equal(t, "", payloadStr(p, "missing"))
	assert.Equal(t, "", payloadStr(nil, "key"))
	// Non-string value should return empty string, not panic.
	assert.Equal(t, "", payloadStr(p, "num"))
}

func TestPayloadFloat(t *testing.T) {
	p := map[string]any{
		"f64": float64(3.14),
		"f32": float32(2.5),
		"i64": int64(100),
		"u64": uint64(200),
		"i":   int(50),
	}
	assert.InDelta(t, 3.14, payloadFloat(p, "f64"), 0.001)
	assert.InDelta(t, 2.5, payloadFloat(p, "f32"), 0.001)
	assert.Equal(t, float64(100), payloadFloat(p, "i64"))
	assert.Equal(t, float64(200), payloadFloat(p, "u64"))
	assert.Equal(t, float64(50), payloadFloat(p, "i"))
	assert.Equal(t, float64(0), payloadFloat(p, "missing"))
	assert.Equal(t, float64(0), payloadFloat(nil, "key"))
}

func TestPayloadInt(t *testing.T) {
	p := map[string]any{
		"i":   int(10),
		"i32": int32(20),
		"i64": int64(30),
		"u32": uint32(40),
		"f64": float64(50),
	}
	assert.Equal(t, 10, payloadInt(p, "i"))
	assert.Equal(t, 20, payloadInt(p, "i32"))
	assert.Equal(t, 30, payloadInt(p, "i64"))
	assert.Equal(t, 40, payloadInt(p, "u32"))
	assert.Equal(t, 50, payloadInt(p, "f64"))
	assert.Equal(t, 0, payloadInt(p, "missing"))
	assert.Equal(t, 0, payloadInt(nil, "key"))
}

func TestIsNotFound(t *testing.T) {
	assert.False(t, isNotFound(nil))
	assert.True(t, isNotFound(errNotFound("not found")))
}

func TestReputationLevel(t *testing.T) {
	assert.Equal(t, "Diamond", reputationLevel(850))
	assert.Equal(t, "Platinum", reputationLevel(650))
	assert.Equal(t, "Gold", reputationLevel(450))
	assert.Equal(t, "Silver", reputationLevel(250))
	assert.Equal(t, "Bronze", reputationLevel(100))
	assert.Equal(t, "Bronze", reputationLevel(0))
}

// errNotFound is a helper for creating simple not-found error values in tests.
type errNotFound string

func (e errNotFound) Error() string { return string(e) }
