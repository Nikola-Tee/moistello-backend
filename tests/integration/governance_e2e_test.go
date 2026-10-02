package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/moistello/backend/internal/domain/governance"
)

// TestGovernance_ProposalLifecycleE2E tests the full end-to-end lifecycle
// of a governance proposal: create → vote → quorum check → execute → verify.
func TestGovernance_ProposalLifecycleE2E(t *testing.T) {
	ctx := context.Background()
	svc := governance.NewService(nil)

	creatorID := uuid.New()

	// Step 1: Create proposal
	created, err := svc.CreateProposal(ctx, governance.CreateProposalInput{
		Title:        "Increase circle payout frequency to weekly",
		Description:  "Currently payouts are monthly. Switching to weekly improves liquidity for all members.",
		ProposalType: "parameter",
		CreatorID:    creatorID.String(),
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.Equal(t, governance.ProposalStatusPending, created.Status)
	assert.Equal(t, "Increase circle payout frequency to weekly", created.Title)
	assert.Equal(t, 0, created.ForVotes)
	assert.Equal(t, 0, created.AgainstVotes)
	assert.WithinDuration(t, time.Now(), created.CreatedAt, 5*time.Second)

	// Step 2: Verify proposal is retrievable
	proposal, err := svc.GetProposal(ctx, created.ID.String())
	require.NoError(t, err)
	assert.Equal(t, created.ID, proposal.ID)
	assert.Equal(t, governance.ProposalStatusPending, proposal.Status)

	// Step 3: Multiple voters vote
	voters := make([]uuid.UUID, 5)
	for i := range voters {
		voters[i] = uuid.New()
	}

	// 3 votes for, 2 against
	for i := 0; i < 3; i++ {
		err = svc.VoteProposal(ctx, created.ID.String(), voters[i].String(), true)
		require.NoError(t, err, "vote-for %d should succeed", i)
	}
	for i := 3; i < 5; i++ {
		err = svc.VoteProposal(ctx, created.ID.String(), voters[i].String(), false)
		require.NoError(t, err, "vote-against %d should succeed", i)
	}

	// Verify vote tallies
	proposal, err = svc.GetProposal(ctx, created.ID.String())
	require.NoError(t, err)
	assert.Equal(t, 3, proposal.ForVotes)
	assert.Equal(t, 2, proposal.AgainstVotes)
	assert.Equal(t, governance.ProposalStatusPending, proposal.Status)

	// Step 4: Verify duplicate vote prevention
	err = svc.VoteProposal(ctx, created.ID.String(), voters[0].String(), true)
	assert.Error(t, err)
	assert.Equal(t, governance.ErrAlreadyVoted, err)

	// Step 5: Creator also votes
	err = svc.VoteProposal(ctx, created.ID.String(), creatorID.String(), true)
	require.NoError(t, err)

	proposal, err = svc.GetProposal(ctx, created.ID.String())
	require.NoError(t, err)
	assert.Equal(t, 4, proposal.ForVotes)
	assert.Equal(t, 2, proposal.AgainstVotes)

	// Step 6: Execute proposal
	err = svc.ExecuteProposal(ctx, created.ID.String())
	require.NoError(t, err)

	// Step 7: Verify final state
	executed, err := svc.GetProposal(ctx, created.ID.String())
	require.NoError(t, err)
	assert.Equal(t, governance.ProposalStatusExecuted, executed.Status)
	assert.NotNil(t, executed.ExecutedAt)
	assert.WithinDuration(t, time.Now(), *executed.ExecutedAt, 5*time.Second)

	// Step 8: Verify re-execution is rejected
	err = svc.ExecuteProposal(ctx, created.ID.String())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already been processed")

	// Step 9: Verify it appears in the list
	proposals, total, err := svc.ListProposals(ctx, 1, 100)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, total, 1)
	found := false
	for _, p := range proposals {
		if p.ID == created.ID {
			found = true
			assert.Equal(t, governance.ProposalStatusExecuted, p.Status)
			break
		}
	}
	assert.True(t, found, "executed proposal should appear in list")
}

// TestGovernance_ConcurrentVoting tests that concurrent votes on the same
// proposal don't cause race conditions or lost updates.
func TestGovernance_ConcurrentVoting(t *testing.T) {
	ctx := context.Background()
	svc := governance.NewService(nil)

	creatorID := uuid.New()
	created, err := svc.CreateProposal(ctx, governance.CreateProposalInput{
		Title:        "Concurrent vote test",
		Description:  "Testing race conditions",
		ProposalType: "parameter",
		CreatorID:    creatorID.String(),
	})
	require.NoError(t, err)

	// Launch 10 concurrent voters
	voters := make([]uuid.UUID, 10)
	for i := range voters {
		voters[i] = uuid.New()
	}

	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			done <- svc.VoteProposal(ctx, created.ID.String(), voters[idx].String(), idx%2 == 0)
		}(i)
	}

	// Collect results
	successes := 0
	for i := 0; i < 10; i++ {
		if err := <-done; err == nil {
			successes++
		}
	}

	// All 10 votes should succeed (different voters)
	assert.Equal(t, 10, successes, "all concurrent votes from different voters should succeed")

	// Verify final tally
	proposal, err := svc.GetProposal(ctx, created.ID.String())
	require.NoError(t, err)
	assert.Equal(t, 5, proposal.ForVotes)
	assert.Equal(t, 5, proposal.AgainstVotes)
}

// TestGovernance_ProposalNotFound verifies proper error handling for
// operations on non-existent proposals.
func TestGovernance_ProposalNotFoundE2E(t *testing.T) {
	ctx := context.Background()
	svc := governance.NewService(nil)

	fakeID := uuid.New().String()

	_, err := svc.GetProposal(ctx, fakeID)
	assert.ErrorIs(t, err, governance.ErrProposalNotFound)

	err = svc.VoteProposal(ctx, fakeID, uuid.New().String(), true)
	assert.ErrorIs(t, err, governance.ErrProposalNotFound)

	err = svc.ExecuteProposal(ctx, fakeID)
	assert.ErrorIs(t, err, governance.ErrProposalNotFound)
}
