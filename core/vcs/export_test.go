package vcs

import (
	"context"
	"time"
)

// Backoff is the pause after the attempt'th lost swap in a row, jittered
// by jitter.
func Backoff(attempt int, jitter func(n int64) int64) time.Duration { return backoff(attempt, jitter) }

// SetBackoff replaces how r pauses between lost swaps and draws its jitter.
func SetBackoff(r *Repo, sleep func(ctx context.Context, d time.Duration) error, jitter func(n int64) int64) {
	r.sleep, r.jitter = sleep, jitter
}

// Sleep is the pause a repository takes between lost swaps by default.
func Sleep(ctx context.Context, d time.Duration) error { return sleep(ctx, d) }

// DecodeConflict and MaxModelConflicts reach the conflict record's decoder
// and its limit from the external tests.
var DecodeConflict = decodeConflict

const MaxModelConflicts = maxModelConflicts
