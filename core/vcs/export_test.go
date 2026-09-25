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
