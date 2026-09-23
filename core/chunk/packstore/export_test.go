package packstore

import "time"

// WithBackoff shortens the retry backoff for tests.
func WithBackoff(o Options, d time.Duration) Options {
	o.backoff = d
	return o
}
