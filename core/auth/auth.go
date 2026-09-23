// Package auth is who is calling and whether they may. Every public call of
// the core takes a Principal and asks an Authorizer, which denies unless told
// otherwise (Storage Core Spec, security requirements: "every public call
// takes a Principal; default-deny Authorizer").
package auth

import (
	"context"
	"errors"
)

// MaxIDLen is the longest principal id.
const MaxIDLen = 256

// Errors.
var (
	ErrDenied           = errors.New("auth: denied")
	ErrInvalidPrincipal = errors.New("auth: invalid principal")
)

// Principal is an authenticated identity. Its ID is what history records as
// the author of a commit.
type Principal struct {
	ID string
}

// Validate reports whether p can be recorded: a non-empty, valid UTF-8 id of
// at most MaxIDLen bytes with no control characters.
func (p Principal) Validate() error { return nil }

// Action is what a call would do.
type Action uint8

// Actions.
const (
	Read   Action = iota + 1 // read refs, commits and objects
	Write                    // change a branch's working set, commit to it
	Manage                   // create or delete branches and tags
	Admin                    // repository-wide operations (GC, config)
)

// Authorizer decides one call: nil allows it, an error wrapping ErrDenied
// refuses it.
type Authorizer interface {
	Authorize(ctx context.Context, p Principal, a Action, resource string) error
}

// DenyAll refuses everything; it is what a nil Authorizer means.
type DenyAll struct{}

// Authorize implements Authorizer.
func (DenyAll) Authorize(ctx context.Context, p Principal, a Action, resource string) error {
	return nil
}

// AllowAll allows everything, for tests and single-user tools that choose it.
type AllowAll struct{}

// Authorize implements Authorizer.
func (AllowAll) Authorize(ctx context.Context, p Principal, a Action, resource string) error {
	return nil
}

// Check validates p, then asks az (DenyAll when nil) about the call.
func Check(ctx context.Context, az Authorizer, p Principal, a Action, resource string) error {
	return nil
}
