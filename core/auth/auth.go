// Package auth is who is calling and whether they may. Every public call of
// the core takes a Principal and asks an Authorizer, which denies unless told
// otherwise (Storage Core Spec, security requirements: "every public call
// takes a Principal; default-deny Authorizer").
package auth

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"
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
func (p Principal) Validate() error {
	if p.ID == "" || len(p.ID) > MaxIDLen || !utf8.ValidString(p.ID) {
		return fmt.Errorf("%w: id of %d bytes", ErrInvalidPrincipal, len(p.ID))
	}
	for _, r := range p.ID {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: id %q holds a control character", ErrInvalidPrincipal, p.ID)
		}
	}
	return nil
}

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
// refuses it. The resource names what the call acts on: "repo" (the whole
// repository: reading history, GC), "branch:<name>", "tag:<name>", or
// "path:<branch>:<path>", asked for write on every path a write changes. A
// branch name holds no colon, so a policy can match by prefix.
type Authorizer interface {
	Authorize(ctx context.Context, p Principal, a Action, resource string) error
}

// DenyAll refuses everything; it is what a nil Authorizer means.
type DenyAll struct{}

// Authorize implements Authorizer.
func (DenyAll) Authorize(ctx context.Context, p Principal, a Action, resource string) error {
	return fmt.Errorf("%w: no authorizer allows %s", ErrDenied, p.ID)
}

// AllowAll allows everything, for tests and single-user tools that choose it.
type AllowAll struct{}

// Authorize implements Authorizer.
func (AllowAll) Authorize(ctx context.Context, p Principal, a Action, resource string) error {
	return nil
}

// Check validates p, then asks az (DenyAll when nil) about the call.
func Check(ctx context.Context, az Authorizer, p Principal, a Action, resource string) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if az == nil {
		az = DenyAll{}
	}
	if err := az.Authorize(ctx, p, a, resource); err != nil {
		if errors.Is(err, ErrDenied) {
			return err
		}
		return fmt.Errorf("%w: %w", ErrDenied, err)
	}
	return nil
}
