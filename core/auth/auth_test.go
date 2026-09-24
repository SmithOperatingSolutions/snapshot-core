package auth_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
)

var (
	ctx   = context.Background()
	alice = auth.Principal{ID: "user:alice"}
	all   = []auth.Action{auth.Read, auth.Write, auth.Manage, auth.Admin, auth.Commit, auth.Merge}
)

func TestDenyAllRefusesEveryCall(t *testing.T) {
	for _, a := range all {
		if err := auth.Check(ctx, auth.AllowAll{}, alice, a, "branch:main"); err != nil {
			t.Fatalf("positive control: AllowAll refused action %d: %v", a, err)
		}
		if err := auth.Check(ctx, auth.DenyAll{}, alice, a, "branch:main"); !errors.Is(err, auth.ErrDenied) {
			t.Errorf("DenyAll on action %d = %v, want ErrDenied", a, err)
		}
		if err := auth.Check(ctx, nil, alice, a, "branch:main"); !errors.Is(err, auth.ErrDenied) {
			t.Errorf("a nil Authorizer on action %d = %v, want ErrDenied: no authorizer must mean deny", a, err)
		}
	}
}

// recording remembers the one call it was asked about and answers with err.
type recording struct {
	got struct {
		p        auth.Principal
		a        auth.Action
		resource string
	}
	calls int
	err   error
}

func (r *recording) Authorize(ctx context.Context, p auth.Principal, a auth.Action, resource string) error {
	r.calls++
	r.got.p, r.got.a, r.got.resource = p, a, resource
	return r.err
}

// Check asks about exactly the call made, and its refusal is the refusal.
func TestCheckAsksAboutTheCallAndKeepsTheAnswer(t *testing.T) {
	r := &recording{}
	if err := auth.Check(ctx, r, alice, auth.Manage, "tag:v1"); err != nil {
		t.Fatalf("an authorizer that allows: %v", err)
	}
	if r.calls != 1 || r.got.p != alice || r.got.a != auth.Manage || r.got.resource != "tag:v1" {
		t.Fatalf("the authorizer was asked %d times, last about %+v", r.calls, r.got)
	}
	r.err = errors.New("policy says no")
	if err := auth.Check(ctx, r, alice, auth.Write, "branch:main"); !errors.Is(err, auth.ErrDenied) || !strings.Contains(err.Error(), "policy says no") {
		t.Fatalf("an authorizer's refusal came back as %v; want ErrDenied carrying its reason", err)
	}
}

// A principal whose id could not be recorded as an author is refused
// before any authorizer is asked, even one that allows everything.
func TestPrincipalsAreValidated(t *testing.T) {
	for name, id := range map[string]string{
		"at the limit": strings.Repeat("a", auth.MaxIDLen),
		"UTF-8":        "user:José",
	} {
		if err := auth.Check(ctx, auth.AllowAll{}, auth.Principal{ID: id}, auth.Read, "x"); err != nil {
			t.Errorf("positive control, %s: %v", name, err)
		}
	}
	for name, id := range map[string]string{
		"empty":          "",
		"over the limit": strings.Repeat("a", auth.MaxIDLen+1),
		"invalid UTF-8":  "user:\xff",
		"a newline":      "user:a\nb",
		"a delete":       "user:a\x7fb",
		"a NUL":          "user:a\x00b",
	} {
		r := &recording{}
		if err := auth.Check(ctx, r, auth.Principal{ID: id}, auth.Read, "x"); !errors.Is(err, auth.ErrInvalidPrincipal) {
			t.Errorf("%s: Check = %v, want ErrInvalidPrincipal", name, err)
		}
		if r.calls != 0 {
			t.Errorf("%s: the authorizer was asked about an invalid principal", name)
		}
	}
}
