package main

import (
	"context"
	"strings"
	"testing"
)

// A command that fails says why: its error carries what it printed, so a CI
// log reads "pull access denied for minio/minio", not "exit status 125".
func TestAFailedCommandSaysWhy(t *testing.T) {
	ctx := context.Background()
	out, err := output(ctx, nil, "sh", "-c", "echo ok")
	if err != nil || out != "ok\n" {
		t.Fatalf("a command that succeeded returned %q, %v; want its output and no error", out, err)
	}
	_, err = output(ctx, nil, "sh", "-c", "echo 'pull access denied for minio/minio' >&2; exit 125")
	if err == nil {
		t.Fatal("a command that exited 125 was reported as a success")
	}
	if !strings.Contains(err.Error(), "pull access denied for minio/minio") {
		t.Fatalf("a failed command's error hides what it printed: %q", err)
	}
}
