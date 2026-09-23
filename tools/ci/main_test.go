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

// A command's output is what it wrote to stdout. docker run -d on a cold
// runner prints its pull progress on stderr and the container's id on
// stdout, and the next command takes the id: with the progress in front of
// it, docker port answered "page not found".
func TestACommandsOutputIsItsStdout(t *testing.T) {
	out, err := output(context.Background(), nil, "sh", "-c", "echo 'Pulling from minio/minio' >&2; echo 0123abcd")
	if err != nil {
		t.Fatalf("a command that succeeded returned %v", err)
	}
	if out != "0123abcd\n" {
		t.Fatalf("output = %q, want only what the command wrote to stdout, %q", out, "0123abcd\n")
	}
}
