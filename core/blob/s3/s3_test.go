package s3_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/s3"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/s3/s3fake"
)

var ctx = context.Background()

const bucket = "snapshot-core-test"

func fake(t *testing.T) (*s3fake.Server, *awss3.Client) {
	t.Helper()
	srv := s3fake.New()
	t.Cleanup(srv.Close)
	srv.CreateBucket(bucket)
	c := s3.NewClient(srv.URL(), "us-east-1", "test-access", "test-secret")
	if c == nil {
		t.Fatal("NewClient returned nil")
	}
	return srv, c
}

func randomPrefix(t *testing.T) string {
	t.Helper()
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return "t-" + hex.EncodeToString(b) + "/"
}

func open(t *testing.T, c *awss3.Client, prefix string) *s3.Store {
	t.Helper()
	st, err := s3.Open(ctx, s3.Options{Client: c, Bucket: bucket, Prefix: prefix, AllowHTTP: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return st
}

// Storage Core Spec: the blob contract on every backend, with 50 racing root
// swappers for S3.
func TestContractAgainstTheInProcessServer(t *testing.T) {
	_, c := fake(t)
	contract.Run(t, func(t *testing.T) blob.BlobStore { return open(t, c, randomPrefix(t)) },
		contract.Options{Swappers: 50})
}

// Engine Spec: "an endpoint that ignores If-Match is detected by the startup
// probe and refused". The same for If-None-Match, which put-if-absent needs.
func TestProbeRefusesEndpointsThatIgnoreConditionalWrites(t *testing.T) {
	srv, c := fake(t)
	if _, err := s3.Open(ctx, s3.Options{Client: c, Bucket: bucket, Prefix: randomPrefix(t), AllowHTTP: true}); err != nil {
		t.Fatalf("positive control: an honest endpoint was refused: %v", err)
	}
	srv.IgnoreIfMatch(true)
	if _, err := s3.Open(ctx, s3.Options{Client: c, Bucket: bucket, Prefix: randomPrefix(t), AllowHTTP: true}); !errors.Is(err, s3.ErrUnsafeEndpoint) {
		t.Errorf("Open against an endpoint that ignores If-Match = %v, want ErrUnsafeEndpoint: "+
			"concurrent commits would overwrite each other", err)
	}
	srv.IgnoreIfMatch(false)
	srv.IgnoreIfNoneMatch(true)
	if _, err := s3.Open(ctx, s3.Options{Client: c, Bucket: bucket, Prefix: randomPrefix(t), AllowHTTP: true}); !errors.Is(err, s3.ErrUnsafeEndpoint) {
		t.Errorf("Open against an endpoint that ignores If-None-Match = %v, want ErrUnsafeEndpoint", err)
	}
}

func TestPlainHTTPIsRefusedUnlessAllowed(t *testing.T) {
	_, c := fake(t)
	if _, err := s3.Open(ctx, s3.Options{Client: c, Bucket: bucket, Prefix: randomPrefix(t)}); !errors.Is(err, s3.ErrInsecureEndpoint) {
		t.Fatalf("Open of an http:// endpoint without AllowHTTP = %v, want ErrInsecureEndpoint", err)
	}
}

func TestOptionsAreValidated(t *testing.T) {
	_, c := fake(t)
	for name, o := range map[string]s3.Options{
		"no client":           {Bucket: bucket, AllowHTTP: true},
		"no bucket":           {Client: c, AllowHTTP: true},
		"prefix without /":    {Client: c, Bucket: bucket, Prefix: "repo", AllowHTTP: true},
		"absolute prefix":     {Client: c, Bucket: bucket, Prefix: "/repo/", AllowHTTP: true},
		"prefix with ..":      {Client: c, Bucket: bucket, Prefix: "a/../b/", AllowHTTP: true},
		"uppercase in prefix": {Client: c, Bucket: bucket, Prefix: "Repo/", AllowHTTP: true},
	} {
		if _, err := s3.Open(ctx, o); !errors.Is(err, s3.ErrOptions) {
			t.Errorf("%s: Open = %v, want ErrOptions", name, err)
		}
	}
}

// A swap whose response is lost after S3 applied it: the SDK retries, the
// retry meets 412 (the ETag moved, because of our own write), and the store
// must recognize its write instead of reporting a conflict.
func TestSwapRootRecognizesItsOwnWriteAfterALostResponse(t *testing.T) {
	srv, c := fake(t)
	st := open(t, c, randomPrefix(t))
	v1, err := st.SwapRoot(ctx, blob.NoVersion, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	srv.DropNextResponses(1)
	v2, err := st.SwapRoot(ctx, v1, []byte("second"))
	if err != nil {
		t.Fatalf("a swap whose response was lost reported %v: the committer would retry a commit that landed", err)
	}
	r, err := st.Root(ctx)
	if err != nil || string(r.Value) != "second" || r.Version != v2 {
		t.Fatalf("root = %q@%q (%v), want second@%q", r.Value, r.Version, err, v2)
	}
}

// Two stores in one bucket are isolated by prefix.
func TestPrefixesIsolateStores(t *testing.T) {
	srv, c := fake(t)
	a, b := open(t, c, "repo-a/"), open(t, c, "repo-b/")
	if err := a.Put(ctx, "packs/1", strings.NewReader("a"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := a.SwapRoot(ctx, blob.NoVersion, []byte("root a")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Stat(ctx, "packs/1"); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("store b sees store a's object (err=%v)", err)
	}
	if r, err := b.Root(ctx); err != nil || len(r.Value) != 0 {
		t.Errorf("store b sees a root %q (%v)", r.Value, err)
	}
	for _, k := range srv.Keys(bucket) {
		if !strings.HasPrefix(k, "repo-a/") && !strings.HasPrefix(k, "repo-b/") {
			t.Errorf("key %q is outside both stores' prefixes", k)
		}
	}
}

func TestSSEKMSIsRequestedWhenConfigured(t *testing.T) {
	srv, c := fake(t)
	st, err := s3.Open(ctx, s3.Options{Client: c, Bucket: bucket, Prefix: randomPrefix(t), AllowHTTP: true,
		SSEKMSKeyID: "arn:aws:kms:us-east-1:111122223333:key/test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(ctx, "packs/k", bytes.NewReader([]byte("x")), 1); err != nil {
		t.Fatal(err)
	}
	h := srv.LastPutHeaders()
	if h.Get("X-Amz-Server-Side-Encryption") != "aws:kms" ||
		h.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id") != "arn:aws:kms:us-east-1:111122223333:key/test" {
		t.Fatalf("PUT headers %v do not request SSE-KMS with the configured key", h)
	}
}

// The root object's bytes are ours to check: a short or truncated one is
// corrupt, never an empty root.
func TestTruncatedRootObjectIsCorrupt(t *testing.T) {
	srv, c := fake(t)
	prefix := randomPrefix(t)
	st := open(t, c, prefix)
	if _, err := st.SwapRoot(ctx, blob.NoVersion, []byte("manifest")); err != nil {
		t.Fatal(err)
	}
	if !srv.Tamper(bucket, prefix+"root", func(b []byte) []byte { return b[:10] }) {
		t.Fatal("no root object to tamper with")
	}
	if r, err := st.Root(ctx); !errors.Is(err, s3.ErrCorrupt) {
		t.Fatalf("Root of a 10-byte root object = %q, %v; want ErrCorrupt", r.Value, err)
	}
}

// Range reads cost one request: a frame is fetched without downloading the pack.
func TestRangeGetIsOneRequest(t *testing.T) {
	srv, c := fake(t)
	st := open(t, c, randomPrefix(t))
	data := bytes.Repeat([]byte("0123456789"), 1000)
	if err := st.Put(ctx, "packs/r", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	srv.ResetRequests()
	rc, err := st.Get(ctx, "packs/r", 500, 100)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(b, data[500:600]) {
		t.Fatalf("range read returned the wrong bytes")
	}
	if got := srv.Requests(); got["GET"] != 1 || len(got) != 1 {
		t.Fatalf("a range read cost %v requests, want exactly one GET", got)
	}
}

// S3 may answer with a truncated page shorter than asked. The port says a
// short page is the last, so List must keep reading until the page is full
// or the listing really ends.
func TestListFillsPagesAcrossShortServerPages(t *testing.T) {
	srv, c := fake(t)
	st := open(t, c, randomPrefix(t))
	for i := 0; i < 7; i++ {
		n := "packs/" + string(rune('a'+i))
		if err := st.Put(ctx, n, strings.NewReader(n), int64(len(n))); err != nil {
			t.Fatal(err)
		}
	}
	srv.MaxPage(2)
	infos, err := st.List(ctx, "packs/", "", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 5 || infos[0].Name != "packs/a" || infos[4].Name != "packs/e" {
		t.Fatalf("List(limit 5) over 2-key server pages returned %d objects %v: a short page would look like the end", len(infos), infos)
	}
	rest, err := st.List(ctx, "packs/", infos[4].Name, 5)
	if err != nil || len(rest) != 2 {
		t.Fatalf("the final page = %v, %v; want the 2 remaining objects", rest, err)
	}
}
