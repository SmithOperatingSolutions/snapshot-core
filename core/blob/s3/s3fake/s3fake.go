// Package s3fake is an in-process, S3-compatible HTTP server for tests: the
// subset of the S3 API blob/s3 uses (path-style PUT with If-None-Match and
// If-Match, ranged GET, HEAD, DELETE, ListObjectsV2), in pure Go, so the
// backend's contract suite runs on every `go test` with no Docker. It also
// injects the faults the spec's tests need: an endpoint that ignores
// conditional headers, a response lost after the write landed, and a request
// counter. The same suite runs against a real S3 server (SeaweedFS) in CI, which keeps
// this fake honest.
package s3fake

import (
	"bytes"
	"crypto/md5" //nolint:gosec // G501: S3's ETag for a single PUT is an MD5; this mimics it, it protects nothing
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type object struct {
	data []byte
	etag string
	mod  time.Time
}

// Server is the fake. Its zero value is not usable; call New.
type Server struct {
	mu                sync.Mutex
	buckets           map[string]map[string]*object
	ignoreIfMatch     bool
	ignoreIfNoneMatch bool
	dropNext          int
	maxPage           int
	requests          map[string]int
	lastPut           http.Header
	srv               *httptest.Server
}

// New starts a server.
func New() *Server {
	s := &Server{buckets: map[string]map[string]*object{}, requests: map[string]int{}}
	s.srv = httptest.NewServer(s)
	return s
}

// URL is the endpoint to point a client at.
func (s *Server) URL() string { return s.srv.URL }

// Close stops the server.
func (s *Server) Close() { s.srv.Close() }

// CreateBucket makes an empty bucket.
func (s *Server) CreateBucket(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buckets[name] == nil {
		s.buckets[name] = map[string]*object{}
	}
}

// IgnoreIfMatch makes the server apply PUTs regardless of If-Match, like an
// S3-compatible store without conditional-write support.
func (s *Server) IgnoreIfMatch(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ignoreIfMatch = on
}

// IgnoreIfNoneMatch makes the server apply PUTs regardless of If-None-Match.
func (s *Server) IgnoreIfNoneMatch(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ignoreIfNoneMatch = on
}

// DropNextResponses applies the next n PUTs and then closes the connection
// without answering, as a network that loses the response would.
func (s *Server) DropNextResponses(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropNext = n
}

// MaxPage caps every ListObjectsV2 page at n keys (0: no cap), marking the
// page truncated: real S3 may return fewer keys than asked for.
func (s *Server) MaxPage(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxPage = n
}

// Requests returns how many requests of each kind (PUT, GET, HEAD, DELETE,
// LIST) the server has answered since the last reset.
func (s *Server) Requests() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.requests))
	for k, v := range s.requests {
		out[k] = v
	}
	return out
}

// ResetRequests zeroes the counters.
func (s *Server) ResetRequests() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = map[string]int{}
}

// LastPutHeaders returns the headers of the most recent object PUT.
func (s *Server) LastPutHeaders() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastPut.Clone()
}

// Tamper rewrites a stored object's bytes in place (keeping its ETag), as
// silent corruption or a malicious operator would.
func (s *Server) Tamper(bucket, key string, f func([]byte) []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.buckets[bucket][key]
	if o == nil {
		return false
	}
	o.data = f(bytes.Clone(o.data))
	return true
}

// Keys lists a bucket's keys in order.
func (s *Server) Keys(bucket string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ks []string
	for k := range s.buckets[bucket] {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

type s3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code string) {
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(s3Error{Code: code, Message: code})
}

func etagOf(b []byte) string {
	sum := md5.Sum(b) //nolint:gosec // G401: mimics S3's ETag
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// ServeHTTP implements the S3 subset.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	switch {
	case key == "" && r.Method == http.MethodPut:
		s.CreateBucket(bucket)
		w.WriteHeader(http.StatusOK)
	case key == "" && r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		s.list(w, r, bucket)
	case key != "" && r.Method == http.MethodPut:
		s.put(w, r, bucket, key)
	case key != "" && (r.Method == http.MethodGet || r.Method == http.MethodHead):
		s.get(w, r, bucket, key)
	case key != "" && r.Method == http.MethodDelete:
		s.count("DELETE")
		s.mu.Lock()
		if b := s.buckets[bucket]; b != nil {
			delete(b, key)
		}
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, r, http.StatusNotImplemented, "NotImplemented")
	}
}

func (s *Server) count(kind string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests[kind]++
}

func (s *Server) put(w http.ResponseWriter, r *http.Request, bucket, key string) {
	s.count("PUT")
	if strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		writeError(w, r, http.StatusNotImplemented, "NotImplemented") // configure clients WhenRequired
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "IncompleteBody")
		return
	}
	if cl := r.Header.Get("Content-Length"); cl != "" && cl != strconv.Itoa(len(body)) {
		writeError(w, r, http.StatusBadRequest, "IncompleteBody")
		return
	}
	s.mu.Lock()
	b := s.buckets[bucket]
	if b == nil {
		s.mu.Unlock()
		writeError(w, r, http.StatusNotFound, "NoSuchBucket")
		return
	}
	cur := b[key]
	if inm := r.Header.Get("If-None-Match"); inm == "*" && cur != nil && !s.ignoreIfNoneMatch {
		s.mu.Unlock()
		writeError(w, r, http.StatusPreconditionFailed, "PreconditionFailed")
		return
	}
	if im := r.Header.Get("If-Match"); im != "" && !s.ignoreIfMatch && (cur == nil || cur.etag != im) {
		s.mu.Unlock()
		writeError(w, r, http.StatusPreconditionFailed, "PreconditionFailed")
		return
	}
	o := &object{data: body, etag: etagOf(body), mod: time.Now().UTC()}
	b[key] = o
	s.lastPut = r.Header.Clone()
	drop := s.dropNext > 0
	if drop {
		s.dropNext--
	}
	s.mu.Unlock()
	if drop {
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close() // the write landed; the answer never arrives
				return
			}
		}
	}
	w.Header().Set("ETag", o.etag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if r.Method == http.MethodHead {
		s.count("HEAD")
	} else {
		s.count("GET")
	}
	s.mu.Lock()
	var o *object
	if b := s.buckets[bucket]; b != nil {
		o = b[key]
	}
	var data []byte
	if o != nil {
		data = o.data
	}
	s.mu.Unlock()
	if o == nil {
		writeError(w, r, http.StatusNotFound, "NoSuchKey")
		return
	}
	size := int64(len(data))
	h := w.Header()
	h.Set("ETag", o.etag)
	h.Set("Last-Modified", o.mod.Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	start, end := int64(0), size-1
	status := http.StatusOK
	if rg := r.Header.Get("Range"); rg != "" && r.Method == http.MethodGet {
		spec, ok := strings.CutPrefix(rg, "bytes=")
		a, b, _ := strings.Cut(spec, "-")
		first, err := strconv.ParseInt(a, 10, 64)
		if !ok || err != nil {
			writeError(w, r, http.StatusBadRequest, "InvalidArgument")
			return
		}
		if first >= size {
			h.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
			writeError(w, r, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
			return
		}
		start = first
		if b != "" {
			last, err := strconv.ParseInt(b, 10, 64)
			if err != nil || last < first {
				writeError(w, r, http.StatusBadRequest, "InvalidArgument")
				return
			}
			end = min(last, size-1)
		}
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		status = http.StatusPartialContent
	}
	h.Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(status)
	if r.Method == http.MethodGet {
		_, _ = w.Write(data[start : end+1])
	}
}

type listResult struct {
	XMLName               xml.Name  `xml:"ListBucketResult"`
	Name                  string    `xml:"Name"`
	Prefix                string    `xml:"Prefix"`
	StartAfter            string    `xml:"StartAfter,omitempty"`
	KeyCount              int       `xml:"KeyCount"`
	MaxKeys               int       `xml:"MaxKeys"`
	IsTruncated           bool      `xml:"IsTruncated"`
	NextContinuationToken string    `xml:"NextContinuationToken,omitempty"`
	Contents              []content `xml:"Contents"`
}

type content struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

func (s *Server) list(w http.ResponseWriter, r *http.Request, bucket string) {
	s.count("LIST")
	q := r.URL.Query()
	prefix, after := q.Get("prefix"), q.Get("start-after")
	if tok := q.Get("continuation-token"); tok > after {
		after = tok
	}
	maxKeys := 1000
	if mk := q.Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n >= 0 && n < maxKeys {
			maxKeys = n
		}
	}
	s.mu.Lock()
	if s.maxPage > 0 && s.maxPage < maxKeys {
		maxKeys = s.maxPage
	}
	b := s.buckets[bucket]
	if b == nil {
		s.mu.Unlock()
		writeError(w, r, http.StatusNotFound, "NoSuchBucket")
		return
	}
	var keys []string
	for k := range b {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	res := listResult{Name: bucket, Prefix: prefix, StartAfter: q.Get("start-after"), MaxKeys: maxKeys}
	if len(keys) > maxKeys {
		res.IsTruncated = true
		keys = keys[:maxKeys]
		res.NextContinuationToken = keys[len(keys)-1]
	}
	for _, k := range keys {
		o := b[k]
		res.Contents = append(res.Contents, content{Key: k, LastModified: o.mod.Format("2006-01-02T15:04:05.000Z"),
			ETag: o.etag, Size: int64(len(o.data)), StorageClass: "STANDARD"})
	}
	s.mu.Unlock()
	res.KeyCount = len(res.Contents)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(res)
}
