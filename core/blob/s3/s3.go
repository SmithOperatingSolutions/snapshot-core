// Package s3 is the S3 (and S3-compatible) backend (Storage Core Spec
// "core/blob/s3"; Engine Spec "s3store" rules). An object named n is the key
// <prefix>objects/n! and the root is <prefix>root. The "!" suffix means no key
// is ever both an object and the path prefix of another (MinIO serves "a/b/c"
// beside an object "a/b" but leaves it out of every listing, which is how a
// garbage collector loses data); "!" sorts below every name byte, "/"
// included, so key order is exactly name order. Put-if-absent is a PUT with
// If-None-Match: *; the root swap is a PUT with If-Match on the ETag read
// earlier. The root object carries a random 16-byte nonce ahead of its value,
// so every swap has a fresh ETag even when the value repeats (no ABA), and a
// swap whose response was lost can recognize its own write. At Open a probe
// refuses any endpoint that ignores If-None-Match or If-Match: without them
// the store cannot be safe. Opened objects only (Options.ObjectsOnly, for
// blob/split, DESIGN §4), the store holds objects and no root on such an
// endpoint: a put is a HEAD and then an unconditional PUT, and Root and
// SwapRoot are blob.ErrNoRoot. Either way it keeps the root's copy a split
// store writes at <prefix>mirror, replaced in place.
package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

// Errors.
var (
	ErrUnsafeEndpoint   = errors.New("s3: endpoint does not honor conditional writes")
	ErrInsecureEndpoint = errors.New("s3: endpoint is plain HTTP")
	ErrOptions          = errors.New("s3: invalid options")
	ErrCorrupt          = errors.New("s3: root object is corrupt")
)

// Options configures a store.
type Options struct {
	// Client is built by the host, with credentials from the instance or
	// workload role (never a config file). NewClient builds one for tests
	// and MinIO.
	Client *awss3.Client
	Bucket string
	// Prefix scopes the store within the bucket: "" or a valid object name
	// followed by "/". The IAM policy should be scoped to it.
	Prefix string
	// AllowHTTP permits a plain-HTTP endpoint (the in-process test server,
	// MinIO in CI). Production endpoints are HTTPS.
	AllowHTTP bool
	// SSEKMSKeyID, when set, asks S3 to encrypt every object at rest with
	// that KMS key as well (defense in depth; the data is already sealed).
	SSEKMSKeyID string
	// ObjectsOnly runs on a provider that ignores conditional writes: the
	// store holds objects and no root (blob.ErrNoRoot), for blob/split.
	ObjectsOnly bool
}

// NewClient builds a path-style client for an endpoint with static
// credentials, for tests and MinIO. Checksums are computed only when S3
// requires them: SigV4's signed payload hash already protects every PUT.
func NewClient(endpoint, region, accessKey, secretKey string) *awss3.Client {
	return awss3.New(awss3.Options{
		Region:                     region,
		BaseEndpoint:               aws.String(endpoint),
		UsePathStyle:               true,
		Credentials:                credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
}

const (
	nonceSize = 16
	// conflictRetries bounds retries of 409 ConditionalRequestConflict, which
	// S3 returns while another conditional write to the key is in flight.
	conflictRetries = 5
)

// Store is an S3 BlobStore.
type Store struct {
	c           *awss3.Client
	bucket      string
	prefix      string
	sseKMS      string
	objectsOnly bool
}

var _ blob.BlobStore = (*Store)(nil)

// Open validates the options and probes the endpoint.
func Open(ctx context.Context, o Options) (*Store, error) {
	switch {
	case o.Client == nil:
		return nil, fmt.Errorf("%w: no client", ErrOptions)
	case o.Bucket == "":
		return nil, fmt.Errorf("%w: no bucket", ErrOptions)
	case o.Prefix != "" && (!strings.HasSuffix(o.Prefix, "/") || blob.ValidName(strings.TrimSuffix(o.Prefix, "/")) != nil):
		return nil, fmt.Errorf("%w: prefix %q must be empty or a valid name followed by '/'", ErrOptions, o.Prefix)
	}
	if ep := o.Client.Options().BaseEndpoint; ep != nil && strings.HasPrefix(strings.ToLower(*ep), "http://") && !o.AllowHTTP {
		return nil, fmt.Errorf("%w: %s (set AllowHTTP only for test servers)", ErrInsecureEndpoint, *ep)
	}
	s := &Store{c: o.Client, bucket: o.Bucket, prefix: o.Prefix, sseKMS: o.SSEKMSKeyID, objectsOnly: o.ObjectsOnly}
	var err error
	if s.objectsOnly {
		err = s.probeObjects(ctx)
	} else {
		err = s.probe(ctx)
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}

// keySuffix ends every object key; see the package comment.
const keySuffix = "!"

func (s *Store) objectBase() string           { return s.prefix + "objects/" }
func (s *Store) objectKey(name string) string { return s.objectBase() + name + keySuffix }
func (s *Store) rootKey() string              { return s.prefix + "root" }
func (s *Store) mirrorKey() string            { return s.prefix + "mirror" }

func statusOf(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	var nf *types.NotFound
	return errors.As(err, &nsk) || errors.As(err, &nf) || statusOf(err) == http.StatusNotFound
}

func random(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, err
}

// put is a PUT of body under key with the given precondition.
func (s *Store) put(ctx context.Context, key string, body io.ReadSeeker, size int64, ifNoneMatch, ifMatch string) (string, error) {
	in := &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(size),
	}
	if ifNoneMatch != "" {
		in.IfNoneMatch = aws.String(ifNoneMatch)
	}
	if ifMatch != "" {
		in.IfMatch = aws.String(ifMatch)
	}
	if s.sseKMS != "" {
		in.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		in.SSEKMSKeyId = aws.String(s.sseKMS)
	}
	for attempt := 0; ; attempt++ {
		if _, err := body.Seek(0, io.SeekStart); err != nil {
			return "", err
		}
		out, err := s.c.PutObject(ctx, in)
		if err == nil {
			return aws.ToString(out.ETag), nil
		}
		if statusOf(err) == http.StatusConflict && attempt < conflictRetries {
			time.Sleep(time.Duration(10<<attempt) * time.Millisecond)
			continue
		}
		return "", err
	}
}

// probe proves the endpoint honors both conditional headers: a store on one
// that does not would let concurrent writers overwrite each other.
func (s *Store) probe(ctx context.Context) error {
	id, err := random(8)
	if err != nil {
		return err
	}
	key := s.prefix + "probe/" + hex.EncodeToString(id)
	body := bytes.NewReader([]byte("snapshot-core conditional-write probe"))
	size := body.Size()
	etag, err := s.put(ctx, key, body, size, "*", "")
	if err != nil {
		return fmt.Errorf("s3: probe write: %w", err)
	}
	defer func() {
		_, _ = s.c.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	}()
	if _, err := s.put(ctx, key, body, size, "*", ""); statusOf(err) != http.StatusPreconditionFailed {
		return unsafe("a second PUT with If-None-Match: * was not refused", err)
	}
	if _, err := s.put(ctx, key, body, size, "", `"00000000000000000000000000000000"`); statusOf(err) != http.StatusPreconditionFailed {
		return unsafe("a PUT with a wrong If-Match ETag was not refused", err)
	}
	if _, err := s.put(ctx, key, body, size, "", etag); err != nil {
		return unsafe("a PUT with the right If-Match ETag failed", err)
	}
	return nil
}

// probeObjects proves the endpoint takes, returns and deletes an object,
// which is all an objects-only store asks of it.
func (s *Store) probeObjects(ctx context.Context) error {
	id, err := random(8)
	if err != nil {
		return err
	}
	key := s.prefix + "probe/" + hex.EncodeToString(id)
	body := bytes.NewReader([]byte("snapshot-core objects probe"))
	if _, err := s.put(ctx, key, body, body.Size(), "", ""); err != nil {
		return fmt.Errorf("s3: probe write: %w", err)
	}
	if _, err := s.c.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}); err != nil {
		return fmt.Errorf("s3: probe read: %w", err)
	}
	if _, err := s.c.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}); err != nil {
		return fmt.Errorf("s3: probe delete: %w", err)
	}
	return nil
}

func unsafe(what string, err error) error {
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrUnsafeEndpoint, what, err)
	}
	return fmt.Errorf("%w: %s", ErrUnsafeEndpoint, what)
}

// Put implements blob.BlobStore.
func (s *Store) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if err := blob.CheckPut(name, size); err != nil {
		return err
	}
	var body io.ReadSeeker
	if rs, ok := r.(io.ReadSeeker); ok {
		cur, err := rs.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		end, err := rs.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		if end-cur != size {
			return fmt.Errorf("%w: body is %d bytes, declared %d", blob.ErrSizeMismatch, end-cur, size)
		}
		body = io.NewSectionReader(readerAt{rs}, cur, size)
	} else {
		var buf bytes.Buffer
		if err := blob.CopyExact(&buf, r, size); err != nil {
			return err
		}
		body = bytes.NewReader(buf.Bytes())
	}
	if s.objectsOnly {
		// Best effort: the endpoint ignores If-None-Match, so ask first.
		// Every name a repository writes is unique, so two writers racing
		// on one name write the same bytes.
		if _, err := s.Stat(ctx, name); err == nil {
			return blob.ErrExists
		} else if !errors.Is(err, blob.ErrNotFound) {
			return err
		}
		_, err := s.put(ctx, s.objectKey(name), body, size, "", "")
		return err
	}
	_, err := s.put(ctx, s.objectKey(name), body, size, "*", "")
	if statusOf(err) == http.StatusPreconditionFailed {
		return blob.ErrExists
	}
	return err
}

// WriteMirror replaces the root's copy a split store keeps here, in place.
func (s *Store) WriteMirror(ctx context.Context, value []byte) error {
	if err := blob.CheckRootValue(value); err != nil {
		return err
	}
	_, err := s.put(ctx, s.mirrorKey(), bytes.NewReader(value), int64(len(value)), "", "")
	return err
}

// ReadMirror returns the root's copy, or nothing when there is none.
func (s *Store) ReadMirror(ctx context.Context) ([]byte, error) {
	out, err := s.c.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.mirrorKey())})
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(io.LimitReader(out.Body, blob.MaxRootSize+1))
	if err != nil {
		return nil, err
	}
	if len(b) > blob.MaxRootSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrCorrupt, len(b))
	}
	return b, nil
}

// readerAt adapts an io.ReadSeeker to io.ReaderAt for io.SectionReader.
// Put is the only user, one reader at a time.
type readerAt struct{ rs io.ReadSeeker }

func (r readerAt) ReadAt(p []byte, off int64) (int, error) {
	if _, err := r.rs.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	return io.ReadFull(r.rs, p)
}

// Get implements blob.BlobStore.
func (s *Store) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	if err := blob.ValidName(name); err != nil {
		return nil, err
	}
	if off < 0 || n < -1 {
		return nil, fmt.Errorf("%w: off=%d n=%d", blob.ErrInvalidRange, off, n)
	}
	if n == 0 {
		// Nothing to read, but the object must exist and off must be in range.
		info, err := s.Stat(ctx, name)
		if err != nil {
			return nil, err
		}
		if _, _, err := blob.Range(off, 0, info.Size); err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	in := &awss3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.objectKey(name))}
	switch {
	case off == 0 && n == -1:
	case n == -1:
		in.Range = aws.String(fmt.Sprintf("bytes=%d-", off))
	default:
		in.Range = aws.String(fmt.Sprintf("bytes=%d-%d", off, off+n-1))
	}
	out, err := s.c.GetObject(ctx, in)
	if err == nil {
		return out.Body, nil
	}
	if isNotFound(err) {
		return nil, blob.ErrNotFound
	}
	if statusOf(err) == http.StatusRequestedRangeNotSatisfiable {
		// S3 answers 416 for off >= size; off == size is an empty read.
		size, ok := rangeSize(err)
		if !ok {
			info, serr := s.Stat(ctx, name)
			if serr != nil {
				return nil, serr
			}
			size = info.Size
		}
		if off == size {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		return nil, fmt.Errorf("%w: off=%d past size %d", blob.ErrInvalidRange, off, size)
	}
	return nil, err
}

// rangeSize reads the object size from a 416's "Content-Range: bytes */size".
func rangeSize(err error) (int64, bool) {
	var re *awshttp.ResponseError
	if !errors.As(err, &re) || re.Response == nil {
		return 0, false
	}
	cr := re.Response.Header.Get("Content-Range")
	sizeStr, ok := strings.CutPrefix(cr, "bytes */")
	if !ok {
		return 0, false
	}
	size, perr := strconv.ParseInt(sizeStr, 10, 64)
	return size, perr == nil
}

// Stat implements blob.BlobStore.
func (s *Store) Stat(ctx context.Context, name string) (blob.Info, error) {
	if err := blob.ValidName(name); err != nil {
		return blob.Info{}, err
	}
	out, err := s.c.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.objectKey(name))})
	if err != nil {
		if isNotFound(err) {
			return blob.Info{}, blob.ErrNotFound
		}
		return blob.Info{}, err
	}
	return blob.Info{Name: name, Size: aws.ToInt64(out.ContentLength), ModTime: aws.ToTime(out.LastModified)}, nil
}

// List implements blob.BlobStore. S3 may return a short truncated page, so
// it keeps reading until the page is full or the listing ends: a short page
// must mean the end.
func (s *Store) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	if err := blob.CheckList(prefix, limit); err != nil {
		return nil, err
	}
	base := s.objectBase()
	in := &awss3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(base + prefix),
	}
	if after != "" {
		in.StartAfter = aws.String(base + after + keySuffix)
	}
	var out []blob.Info
	for len(out) < limit {
		in.MaxKeys = aws.Int32(int32(limit - len(out)))
		page, err := s.c.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			name, ok := strings.CutSuffix(strings.TrimPrefix(aws.ToString(o.Key), base), keySuffix)
			if !ok {
				continue // not one of ours
			}
			out = append(out, blob.Info{Name: name, Size: aws.ToInt64(o.Size), ModTime: aws.ToTime(o.LastModified)})
		}
		if !aws.ToBool(page.IsTruncated) || page.NextContinuationToken == nil {
			break
		}
		in.ContinuationToken = page.NextContinuationToken
	}
	return out, nil
}

// Delete implements blob.BlobStore.
func (s *Store) Delete(ctx context.Context, name string) error {
	if err := blob.ValidName(name); err != nil {
		return err
	}
	_, err := s.c.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.objectKey(name))})
	if err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

// readRoot returns the root's nonce-prefixed body and ETag; a missing root
// is an empty body and NoVersion.
func (s *Store) readRoot(ctx context.Context) ([]byte, blob.Version, error) {
	out, err := s.c.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.rootKey())})
	if err != nil {
		if isNotFound(err) {
			return nil, blob.NoVersion, nil
		}
		return nil, blob.NoVersion, err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(io.LimitReader(out.Body, nonceSize+blob.MaxRootSize+1))
	if err != nil {
		return nil, blob.NoVersion, err
	}
	if len(body) <= nonceSize || len(body) > nonceSize+blob.MaxRootSize {
		return nil, blob.NoVersion, fmt.Errorf("%w: %d bytes", ErrCorrupt, len(body))
	}
	return body, blob.Version(aws.ToString(out.ETag)), nil
}

// Root implements blob.BlobStore.
func (s *Store) Root(ctx context.Context) (blob.Root, error) {
	if s.objectsOnly {
		return blob.Root{}, fmt.Errorf("%w: this S3 store holds objects only", blob.ErrNoRoot)
	}
	body, v, err := s.readRoot(ctx)
	if err != nil || v == blob.NoVersion {
		return blob.Root{}, err
	}
	return blob.Root{Value: body[nonceSize:], Version: v}, nil
}

// SwapRoot implements blob.BlobStore.
func (s *Store) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	if s.objectsOnly {
		return blob.NoVersion, fmt.Errorf("%w: this S3 store holds objects only", blob.ErrNoRoot)
	}
	if err := blob.CheckRootValue(next); err != nil {
		return blob.NoVersion, err
	}
	nonce, err := random(nonceSize)
	if err != nil {
		return blob.NoVersion, err
	}
	body := make([]byte, 0, nonceSize+len(next))
	body = append(append(body, nonce...), next...)
	ifNoneMatch, ifMatch := "", string(expected)
	if expected == blob.NoVersion {
		ifNoneMatch = "*"
	}
	etag, err := s.put(ctx, s.rootKey(), bytes.NewReader(body), int64(len(body)), ifNoneMatch, ifMatch)
	if err == nil {
		return blob.Version(etag), nil
	}
	if st := statusOf(err); st != http.StatusPreconditionFailed && st != http.StatusConflict {
		return blob.NoVersion, err
	}
	// Refused. Either someone else swapped first, or our own first attempt
	// landed and only its response was lost: the nonce tells them apart.
	cur, v, rerr := s.readRoot(ctx)
	if rerr == nil && bytes.Equal(cur, body) {
		return v, nil
	}
	return blob.NoVersion, blob.ErrRootConflict
}
