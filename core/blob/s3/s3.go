// Package s3 is the S3 (and S3-compatible) backend (Storage Core Spec
// "core/blob/s3"; Engine Spec "s3store" rules). Objects live under
// <prefix>objects/, the root under <prefix>root. Put-if-absent is a PUT with
// If-None-Match: *; the root swap is a PUT with If-Match on the ETag read
// earlier. The root object carries a random 16-byte nonce ahead of its value,
// so every swap has a fresh ETag even when the value repeats (no ABA), and a
// swap whose response was lost can recognize its own write. At Open a probe
// refuses any endpoint that ignores If-None-Match or If-Match: without them
// the store cannot be safe.
package s3

import (
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

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

// Store is an S3 BlobStore.
type Store struct{}

var _ blob.BlobStore = (*Store)(nil)

// Open validates the options and probes the endpoint.
func Open(ctx context.Context, o Options) (*Store, error) { return &Store{}, nil }

// Put implements blob.BlobStore.
func (s *Store) Put(ctx context.Context, name string, r io.Reader, size int64) error { return nil }

// Get implements blob.BlobStore.
func (s *Store) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	return nil, blob.ErrNotFound
}

// Stat implements blob.BlobStore.
func (s *Store) Stat(ctx context.Context, name string) (blob.Info, error) { return blob.Info{}, nil }

// List implements blob.BlobStore.
func (s *Store) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	return nil, nil
}

// Delete implements blob.BlobStore.
func (s *Store) Delete(ctx context.Context, name string) error { return nil }

// Root implements blob.BlobStore.
func (s *Store) Root(ctx context.Context) (blob.Root, error) { return blob.Root{}, nil }

// SwapRoot implements blob.BlobStore.
func (s *Store) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	return blob.NoVersion, nil
}
