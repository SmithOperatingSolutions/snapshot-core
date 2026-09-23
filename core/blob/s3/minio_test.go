package s3_test

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/s3"
)

// realS3 returns a client for the S3-compatible server named by the
// environment (MinIO in CI; `mise run minio` starts one locally). Without one
// the test skips, unless SNAPSHOT_S3_REQUIRED=1 says the tier must run: then
// a missing endpoint is a failure, never a silent skip (docs/TESTING.md §7).
func realS3(t *testing.T) (*awss3.Client, string) {
	t.Helper()
	ep := os.Getenv("SNAPSHOT_S3_ENDPOINT")
	if ep == "" {
		if os.Getenv("SNAPSHOT_S3_REQUIRED") == "1" {
			t.Fatal("SNAPSHOT_S3_REQUIRED=1 but SNAPSHOT_S3_ENDPOINT is unset: the real-S3 tier must run, not skip")
		}
		t.Skip("set SNAPSHOT_S3_ENDPOINT (and SNAPSHOT_S3_REQUIRED=1 in CI) to run against a real S3-compatible server")
	}
	region := os.Getenv("SNAPSHOT_S3_REGION")
	if region == "" {
		region = "us-east-1"
	}
	b := os.Getenv("SNAPSHOT_S3_BUCKET")
	if b == "" {
		b = bucket
	}
	c := s3.NewClient(ep, region, os.Getenv("SNAPSHOT_S3_ACCESS_KEY"), os.Getenv("SNAPSHOT_S3_SECRET_KEY"))
	if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
		t.Fatalf("SNAPSHOT_S3_ENDPOINT %q is not a URL", ep)
	}
	_, err := c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(b)})
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if err != nil && !errors.As(err, &owned) && !errors.As(err, &exists) {
		t.Fatalf("creating bucket %s: %v", b, err)
	}
	return c, b
}

// The Storage Core Spec's "MinIO in CI" tier: the full contract, 50 racing
// swappers included, against a real S3-compatible server. Passing the probe
// at Open is part of it. With SNAPSHOT_S3_OBJECTS_ONLY=1 (the nightly run on
// a provider that ignores conditional writes, #4) the store opens objects
// only and the contract runs for objects.
func TestContractAgainstRealS3(t *testing.T) {
	c, b := realS3(t)
	objectsOnly := os.Getenv("SNAPSHOT_S3_OBJECTS_ONLY") == "1"
	contract.Run(t, func(t *testing.T) blob.BlobStore {
		st, err := s3.Open(ctx, s3.Options{Client: c, Bucket: b, Prefix: randomPrefix(t), AllowHTTP: true, ObjectsOnly: objectsOnly})
		if err != nil {
			t.Fatalf("Open against the real server: %v", err)
		}
		return st
	}, contract.Options{Swappers: 50, ObjectsOnly: objectsOnly})
}
