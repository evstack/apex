package e2e

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3ObjectLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed e2e test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), chainStartupTimeout)
	defer cancel()

	grpcAddr, chainID, signerKeyHex, signerAddress := startSubmissionTestChain(t, ctx)
	namespace := testNamespace(t, []byte("apex-s3"))
	data := []byte("apex s3 e2e")
	commitment := mustBlobCommitment(t, namespace, data)
	accessKeyID := "settings-key"
	secretKey := "settings-secret"

	apexBinary := buildApexBinary(t)
	apexRPCAddr := reserveTCPAddr(t)
	apexGRPCAddr := reserveTCPAddr(t)
	apexS3Addr := reserveTCPAddr(t)
	keyPath := writeSignerKey(t, signerKeyHex)
	configPath := writeApexConfig(t, apexConfig{
		Namespace:       namespace,
		DataGRPCAddr:    grpcAddr,
		SubmissionGRPC:  grpcAddr,
		ChainID:         chainID,
		SignerKeyPath:   keyPath,
		StoragePath:     filepath.Join(t.TempDir(), "apex.db"),
		RPCListenAddr:   apexRPCAddr,
		GRPCListenAddr:  apexGRPCAddr,
		S3Enabled:       true,
		S3ListenAddr:    apexS3Addr,
		S3Region:        "us-east-1",
		S3Namespace:     namespace,
		S3AccessKeyID:   accessKeyID,
		S3SecretKey:     secretKey,
		GasPrice:        submissionGasPrice,
		MaxGasPrice:     submissionGasPrice,
		ConfirmTimeoutS: submissionConfirmTimeout,
	})

	proc := startApexProcess(t, apexBinary, configPath)
	defer proc.Stop(t)

	waitForApexHTTP(t, proc, apexRPCAddr)
	waitForS3HTTP(t, proc, apexS3Addr)

	client := newS3Client(t, "http://"+apexS3Addr, accessKeyID, secretKey)

	bucket := "apex-s3-e2e"
	key := "hello.txt"

	listBuckets, err := client.ListBuckets(context.Background(), &awss3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(listBuckets.Buckets) != 0 {
		t.Fatalf("expected no buckets, got %d", len(listBuckets.Buckets))
	}

	if _, err := client.CreateBucket(context.Background(), &awss3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	putOut, err := client.PutObject(context.Background(), &awss3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if aws.ToString(putOut.ETag) == "" {
		t.Fatal("expected ETag from PutObject")
	}

	headOut, err := client.HeadObject(context.Background(), &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if headOut.ContentLength == nil || aws.ToInt64(headOut.ContentLength) != int64(len(data)) {
		t.Fatalf("content length = %v, want %d", headOut.ContentLength, len(data))
	}
	if aws.ToString(headOut.ETag) == "" {
		t.Fatal("expected ETag from HeadObject")
	}

	getOut, err := client.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	gotData, err := io.ReadAll(getOut.Body)
	_ = getOut.Body.Close()
	if err != nil {
		t.Fatalf("read object body: %v", err)
	}
	if !bytes.Equal(gotData, data) {
		t.Fatalf("object data = %q, want %q", gotData, data)
	}

	listObjects, err := client.ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(listObjects.Contents) != 1 {
		t.Fatalf("expected 1 object, got %d", len(listObjects.Contents))
	}
	if got := aws.ToString(listObjects.Contents[0].Key); got != key {
		t.Fatalf("listed key = %q, want %q", got, key)
	}

	waitForIndexedBlob(t, proc, apexRPCAddr, commitment, data, namespace, signerAddress)

	if _, err := client.DeleteObject(context.Background(), &awss3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	if _, err := client.DeleteBucket(context.Background(), &awss3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	listBuckets, err = client.ListBuckets(context.Background(), &awss3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets after delete: %v", err)
	}
	if len(listBuckets.Buckets) != 0 {
		t.Fatalf("expected no buckets after delete, got %d", len(listBuckets.Buckets))
	}
}

func newS3Client(t *testing.T, endpoint, accessKeyID, secretKey string) *awss3.Client {
	t.Helper()

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, "")),
	)
	if err != nil {
		t.Fatalf("load AWS config: %v", err)
	}

	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
}
