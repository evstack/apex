package s3

import (
	"time"
)

const (
	maxObjectSize = 5 * 1024 * 1024 * 1024 // 5GB S3 limit
)

type Bucket struct {
	Name         string
	CreatedAt    time.Time
	LastModified time.Time
}

type Object struct {
	Key          string
	Bucket       string
	Size         int64
	ETag         string // MD5 hash of object content
	ContentType  string
	LastModified time.Time

	Height      uint64 // Celestia height where blobs were submitted
	Namespace   string // Namespace used for blob storage
	BlobCount   int    // Number of blobs the object was split into
	Commitments []string
}

type ListBucketsResult struct {
	Buckets []Bucket
}

type ListObjectsResult struct {
	Bucket    string
	Prefix    string
	Delimiter string
	IsTruncated bool
	Objects   []ObjectInfo
	CommonPrefixes []string
}

type ObjectInfo struct {
	Key          string
	LastModified time.Time
	ETag         string
	Size         int64
	StorageClass string
}

type CopyObjectResult struct {
	LastModified time.Time
	ETag         string
}

type Part struct {
	PartNumber   int
	ETag         string
	LastModified time.Time
	Size         int64
}

type MultipartUpload struct {
	UploadID   string
	Bucket     string
	Key        string
	Initiated  time.Time
	Parts      []Part
}
