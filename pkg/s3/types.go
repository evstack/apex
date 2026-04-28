package s3

import (
	"time"
)

const (
	// maxObjectSize is the maximum single-PUT object size.
	// Capped at Celestia's blob size limit since each object maps to one blob.
	maxObjectSize = 2 * 1024 * 1024 // 2MB
)

// Bucket represents an S3 bucket.
type Bucket struct {
	Name         string
	CreatedAt    time.Time
	LastModified time.Time
}

// Object represents an S3 object with optional Celestia anchoring metadata.
type Object struct {
	Key          string
	Bucket       string
	Size         int64
	ETag         string // MD5 hash of object content
	ContentType  string
	LastModified time.Time

	Height      uint64   // Celestia height where blob was submitted
	Namespace   string   // Namespace used for blob storage
	Commitments []string // Celestia blob commitments
}

// ListObjectsResult is the result of a ListObjects call.
type ListObjectsResult struct {
	Bucket         string
	Prefix         string
	Delimiter      string
	IsTruncated    bool
	NextMarker     string
	Objects        []ObjectInfo
	CommonPrefixes []string
}

// ObjectInfo is a summary of an object for list responses.
type ObjectInfo struct {
	Key          string
	LastModified time.Time
	ETag         string
	Size         int64
	StorageClass string
}
