package s3

import (
	"time"
)

const (
	// maxObjectSize is the maximum single-PUT object size accepted by the API.
	// Data is stored in SQLite; only a small commitment envelope (~200 bytes)
	// is submitted to Celestia, so this limit is independent of Celestia's blob size.
	maxObjectSize = 2 * 1024 * 1024 // 2MB

	// maxKeyLength is the S3 maximum object key length in bytes.
	maxKeyLength = 1024
)

// CommitmentEnvelope is the JSON payload submitted to Celestia as an audit record.
// It contains a SHA256 digest of the object content rather than the raw data,
// making Celestia a verification layer rather than a storage layer. Clients can
// prove data authenticity by hashing a downloaded object and comparing against
// the on-chain record.
type CommitmentEnvelope struct {
	Version     int    `json:"version"`      // schema version, currently 1
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"` // hex-encoded SHA-256 of raw object data
	ETag        string `json:"etag"`   // hex-encoded MD5, kept for S3 compatibility
}

// Bucket represents an S3 bucket.
type Bucket struct {
	Name         string
	CreatedAt    time.Time
	LastModified time.Time
}

// Object represents an S3 object with Celestia verification metadata.
type Object struct {
	Key          string
	Bucket       string
	Size         int64
	ETag         string // MD5 hash of object content
	ContentType  string
	LastModified time.Time

	SHA256      string   // hex SHA-256 of object content, anchored on Celestia
	Height      uint64   // Celestia height where commitment was submitted
	Namespace   string   // Namespace used for commitment submission
	Commitments []string // Celestia blob commitments (of the envelope, not raw data)
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
