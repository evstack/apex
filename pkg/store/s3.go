package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/evstack/apex/config"
	"github.com/evstack/apex/pkg/metrics"
	"github.com/evstack/apex/pkg/types"
)

// s3Client abstracts the S3 operations used by S3Store, enabling mock-based testing.
type s3Client interface {
	GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// blobChunkKey identifies a blob chunk in S3 by namespace and chunk start height.
type blobChunkKey struct {
	namespace  types.Namespace
	chunkStart uint64
}

// commitEntry is a buffered commitment index write.
type commitEntry struct {
	commitmentHex string
	pointer       commitPointer
}

// commitPointer is the JSON content of a commitment index object.
type commitPointer struct {
	Namespace string `json:"namespace"`
	Height    uint64 `json:"height"`
	Index     int    `json:"index"`
}

// s3Blob is the JSON representation of a blob in an S3 chunk object.
type s3Blob struct {
	Height       uint64 `json:"height"`
	Namespace    string `json:"namespace"`
	Data         []byte `json:"data"`
	Commitment   []byte `json:"commitment"`
	ShareVersion uint32 `json:"share_version"`
	Signer       []byte `json:"signer"`
	Index        int    `json:"index"`
}

// s3Header is the JSON representation of a header in an S3 chunk object.
type s3Header struct {
	Height    uint64    `json:"height"`
	Hash      []byte    `json:"hash"`
	DataHash  []byte    `json:"data_hash"`
	Time      time.Time `json:"time"`
	RawHeader []byte    `json:"raw_header"`
}

// maxFlushConcurrency bounds parallel S3 I/O during flush.
const maxFlushConcurrency = 8

// S3Store implements Store using an S3-compatible object store.
// Writes are buffered in memory and flushed to S3 at checkpoint boundaries
// (SetSyncState calls and Close).
type S3Store struct {
	client    s3Client
	bucket    string
	prefix    string
	chunkSize uint64
	metrics   metrics.Recorder

	mu        sync.Mutex
	blobBuf   map[blobChunkKey][]types.Blob
	headerBuf map[uint64][]*types.Header
	commitBuf []commitEntry

	nsMu    sync.Mutex // guards PutNamespace read-modify-write (separate from buffer mu)
	flushMu sync.Mutex // serializes flush operations
}

// NewS3Store creates a new S3Store from the given config.
func NewS3Store(ctx context.Context, cfg *config.S3Config) (*S3Store, error) {
	opts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	chunkSize := uint64(cfg.ChunkSize)
	if chunkSize == 0 {
		chunkSize = 64
	}

	return newS3Store(client, cfg.Bucket, cfg.Prefix, chunkSize), nil
}

// NewS3StoreWithStaticCredentials creates an S3Store with explicit credentials.
// Useful for testing against MinIO or other S3-compatible services.
func NewS3StoreWithStaticCredentials(ctx context.Context, cfg *config.S3Config, accessKey, secretKey, token string) (*S3Store, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, token)),
	}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	chunkSize := uint64(cfg.ChunkSize)
	if chunkSize == 0 {
		chunkSize = 64
	}

	return newS3Store(client, cfg.Bucket, cfg.Prefix, chunkSize), nil
}

// newS3Store creates an S3Store with an injected client (for testing).
func newS3Store(client s3Client, bucket, prefix string, chunkSize uint64) *S3Store {
	return &S3Store{
		client:    client,
		bucket:    bucket,
		prefix:    strings.TrimSuffix(prefix, "/"),
		chunkSize: chunkSize,
		metrics:   metrics.Nop(),
		blobBuf:   make(map[blobChunkKey][]types.Blob),
		headerBuf: make(map[uint64][]*types.Header),
	}
}

// SetMetrics sets the metrics recorder.
// Must be called before any other method — not safe for concurrent use.
func (s *S3Store) SetMetrics(m metrics.Recorder) {
	s.metrics = m
}

// chunkStart returns the chunk start height for a given height.
func (s *S3Store) chunkStart(height uint64) uint64 {
	return (height / s.chunkSize) * s.chunkSize
}

// key builds an S3 object key with the configured prefix.
func (s *S3Store) key(parts ...string) string {
	if s.prefix == "" {
		return strings.Join(parts, "/")
	}
	return s.prefix + "/" + strings.Join(parts, "/")
}

func chunkFileName(start uint64) string {
	return fmt.Sprintf("chunk_%016d.json", start)
}

// --- Write methods (buffer) ---

func (s *S3Store) PutBlobs(ctx context.Context, blobs []types.Blob) error {
	if len(blobs) == 0 {
		return nil
	}
	start := time.Now()
	defer func() { s.metrics.ObserveStoreQueryDuration("PutBlobs", time.Since(start)) }()

	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range blobs {
		b := &blobs[i]
		key := blobChunkKey{namespace: b.Namespace, chunkStart: s.chunkStart(b.Height)}
		s.blobBuf[key] = append(s.blobBuf[key], *b)

		s.commitBuf = append(s.commitBuf, commitEntry{
			commitmentHex: hex.EncodeToString(b.Commitment),
			pointer: commitPointer{
				Namespace: b.Namespace.String(),
				Height:    b.Height,
				Index:     b.Index,
			},
		})
	}
	return nil
}

func (s *S3Store) PutHeader(ctx context.Context, header *types.Header) error {
	start := time.Now()
	defer func() { s.metrics.ObserveStoreQueryDuration("PutHeader", time.Since(start)) }()

	s.mu.Lock()
	defer s.mu.Unlock()

	cs := s.chunkStart(header.Height)
	s.headerBuf[cs] = append(s.headerBuf[cs], header)
	return nil
}

func (s *S3Store) PutNamespace(ctx context.Context, ns types.Namespace) error {
	s.nsMu.Lock()
	defer s.nsMu.Unlock()

	key := s.key("meta", "namespaces.json")

	existing, err := s.getObject(ctx, key)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("get namespaces: %w", err)
	}

	namespaces := make([]string, 0, 1)
	if existing != nil {
		if err := json.Unmarshal(existing, &namespaces); err != nil {
			return fmt.Errorf("decode namespaces: %w", err)
		}
	}

	nsHex := ns.String()
	if slices.Contains(namespaces, nsHex) {
		return nil
	}
	namespaces = append(namespaces, nsHex)

	data, err := json.Marshal(namespaces)
	if err != nil {
		return fmt.Errorf("encode namespaces: %w", err)
	}
	return s.putObject(ctx, key, data)
}

// --- Read methods ---

func (s *S3Store) GetBlob(ctx context.Context, ns types.Namespace, height uint64, index int) (*types.Blob, error) {
	start := time.Now()
	defer func() { s.metrics.ObserveStoreQueryDuration("GetBlob", time.Since(start)) }()

	// Check buffer first.
	if b := s.findBlobInBuffer(ns, height, index); b != nil {
		return b, nil
	}

	cs := s.chunkStart(height)
	key := s.key("blobs", ns.String(), chunkFileName(cs))

	data, err := s.getObject(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get blob chunk: %w", err)
	}

	blobs, err := decodeS3Blobs(data)
	if err != nil {
		return nil, err
	}

	for i := range blobs {
		if blobs[i].Height == height && blobs[i].Index == index && blobs[i].Namespace == ns {
			return &blobs[i], nil
		}
	}
	return nil, ErrNotFound
}

func (s *S3Store) GetBlobs(ctx context.Context, ns types.Namespace, startHeight, endHeight uint64, limit, offset int) ([]types.Blob, error) {
	start := time.Now()
	defer func() { s.metrics.ObserveStoreQueryDuration("GetBlobs", time.Since(start)) }()

	buffered := s.collectBufferedBlobs(ns, startHeight, endHeight)

	s3Blobs, err := s.fetchBlobChunks(ctx, ns, startHeight, endHeight)
	if err != nil {
		return nil, err
	}

	allBlobs := deduplicateBlobs(append(buffered, s3Blobs...))

	sort.Slice(allBlobs, func(i, j int) bool {
		if allBlobs[i].Height != allBlobs[j].Height {
			return allBlobs[i].Height < allBlobs[j].Height
		}
		return allBlobs[i].Index < allBlobs[j].Index
	})

	return applyOffsetLimit(allBlobs, offset, limit), nil
}

// collectBufferedBlobs returns in-buffer blobs matching the namespace and height range.
func (s *S3Store) collectBufferedBlobs(ns types.Namespace, startHeight, endHeight uint64) []types.Blob {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []types.Blob
	for key, bufs := range s.blobBuf {
		if key.namespace != ns {
			continue
		}
		for i := range bufs {
			if bufs[i].Height >= startHeight && bufs[i].Height <= endHeight {
				result = append(result, bufs[i])
			}
		}
	}
	return result
}

// fetchBlobChunks reads all S3 blob chunks overlapping the height range for a namespace.
func (s *S3Store) fetchBlobChunks(ctx context.Context, ns types.Namespace, startHeight, endHeight uint64) ([]types.Blob, error) {
	firstChunk := s.chunkStart(startHeight)
	lastChunk := s.chunkStart(endHeight)

	var allBlobs []types.Blob
	for cs := firstChunk; cs <= lastChunk; cs += s.chunkSize {
		key := s.key("blobs", ns.String(), chunkFileName(cs))
		data, err := s.getObject(ctx, key)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get blob chunk at %d: %w", cs, err)
		}

		blobs, err := decodeS3Blobs(data)
		if err != nil {
			return nil, err
		}
		for i := range blobs {
			if blobs[i].Height >= startHeight && blobs[i].Height <= endHeight && blobs[i].Namespace == ns {
				allBlobs = append(allBlobs, blobs[i])
			}
		}
	}
	return allBlobs, nil
}

func applyOffsetLimit(items []types.Blob, offset, limit int) []types.Blob {
	if offset > 0 {
		if offset >= len(items) {
			return nil
		}
		items = items[offset:]
	}
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	return items
}

func (s *S3Store) GetBlobByCommitment(ctx context.Context, commitment []byte) (*types.Blob, error) {
	start := time.Now()
	defer func() { s.metrics.ObserveStoreQueryDuration("GetBlobByCommitment", time.Since(start)) }()

	// Check buffer first.
	commitHex := hex.EncodeToString(commitment)

	s.mu.Lock()
	for _, entry := range s.commitBuf {
		if entry.commitmentHex == commitHex {
			ns, err := types.NamespaceFromHex(entry.pointer.Namespace)
			if err == nil {
				if b := s.findBlobInBufferLocked(ns, entry.pointer.Height, entry.pointer.Index); b != nil {
					s.mu.Unlock()
					return b, nil
				}
			}
		}
	}
	s.mu.Unlock()

	// Look up commitment index in S3.
	key := s.key("index", "commitments", commitHex+".json")
	data, err := s.getObject(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get commitment index: %w", err)
	}

	var ptr commitPointer
	if err := json.Unmarshal(data, &ptr); err != nil {
		return nil, fmt.Errorf("decode commitment pointer: %w", err)
	}

	ns, err := types.NamespaceFromHex(ptr.Namespace)
	if err != nil {
		return nil, fmt.Errorf("parse namespace from commitment index: %w", err)
	}

	return s.GetBlob(ctx, ns, ptr.Height, ptr.Index)
}

func (s *S3Store) GetHeader(ctx context.Context, height uint64) (*types.Header, error) {
	start := time.Now()
	defer func() { s.metrics.ObserveStoreQueryDuration("GetHeader", time.Since(start)) }()

	// Check buffer first.
	if h := s.findHeaderInBuffer(height); h != nil {
		return h, nil
	}

	cs := s.chunkStart(height)
	key := s.key("headers", chunkFileName(cs))

	data, err := s.getObject(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get header chunk: %w", err)
	}

	headers, err := decodeS3Headers(data)
	if err != nil {
		return nil, err
	}
	for i := range headers {
		if headers[i].Height == height {
			return &headers[i], nil
		}
	}
	return nil, ErrNotFound
}

func (s *S3Store) GetNamespaces(ctx context.Context) ([]types.Namespace, error) {
	key := s.key("meta", "namespaces.json")
	data, err := s.getObject(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get namespaces: %w", err)
	}

	var hexList []string
	if err := json.Unmarshal(data, &hexList); err != nil {
		return nil, fmt.Errorf("decode namespaces: %w", err)
	}

	namespaces := make([]types.Namespace, 0, len(hexList))
	for _, h := range hexList {
		ns, err := types.NamespaceFromHex(h)
		if err != nil {
			return nil, fmt.Errorf("parse namespace %q: %w", h, err)
		}
		namespaces = append(namespaces, ns)
	}
	return namespaces, nil
}

func (s *S3Store) GetSyncState(ctx context.Context) (*types.SyncStatus, error) {
	start := time.Now()
	defer func() { s.metrics.ObserveStoreQueryDuration("GetSyncState", time.Since(start)) }()

	key := s.key("meta", "sync_state.json")
	data, err := s.getObject(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get sync state: %w", err)
	}

	var state struct {
		State         int    `json:"state"`
		LatestHeight  uint64 `json:"latest_height"`
		NetworkHeight uint64 `json:"network_height"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode sync state: %w", err)
	}
	return &types.SyncStatus{
		State:         types.SyncState(state.State),
		LatestHeight:  state.LatestHeight,
		NetworkHeight: state.NetworkHeight,
	}, nil
}

func (s *S3Store) SetSyncState(ctx context.Context, status types.SyncStatus) error {
	// Flush buffered data first — this is the checkpoint boundary.
	if err := s.flush(ctx); err != nil {
		return fmt.Errorf("flush before sync state: %w", err)
	}

	data, err := json.Marshal(struct {
		State         int    `json:"state"`
		LatestHeight  uint64 `json:"latest_height"`
		NetworkHeight uint64 `json:"network_height"`
	}{
		State:         int(status.State),
		LatestHeight:  status.LatestHeight,
		NetworkHeight: status.NetworkHeight,
	})
	if err != nil {
		return fmt.Errorf("encode sync state: %w", err)
	}

	key := s.key("meta", "sync_state.json")
	return s.putObject(ctx, key, data)
}

func (s *S3Store) Close() error {
	return s.flush(context.Background())
}

// --- Flush ---

// flush drains the write buffer to S3. Called at checkpoint boundaries.
// Serialized by flushMu to prevent concurrent read-modify-write races on S3 chunk objects.
func (s *S3Store) flush(ctx context.Context) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.Lock()
	blobBuf := s.blobBuf
	headerBuf := s.headerBuf
	commitBuf := s.commitBuf
	s.blobBuf = make(map[blobChunkKey][]types.Blob)
	s.headerBuf = make(map[uint64][]*types.Header)
	s.commitBuf = nil
	s.mu.Unlock()

	if len(blobBuf) == 0 && len(headerBuf) == 0 && len(commitBuf) == 0 {
		return nil
	}

	// Use a semaphore to bound concurrency.
	sem := make(chan struct{}, maxFlushConcurrency)
	var (
		mu   sync.Mutex
		errs []error
	)
	addErr := func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}

	var wg sync.WaitGroup

	// Flush blob chunks.
	for key, blobs := range blobBuf {
		wg.Add(1)
		go func(key blobChunkKey, blobs []types.Blob) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := s.flushBlobChunk(ctx, key, blobs); err != nil {
				addErr(err)
			}
		}(key, blobs)
	}

	// Flush header chunks.
	for cs, headers := range headerBuf {
		wg.Add(1)
		go func(cs uint64, headers []*types.Header) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := s.flushHeaderChunk(ctx, cs, headers); err != nil {
				addErr(err)
			}
		}(cs, headers)
	}

	// Flush commitment indices.
	for _, entry := range commitBuf {
		wg.Add(1)
		go func(e commitEntry) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := s.flushCommitIndex(ctx, e); err != nil {
				addErr(err)
			}
		}(entry)
	}

	wg.Wait()
	return errors.Join(errs...)
}

func (s *S3Store) flushBlobChunk(ctx context.Context, key blobChunkKey, newBlobs []types.Blob) error {
	objKey := s.key("blobs", key.namespace.String(), chunkFileName(key.chunkStart))

	// Read existing chunk (may 404).
	existing, err := s.getObject(ctx, objKey)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("read blob chunk for merge: %w", err)
	}

	var merged []types.Blob
	if existing != nil {
		merged, err = decodeS3Blobs(existing)
		if err != nil {
			return fmt.Errorf("decode existing blob chunk: %w", err)
		}
	}

	merged = append(merged, newBlobs...)
	merged = deduplicateBlobs(merged)
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Height != merged[j].Height {
			return merged[i].Height < merged[j].Height
		}
		return merged[i].Index < merged[j].Index
	})

	data, err := encodeS3Blobs(merged)
	if err != nil {
		return fmt.Errorf("encode blob chunk: %w", err)
	}
	return s.putObject(ctx, objKey, data)
}

func (s *S3Store) flushHeaderChunk(ctx context.Context, cs uint64, newHeaders []*types.Header) error {
	objKey := s.key("headers", chunkFileName(cs))

	existing, err := s.getObject(ctx, objKey)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("read header chunk for merge: %w", err)
	}

	merged := make([]types.Header, 0, len(newHeaders))
	if existing != nil {
		merged, err = decodeS3Headers(existing)
		if err != nil {
			return fmt.Errorf("decode existing header chunk: %w", err)
		}
	}

	for _, h := range newHeaders {
		merged = append(merged, *h)
	}

	merged = deduplicateHeaders(merged)
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Height < merged[j].Height
	})

	data, err := encodeS3Headers(merged)
	if err != nil {
		return fmt.Errorf("encode header chunk: %w", err)
	}
	return s.putObject(ctx, objKey, data)
}

func (s *S3Store) flushCommitIndex(ctx context.Context, e commitEntry) error {
	key := s.key("index", "commitments", e.commitmentHex+".json")
	data, err := json.Marshal(e.pointer)
	if err != nil {
		return fmt.Errorf("encode commitment index: %w", err)
	}
	return s.putObject(ctx, key, data)
}

// --- Buffer lookup helpers ---

func (s *S3Store) findBlobInBuffer(ns types.Namespace, height uint64, index int) *types.Blob {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.findBlobInBufferLocked(ns, height, index)
}

func (s *S3Store) findBlobInBufferLocked(ns types.Namespace, height uint64, index int) *types.Blob {
	key := blobChunkKey{namespace: ns, chunkStart: s.chunkStart(height)}
	for i := range s.blobBuf[key] {
		b := &s.blobBuf[key][i]
		if b.Height == height && b.Index == index {
			cp := *b
			return &cp
		}
	}
	return nil
}

func (s *S3Store) findHeaderInBuffer(height uint64) *types.Header {
	s.mu.Lock()
	defer s.mu.Unlock()

	cs := s.chunkStart(height)
	for _, h := range s.headerBuf[cs] {
		if h.Height == height {
			cp := *h
			return &cp
		}
	}
	return nil
}

// --- S3 helpers ---

func (s *S3Store) getObject(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close() //nolint:errcheck
	return io.ReadAll(out.Body)
}

func (s *S3Store) putObject(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	return err
}

// isNotFound returns true if the error indicates an S3 NoSuchKey.
func isNotFound(err error) bool {
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	// Some S3-compatible services return NotFound via the HTTP status code
	// wrapped in a smithy-go OperationError rather than a typed NoSuchKey error.
	var nsb *s3types.NotFound
	return errors.As(err, &nsb)
}

// --- Encoding/decoding ---

func encodeS3Blobs(blobs []types.Blob) ([]byte, error) {
	out := make([]s3Blob, len(blobs))
	for i, b := range blobs {
		out[i] = s3Blob{
			Height:       b.Height,
			Namespace:    b.Namespace.String(),
			Data:         b.Data,
			Commitment:   b.Commitment,
			ShareVersion: b.ShareVersion,
			Signer:       b.Signer,
			Index:        b.Index,
		}
	}
	return json.Marshal(out)
}

func decodeS3Blobs(data []byte) ([]types.Blob, error) {
	var raw []s3Blob
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode blobs JSON: %w", err)
	}
	blobs := make([]types.Blob, len(raw))
	for i, r := range raw {
		ns, err := types.NamespaceFromHex(r.Namespace)
		if err != nil {
			return nil, fmt.Errorf("decode blob namespace: %w", err)
		}
		blobs[i] = types.Blob{
			Height:       r.Height,
			Namespace:    ns,
			Data:         r.Data,
			Commitment:   r.Commitment,
			ShareVersion: r.ShareVersion,
			Signer:       r.Signer,
			Index:        r.Index,
		}
	}
	return blobs, nil
}

func encodeS3Headers(headers []types.Header) ([]byte, error) {
	out := make([]s3Header, len(headers))
	for i, h := range headers {
		out[i] = s3Header{
			Height:    h.Height,
			Hash:      h.Hash,
			DataHash:  h.DataHash,
			Time:      h.Time,
			RawHeader: h.RawHeader,
		}
	}
	return json.Marshal(out)
}

func decodeS3Headers(data []byte) ([]types.Header, error) {
	var raw []s3Header
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode headers JSON: %w", err)
	}
	headers := make([]types.Header, len(raw))
	for i, r := range raw {
		headers[i] = types.Header{
			Height:    r.Height,
			Hash:      r.Hash,
			DataHash:  r.DataHash,
			Time:      r.Time,
			RawHeader: r.RawHeader,
		}
	}
	return headers, nil
}

// --- Deduplication ---

func deduplicateBlobs(blobs []types.Blob) []types.Blob {
	type blobKey struct {
		height    uint64
		namespace types.Namespace
		index     int
	}
	seen := make(map[blobKey]struct{}, len(blobs))
	out := make([]types.Blob, 0, len(blobs))
	for _, b := range blobs {
		k := blobKey{height: b.Height, namespace: b.Namespace, index: b.Index}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, b)
	}
	return out
}

func deduplicateHeaders(headers []types.Header) []types.Header {
	seen := make(map[uint64]struct{}, len(headers))
	out := make([]types.Header, 0, len(headers))
	for _, h := range headers {
		if _, ok := seen[h.Height]; ok {
			continue
		}
		seen[h.Height] = struct{}{}
		out = append(out, h)
	}
	return out
}
