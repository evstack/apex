package submit

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	txv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/tx/v1beta1"
	"github.com/evstack/apex/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type fakeAppClient struct {
	accountInfos      []*AccountInfo
	accountErrs       []error
	broadcastStatuses []*TxStatus
	broadcastErrs     []error
	getTxStatuses     []*TxStatus
	getTxErrs         []error
	broadcastRequests [][]byte
	accountCalls      int
	broadcastCalls    int
	getTxCalls        int
}

func (f *fakeAppClient) AccountInfo(_ context.Context, _ string) (*AccountInfo, error) {
	call := f.accountCalls
	f.accountCalls++
	if err := queueErr(f.accountErrs, call); err != nil {
		return nil, err
	}
	return queueAccount(f.accountInfos, call), nil
}

func (f *fakeAppClient) BroadcastTx(_ context.Context, txBytes []byte) (*TxStatus, error) {
	call := f.broadcastCalls
	f.broadcastCalls++
	f.broadcastRequests = append(f.broadcastRequests, append([]byte(nil), txBytes...))
	if err := queueErr(f.broadcastErrs, call); err != nil {
		return nil, err
	}
	return queueTxStatus(f.broadcastStatuses, call), nil
}

func (f *fakeAppClient) GetTx(_ context.Context, _ string) (*TxStatus, error) {
	call := f.getTxCalls
	f.getTxCalls++
	if err := queueErr(f.getTxErrs, call); err != nil {
		return nil, err
	}
	if len(f.getTxStatuses) == 0 && len(f.getTxErrs) > 0 {
		return nil, f.getTxErrs[len(f.getTxErrs)-1]
	}
	return queueTxStatus(f.getTxStatuses, call), nil
}

func (*fakeAppClient) Close() error {
	return nil
}

func TestDirectSubmitterSubmit(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t)
	client := &fakeAppClient{
		accountInfos: []*AccountInfo{{
			Address:       signer.Address(),
			AccountNumber: 7,
			Sequence:      1,
		}},
		broadcastStatuses: []*TxStatus{{
			Hash: "ABC123",
		}},
		getTxErrs: []error{
			status.Error(codes.NotFound, "not found"),
		},
		getTxStatuses: []*TxStatus{{
			Hash:   "ABC123",
			Height: 55,
		}},
	}

	submitter, err := NewDirectSubmitter(client, signer, DirectConfig{
		ChainID:             "mocha-4",
		GasPrice:            0.002,
		MaxGasPrice:         0.01,
		ConfirmationTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDirectSubmitter: %v", err)
	}
	submitter.pollInterval = time.Millisecond

	result, err := submitter.Submit(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if result.Height != 55 {
		t.Fatalf("height = %d, want 55", result.Height)
	}
	if client.broadcastCalls != 1 {
		t.Fatalf("broadcast calls = %d, want 1", client.broadcastCalls)
	}
	_, _, typeID := decodeBlobTxEnvelope(t, client.broadcastRequests[0])
	if typeID != types.ProtoBlobTxTypeID {
		t.Fatalf("blob tx type_id = %q, want %q", typeID, types.ProtoBlobTxTypeID)
	}
}

func TestDirectSubmitterRetriesSequenceMismatch(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t)
	client := &fakeAppClient{
		accountInfos: []*AccountInfo{
			{Address: signer.Address(), AccountNumber: 7, Sequence: 1},
			{Address: signer.Address(), AccountNumber: 7, Sequence: 2},
		},
		broadcastStatuses: []*TxStatus{
			{Code: 32, RawLog: "account sequence mismatch, expected 2, got 1"},
			{Hash: "DEF456"},
		},
		getTxStatuses: []*TxStatus{{
			Hash:   "DEF456",
			Height: 77,
		}},
	}

	submitter, err := NewDirectSubmitter(client, signer, DirectConfig{
		ChainID:             "mocha-4",
		GasPrice:            0.002,
		ConfirmationTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDirectSubmitter: %v", err)
	}
	submitter.pollInterval = time.Millisecond

	result, err := submitter.Submit(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if result.Height != 77 {
		t.Fatalf("height = %d, want 77", result.Height)
	}
	if client.accountCalls != 1 {
		t.Fatalf("account calls = %d, want 1", client.accountCalls)
	}
	if client.broadcastCalls != 2 {
		t.Fatalf("broadcast calls = %d, want 2", client.broadcastCalls)
	}
}

func TestDirectSubmitterRejectsSignerMismatch(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t)
	client := &fakeAppClient{
		accountInfos: []*AccountInfo{{
			Address:       signer.Address(),
			AccountNumber: 7,
			Sequence:      1,
		}},
	}

	submitter, err := NewDirectSubmitter(client, signer, DirectConfig{
		ChainID:             "mocha-4",
		GasPrice:            0.002,
		ConfirmationTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewDirectSubmitter: %v", err)
	}

	_, err = submitter.Submit(context.Background(), &Request{
		Blobs: testRequest().Blobs,
		Options: &TxConfig{
			SignerAddress: "celestia1different",
		},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected signer mismatch error, got: %v", err)
	}
	if client.broadcastCalls != 0 {
		t.Fatalf("broadcast calls = %d, want 0", client.broadcastCalls)
	}
}

func TestDirectSubmitterRejectsPerRequestMaxGasPrice(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t)
	client := &fakeAppClient{
		accountInfos: []*AccountInfo{{
			Address:       signer.Address(),
			AccountNumber: 7,
			Sequence:      1,
		}},
	}

	submitter, err := NewDirectSubmitter(client, signer, DirectConfig{
		ChainID:             "mocha-4",
		GasPrice:            0.002,
		MaxGasPrice:         0.01,
		ConfirmationTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewDirectSubmitter: %v", err)
	}

	_, err = submitter.Submit(context.Background(), &Request{
		Blobs: testRequest().Blobs,
		Options: &TxConfig{
			GasPrice:      0.002,
			IsGasPriceSet: true,
			MaxGasPrice:   0.001,
		},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds the max gas price") {
		t.Fatalf("expected max gas price error, got: %v", err)
	}
	if client.broadcastCalls != 0 {
		t.Fatalf("broadcast calls = %d, want 0", client.broadcastCalls)
	}
}

func TestDirectSubmitterConfirmationTimeout(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t)
	client := &fakeAppClient{
		accountInfos: []*AccountInfo{{
			Address:       signer.Address(),
			AccountNumber: 7,
			Sequence:      1,
		}},
		broadcastStatuses: []*TxStatus{{
			Hash: "ABC123",
		}},
		getTxErrs: []error{
			status.Error(codes.NotFound, "not found"),
		},
	}

	submitter, err := NewDirectSubmitter(client, signer, DirectConfig{
		ChainID:             "mocha-4",
		GasPrice:            0.002,
		ConfirmationTimeout: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDirectSubmitter: %v", err)
	}
	submitter.pollInterval = time.Millisecond

	_, err = submitter.Submit(context.Background(), testRequest())
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
}

func TestDirectSubmitterSpamReservesSequencesBeforeConfirmation(t *testing.T) {
	t.Parallel()

	const submissions = 6

	signer := mustSigner(t)
	client := newSequenceSpamAppClient(signer.Address(), 7, 11, submissions)

	submitter, err := NewDirectSubmitter(client, signer, DirectConfig{
		ChainID:             "mocha-4",
		GasPrice:            0.002,
		ConfirmationTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDirectSubmitter: %v", err)
	}
	submitter.pollInterval = time.Millisecond

	var (
		wg      sync.WaitGroup
		start   = make(chan struct{})
		errs    = make(chan error, submissions)
		heights = make(chan uint64, submissions)
	)

	for range submissions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			result, submitErr := submitter.Submit(context.Background(), testRequest())
			if submitErr != nil {
				errs <- submitErr
				return
			}
			heights <- result.Height
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	close(heights)

	if len(errs) > 0 {
		t.Fatalf("unexpected submit error: %v", <-errs)
	}
	if client.accountCalls != 1 {
		t.Fatalf("account calls = %d, want 1", client.accountCalls)
	}
	if client.broadcastCalls != submissions {
		t.Fatalf("broadcast calls = %d, want %d", client.broadcastCalls, submissions)
	}

	wantSequences := []uint64{11, 12, 13, 14, 15, 16}
	if !slices.Equal(client.sequences, wantSequences) {
		t.Fatalf("broadcast sequences = %v, want %v", client.sequences, wantSequences)
	}

	if got := len(heights); got != submissions {
		t.Fatalf("confirmed results = %d, want %d", got, submissions)
	}
}

func TestDirectSubmitterRecoversAfterRestartWithPendingSequences(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t)
	client := newSequenceRecoveryAppClient(signer.Address(), 7, 11, 16)

	// Simulate a fresh process with no local sequence cache while the mempool
	// still holds earlier pending transactions.
	submitter, err := NewDirectSubmitter(client, signer, DirectConfig{
		ChainID:             "mocha-4",
		GasPrice:            0.002,
		ConfirmationTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDirectSubmitter: %v", err)
	}
	submitter.pollInterval = time.Millisecond

	result, err := submitter.Submit(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if result.Height != 116 {
		t.Fatalf("height = %d, want 116", result.Height)
	}
	if client.accountCalls != 1 {
		t.Fatalf("account calls = %d, want 1", client.accountCalls)
	}
	if !slices.Equal(client.attemptSequences, []uint64{11, 16}) {
		t.Fatalf("attempt sequences = %v, want [11 16]", client.attemptSequences)
	}
	if client.lastSuccessfulSequence() != 16 {
		t.Fatalf("successful sequence = %d, want 16", client.lastSuccessfulSequence())
	}
}

func TestDirectSubmitterRecoversWhenCachedSequenceFallsBehindExternalWriter(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t)
	client := newSequenceRecoveryAppClient(signer.Address(), 7, 16, 16)
	client.afterSuccessNext = []uint64{19}

	submitter, err := NewDirectSubmitter(client, signer, DirectConfig{
		ChainID:             "mocha-4",
		GasPrice:            0.002,
		ConfirmationTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDirectSubmitter: %v", err)
	}
	submitter.pollInterval = time.Millisecond

	first, err := submitter.Submit(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	if first.Height != 116 {
		t.Fatalf("first height = %d, want 116", first.Height)
	}

	second, err := submitter.Submit(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("second Submit: %v", err)
	}
	if second.Height != 119 {
		t.Fatalf("second height = %d, want 119", second.Height)
	}
	if client.accountCalls != 1 {
		t.Fatalf("account calls = %d, want 1", client.accountCalls)
	}
	if !slices.Equal(client.attemptSequences, []uint64{16, 17, 19}) {
		t.Fatalf("attempt sequences = %v, want [16 17 19]", client.attemptSequences)
	}
}

func TestDirectSubmitterReconcilesPersistedPendingSequenceBeforeBroadcast(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t)
	client := &persistedPendingReconcileAppClient{
		address:                   signer.Address(),
		accountNumber:             7,
		committedSequence:         11,
		pendingHash:               "tx-11",
		expectedBroadcastSequence: 12,
	}

	submitter, err := NewDirectSubmitter(client, signer, DirectConfig{
		ChainID:             "mocha-4",
		GasPrice:            0.002,
		ConfirmationTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDirectSubmitter: %v", err)
	}
	submitter.pollInterval = time.Millisecond
	submitter.pendingSequences = map[string]uint64{"tx-11": 11}

	result, err := submitter.Submit(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if result.Height != 112 {
		t.Fatalf("height = %d, want 112", result.Height)
	}
	if !slices.Equal(client.getTxHashes, []string{"tx-11", "tx-12"}) {
		t.Fatalf("GetTx hashes = %v, want [tx-11 tx-12]", client.getTxHashes)
	}
	if !slices.Equal(client.attemptSequences, []uint64{12}) {
		t.Fatalf("attempt sequences = %v, want [12]", client.attemptSequences)
	}
}

func TestDirectSubmitterRejectsWhenMaxInFlightExceeded(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t)
	client := newInFlightLimitAppClient(signer.Address(), 7, 1)

	submitter, err := NewDirectSubmitter(client, signer, DirectConfig{
		ChainID:             "mocha-4",
		GasPrice:            0.002,
		ConfirmationTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewDirectSubmitter: %v", err)
	}
	submitter.pollInterval = time.Millisecond
	submitter.maxInFlight = 1

	firstDone := make(chan error, 1)
	go func() {
		_, submitErr := submitter.Submit(context.Background(), testRequest())
		firstDone <- submitErr
	}()

	client.waitForBroadcast(t)

	secondCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = submitter.Submit(secondCtx, testRequest())
	if err == nil {
		t.Fatal("expected max in-flight error, got nil")
	}
	if !strings.Contains(err.Error(), "too many in-flight") {
		t.Fatalf("error = %v, want max in-flight rejection", err)
	}
	if client.broadcastCount() != 1 {
		t.Fatalf("broadcast calls = %d, want 1", client.broadcastCount())
	}

	close(client.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Submit: %v", err)
	}
}

func mustSigner(t *testing.T) *Signer {
	t.Helper()

	signer, err := LoadSigner(writeSignerKey(t, testSignerKeyHex))
	if err != nil {
		t.Fatalf("LoadSigner: %v", err)
	}
	return signer
}

func testRequest() *Request {
	return &Request{
		Blobs: []Blob{{
			Namespace:    testNamespace(1),
			Data:         []byte("hello world"),
			ShareVersion: 0,
			Commitment:   []byte("commitment-1"),
		}},
	}
}

func queueErr(errs []error, idx int) error {
	if len(errs) == 0 {
		return nil
	}
	if idx >= len(errs) {
		return nil
	}
	return errs[idx]
}

func queueAccount(accounts []*AccountInfo, idx int) *AccountInfo {
	if len(accounts) == 0 {
		return nil
	}
	if idx >= len(accounts) {
		return accounts[len(accounts)-1]
	}
	return accounts[idx]
}

func queueTxStatus(statuses []*TxStatus, idx int) *TxStatus {
	if len(statuses) == 0 {
		return nil
	}
	if idx >= len(statuses) {
		return statuses[len(statuses)-1]
	}
	return statuses[idx]
}

type sequenceSpamAppClient struct {
	address       string
	accountNumber uint64
	baseSequence  uint64
	total         int

	mu             sync.Mutex
	sequences      []uint64
	hashes         map[string]uint64
	accountCalls   int
	broadcastCalls int
}

func newSequenceSpamAppClient(address string, accountNumber, baseSequence uint64, total int) *sequenceSpamAppClient {
	return &sequenceSpamAppClient{
		address:       address,
		accountNumber: accountNumber,
		baseSequence:  baseSequence,
		total:         total,
		hashes:        make(map[string]uint64, total),
	}
}

func (c *sequenceSpamAppClient) AccountInfo(_ context.Context, _ string) (*AccountInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.accountCalls++
	return &AccountInfo{
		Address:       c.address,
		AccountNumber: c.accountNumber,
		Sequence:      c.baseSequence,
	}, nil
}

func (c *sequenceSpamAppClient) BroadcastTx(_ context.Context, txBytes []byte) (*TxStatus, error) {
	sequence, err := decodeSequenceFromBlobTx(txBytes)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.broadcastCalls++
	expected := c.baseSequence + uint64(len(c.sequences))
	if sequence != expected {
		return &TxStatus{
			Code:   32,
			RawLog: fmt.Sprintf("account sequence mismatch, expected %d, got %d", expected, sequence),
		}, nil
	}

	hash := fmt.Sprintf("tx-%d", sequence)
	c.sequences = append(c.sequences, sequence)
	c.hashes[hash] = sequence
	return &TxStatus{Hash: hash}, nil
}

func (c *sequenceSpamAppClient) GetTx(_ context.Context, hash string) (*TxStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sequence, ok := c.hashes[hash]
	if !ok || len(c.sequences) < c.total {
		return nil, status.Error(codes.NotFound, "not found")
	}

	return &TxStatus{
		Hash:   hash,
		Height: int64(100 + sequence),
	}, nil
}

func (*sequenceSpamAppClient) Close() error {
	return nil
}

func decodeSequenceFromBlobTx(raw []byte) (uint64, error) {
	innerTx, err := decodeInnerTx(raw)
	if err != nil {
		return 0, err
	}

	var txRaw txv1beta1.TxRaw
	if err := proto.Unmarshal(innerTx, &txRaw); err != nil {
		return 0, fmt.Errorf("unmarshal tx raw: %w", err)
	}

	var authInfo txv1beta1.AuthInfo
	if err := proto.Unmarshal(txRaw.GetAuthInfoBytes(), &authInfo); err != nil {
		return 0, fmt.Errorf("unmarshal auth info: %w", err)
	}

	signerInfos := authInfo.GetSignerInfos()
	if len(signerInfos) != 1 {
		return 0, fmt.Errorf("signer infos = %d, want 1", len(signerInfos))
	}

	return signerInfos[0].GetSequence(), nil
}

func decodeInnerTx(raw []byte) ([]byte, error) {
	data := raw
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, errors.New("decode blob tx tag")
		}
		data = data[n:]
		if typ != protowire.BytesType {
			return nil, fmt.Errorf("unexpected wire type %d for field %d", typ, num)
		}

		value, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, fmt.Errorf("decode blob tx field %d", num)
		}
		data = data[n:]

		if num == 1 {
			return value, nil
		}
	}

	return nil, errors.New("blob tx inner tx missing")
}

type sequenceRecoveryAppClient struct {
	address           string
	accountNumber     uint64
	committedSequence uint64
	nextAvailable     uint64
	afterSuccessNext  []uint64

	mu               sync.Mutex
	accountCalls     int
	attemptSequences []uint64
	successHashes    map[string]uint64
	successCount     int
}

func newSequenceRecoveryAppClient(address string, accountNumber, committedSequence, nextAvailable uint64) *sequenceRecoveryAppClient {
	return &sequenceRecoveryAppClient{
		address:           address,
		accountNumber:     accountNumber,
		committedSequence: committedSequence,
		nextAvailable:     nextAvailable,
		successHashes:     make(map[string]uint64),
	}
}

func (c *sequenceRecoveryAppClient) AccountInfo(_ context.Context, _ string) (*AccountInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.accountCalls++
	return &AccountInfo{
		Address:       c.address,
		AccountNumber: c.accountNumber,
		Sequence:      c.committedSequence,
	}, nil
}

func (c *sequenceRecoveryAppClient) BroadcastTx(_ context.Context, txBytes []byte) (*TxStatus, error) {
	sequence, err := decodeSequenceFromBlobTx(txBytes)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.attemptSequences = append(c.attemptSequences, sequence)
	if sequence != c.nextAvailable {
		return &TxStatus{
			Code:   32,
			RawLog: fmt.Sprintf("account sequence mismatch, expected %d, got %d", c.nextAvailable, sequence),
		}, nil
	}

	hash := fmt.Sprintf("tx-%d", sequence)
	c.successHashes[hash] = sequence
	c.successCount++
	if c.successCount <= len(c.afterSuccessNext) {
		c.nextAvailable = c.afterSuccessNext[c.successCount-1]
	} else {
		c.nextAvailable = sequence + 1
	}

	return &TxStatus{Hash: hash}, nil
}

func (c *sequenceRecoveryAppClient) GetTx(_ context.Context, hash string) (*TxStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sequence, ok := c.successHashes[hash]
	if !ok {
		return nil, status.Error(codes.NotFound, "not found")
	}

	return &TxStatus{
		Hash:   hash,
		Height: int64(100 + sequence),
	}, nil
}

func (c *sequenceRecoveryAppClient) lastSuccessfulSequence() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	var last uint64
	for _, sequence := range c.successHashes {
		if sequence > last {
			last = sequence
		}
	}
	return last
}

func (*sequenceRecoveryAppClient) Close() error {
	return nil
}

type persistedPendingReconcileAppClient struct {
	address                   string
	accountNumber             uint64
	committedSequence         uint64
	pendingHash               string
	expectedBroadcastSequence uint64

	mu               sync.Mutex
	getTxHashes      []string
	attemptSequences []uint64
}

func (c *persistedPendingReconcileAppClient) AccountInfo(_ context.Context, _ string) (*AccountInfo, error) {
	return &AccountInfo{
		Address:       c.address,
		AccountNumber: c.accountNumber,
		Sequence:      c.committedSequence,
	}, nil
}

func (c *persistedPendingReconcileAppClient) BroadcastTx(_ context.Context, txBytes []byte) (*TxStatus, error) {
	sequence, err := decodeSequenceFromBlobTx(txBytes)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.attemptSequences = append(c.attemptSequences, sequence)
	if sequence != c.expectedBroadcastSequence {
		return &TxStatus{
			Code:   32,
			RawLog: fmt.Sprintf("account sequence mismatch, expected %d, got %d", c.expectedBroadcastSequence, sequence),
		}, nil
	}

	return &TxStatus{Hash: fmt.Sprintf("tx-%d", sequence)}, nil
}

func (c *persistedPendingReconcileAppClient) GetTx(_ context.Context, hash string) (*TxStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.getTxHashes = append(c.getTxHashes, hash)
	switch hash {
	case c.pendingHash:
		return nil, status.Error(codes.NotFound, "not found")
	case fmt.Sprintf("tx-%d", c.expectedBroadcastSequence):
		return &TxStatus{
			Hash:   hash,
			Height: int64(100 + c.expectedBroadcastSequence),
		}, nil
	default:
		return nil, status.Error(codes.NotFound, "not found")
	}
}

func (*persistedPendingReconcileAppClient) Close() error {
	return nil
}

type inFlightLimitAppClient struct {
	address       string
	accountNumber uint64
	baseSequence  uint64

	release     chan struct{}
	broadcasted chan struct{}

	mu             sync.Mutex
	broadcastCalls int
	hashes         map[string]uint64
}

func newInFlightLimitAppClient(address string, accountNumber, baseSequence uint64) *inFlightLimitAppClient {
	return &inFlightLimitAppClient{
		address:       address,
		accountNumber: accountNumber,
		baseSequence:  baseSequence,
		release:       make(chan struct{}),
		broadcasted:   make(chan struct{}, 1),
		hashes:        make(map[string]uint64),
	}
}

func (c *inFlightLimitAppClient) AccountInfo(_ context.Context, _ string) (*AccountInfo, error) {
	return &AccountInfo{
		Address:       c.address,
		AccountNumber: c.accountNumber,
		Sequence:      c.baseSequence,
	}, nil
}

func (c *inFlightLimitAppClient) BroadcastTx(_ context.Context, txBytes []byte) (*TxStatus, error) {
	sequence, err := decodeSequenceFromBlobTx(txBytes)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	hash := fmt.Sprintf("tx-%d", sequence)
	c.broadcastCalls++
	c.hashes[hash] = sequence
	select {
	case c.broadcasted <- struct{}{}:
	default:
	}
	return &TxStatus{Hash: hash}, nil
}

func (c *inFlightLimitAppClient) GetTx(_ context.Context, hash string) (*TxStatus, error) {
	c.mu.Lock()
	sequence, ok := c.hashes[hash]
	c.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "not found")
	}

	select {
	case <-c.release:
		return &TxStatus{
			Hash:   hash,
			Height: int64(100 + sequence),
		}, nil
	default:
		return nil, status.Error(codes.NotFound, "not found")
	}
}

func (*inFlightLimitAppClient) Close() error {
	return nil
}

func (c *inFlightLimitAppClient) waitForBroadcast(t *testing.T) {
	t.Helper()

	select {
	case <-c.broadcasted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first broadcast")
	}
}

func (c *inFlightLimitAppClient) broadcastCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.broadcastCalls
}
