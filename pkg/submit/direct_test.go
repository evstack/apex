package submit

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	if typeID != protoBlobTxTypeID {
		t.Fatalf("blob tx type_id = %q, want %q", typeID, protoBlobTxTypeID)
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
	if client.accountCalls != 2 {
		t.Fatalf("account calls = %d, want 2", client.accountCalls)
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
