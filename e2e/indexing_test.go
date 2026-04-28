package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestIndexingQueryPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed e2e test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), chainStartupTimeout)
	defer cancel()

	grpcAddr, chainID, signerKeyHex, signerAddress := startSubmissionTestChain(t, ctx)
	namespace := testNamespace(t, []byte("apex-idx"))
	data := []byte("apex indexing query e2e")
	commitment := mustBlobCommitment(t, namespace, data)

	apexBinary := buildApexBinary(t)
	apexRPCAddr := reserveTCPAddr(t)
	apexGRPCAddr := reserveTCPAddr(t)
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
		GasPrice:        submissionGasPrice,
		MaxGasPrice:     submissionGasPrice,
		ConfirmTimeoutS: submissionConfirmTimeout,
	})

	proc := startApexProcess(t, apexBinary, configPath)
	defer proc.Stop(t)
	waitForApexHTTP(t, proc, apexRPCAddr)

	resp := doRPC(t, proc, apexRPCAddr, "blob.Submit",
		[]map[string]any{{
			"namespace":     namespace,
			"data":          data,
			"share_version": 0,
			"commitment":    commitment,
			"index":         -1,
		}},
		map[string]any{
			"gas_price":        submissionGasPrice,
			"is_gas_price_set": true,
		},
	)
	if resp.Error != nil {
		t.Fatalf("blob.Submit error: %s", resp.Error.Message)
	}
	height := decodeSubmitHeight(t, resp.Result)
	if height == 0 {
		t.Fatal("submission height must be positive")
	}

	waitForIndexedBlob(t, proc, apexRPCAddr, commitment, data, namespace, signerAddress)

	t.Run("blob.Get", func(t *testing.T) {
		r := doRPC(t, proc, apexRPCAddr, "blob.Get", height, namespace, commitment)
		if r.Error != nil {
			t.Fatalf("blob.Get error: %s", r.Error.Message)
		}
		var b rpcBlob
		if err := json.Unmarshal(r.Result, &b); err != nil {
			t.Fatalf("decode blob: %v", err)
		}
		if !bytes.Equal(b.Data, data) {
			t.Fatalf("data mismatch: got %q want %q", b.Data, data)
		}
		if !bytes.Equal(b.Commitment, commitment) {
			t.Fatalf("commitment mismatch: got %x want %x", b.Commitment, commitment)
		}
	})

	t.Run("blob.GetAll", func(t *testing.T) {
		r := doRPC(t, proc, apexRPCAddr, "blob.GetAll", height, [][]byte{namespace})
		if r.Error != nil {
			t.Fatalf("blob.GetAll error: %s", r.Error.Message)
		}
		var blobs []rpcBlob
		if err := json.Unmarshal(r.Result, &blobs); err != nil {
			t.Fatalf("decode blobs: %v", err)
		}
		if len(blobs) == 0 {
			t.Fatal("expected at least one blob from GetAll")
		}
		found := false
		for _, b := range blobs {
			if bytes.Equal(b.Commitment, commitment) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("blob with commitment %x not found in GetAll result", commitment)
		}
	})

	t.Run("header.GetByHeight", func(t *testing.T) {
		r := doRPC(t, proc, apexRPCAddr, "header.GetByHeight", height)
		if r.Error != nil {
			t.Fatalf("header.GetByHeight error: %s", r.Error.Message)
		}
		if len(r.Result) == 0 || string(r.Result) == "null" {
			t.Fatal("expected non-null header")
		}
	})

	t.Run("header.LocalHead", func(t *testing.T) {
		r := doRPC(t, proc, apexRPCAddr, "header.LocalHead")
		if r.Error != nil {
			t.Fatalf("header.LocalHead error: %s", r.Error.Message)
		}
		if len(r.Result) == 0 || string(r.Result) == "null" {
			t.Fatal("expected non-null local head")
		}
	})
}
