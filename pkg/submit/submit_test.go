package submit

import (
	"encoding/json"
	"testing"

	gsquare "github.com/celestiaorg/go-square/v3/share"
	"github.com/evstack/apex/pkg/types"
)

func TestDecodeRequestConvertsNamespace(t *testing.T) {
	t.Parallel()

	ns := testNamespace(7)
	blobsRaw, err := json.Marshal([]map[string]any{{
		"namespace":     ns[:],
		"data":          []byte("hello"),
		"share_version": 0,
		"commitment":    []byte("c1"),
		"index":         0,
	}})
	if err != nil {
		t.Fatalf("marshal blobs: %v", err)
	}

	req, err := DecodeRequest(blobsRaw, nil)
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	if len(req.Blobs) != 1 {
		t.Fatalf("got %d blobs, want 1", len(req.Blobs))
	}
	if req.Blobs[0].Namespace != ns {
		t.Fatalf("namespace = %x, want %x", req.Blobs[0].Namespace, ns)
	}
}

func TestDecodeRequestRejectsInvalidNamespace(t *testing.T) {
	t.Parallel()

	// AQI= is base64 for [1, 2] — only 2 bytes, not 29.
	blobsRaw := json.RawMessage(`[{"namespace":"AQI=","data":"aGVsbG8=","share_version":0,"commitment":"YzE=","index":0}]`)

	_, err := DecodeRequest(blobsRaw, nil)
	if err == nil {
		t.Fatal("expected error for short namespace")
	}
}

func TestDecodeRequestRejectsReservedNamespace(t *testing.T) {
	t.Parallel()

	ns := reservedNamespace()
	blobsRaw, err := json.Marshal([]map[string]any{{
		"namespace":     ns[:],
		"data":          []byte("hello"),
		"share_version": 0,
		"commitment":    []byte("c1"),
		"index":         0,
	}})
	if err != nil {
		t.Fatalf("marshal blobs: %v", err)
	}

	_, err = DecodeRequest(blobsRaw, nil)
	if err == nil {
		t.Fatal("expected error for reserved namespace")
	}
}

func TestDecodeRequestRejectsShareVersionOneWithoutSigner(t *testing.T) {
	t.Parallel()

	ns := testNamespace(7)
	blobsRaw, err := json.Marshal([]map[string]any{{
		"namespace":     ns[:],
		"data":          []byte("hello"),
		"share_version": 1,
		"commitment":    []byte("c1"),
		"index":         0,
	}})
	if err != nil {
		t.Fatalf("marshal blobs: %v", err)
	}

	_, err = DecodeRequest(blobsRaw, nil)
	if err == nil {
		t.Fatal("expected error for share version one without signer")
	}
}

func TestDecodeRequestAcceptsShareVersionOneSigner(t *testing.T) {
	t.Parallel()

	ns := testNamespace(8)
	wantSigner := testBlobSigner()
	blobsRaw, err := json.Marshal([]map[string]any{{
		"namespace":     ns[:],
		"data":          []byte("hello"),
		"share_version": 1,
		"commitment":    []byte("c1"),
		"signer":        wantSigner,
		"index":         0,
	}})
	if err != nil {
		t.Fatalf("marshal blobs: %v", err)
	}

	req, err := DecodeRequest(blobsRaw, nil)
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	if string(req.Blobs[0].Signer) != string(wantSigner) {
		t.Fatalf("signer = %x, want %x", req.Blobs[0].Signer, wantSigner)
	}
}

func TestDecodeRequestTreatsWhitespaceNullOptionsAsNil(t *testing.T) {
	t.Parallel()

	ns := testNamespace(7)
	blobsRaw, err := json.Marshal([]map[string]any{{
		"namespace":     ns[:],
		"data":          []byte("hello"),
		"share_version": 0,
		"commitment":    []byte("c1"),
		"index":         0,
	}})
	if err != nil {
		t.Fatalf("marshal blobs: %v", err)
	}

	req, err := DecodeRequest(blobsRaw, json.RawMessage(" \n null \t "))
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	if req.Options != nil {
		t.Fatalf("options = %#v, want nil", req.Options)
	}
}

func TestDecodeRequestRejectsNegativeMaxGasPrice(t *testing.T) {
	t.Parallel()

	ns := testNamespace(7)
	blobsRaw, err := json.Marshal([]map[string]any{{
		"namespace":     ns[:],
		"data":          []byte("hello"),
		"share_version": 0,
		"commitment":    []byte("c1"),
		"index":         0,
	}})
	if err != nil {
		t.Fatalf("marshal blobs: %v", err)
	}

	_, err = DecodeRequest(blobsRaw, json.RawMessage(`{"max_gas_price":-1}`))
	if err == nil {
		t.Fatal("expected error for negative max_gas_price")
	}
}

func TestDecodeRequestRejectsInvalidPriority(t *testing.T) {
	t.Parallel()

	ns := testNamespace(7)
	blobsRaw, err := json.Marshal([]map[string]any{{
		"namespace":     ns[:],
		"data":          []byte("hello"),
		"share_version": 0,
		"commitment":    []byte("c1"),
		"index":         0,
	}})
	if err != nil {
		t.Fatalf("marshal blobs: %v", err)
	}

	_, err = DecodeRequest(blobsRaw, json.RawMessage(`{"tx_priority":99}`))
	if err == nil {
		t.Fatal("expected error for invalid tx_priority")
	}
}

func TestMarshalResultRejectsNil(t *testing.T) {
	t.Parallel()

	if _, err := MarshalResult(nil); err == nil {
		t.Fatal("expected error for nil result")
	}
}

func testNamespace(b byte) types.Namespace {
	namespace := gsquare.MustNewV0Namespace([]byte("apexns" + string([]byte{b})))
	var ns types.Namespace
	copy(ns[:], namespace.Bytes())
	return ns
}

func reservedNamespace() types.Namespace {
	var ns types.Namespace
	ns[types.NamespaceSize-1] = 1
	return ns
}

func testBlobSigner() []byte {
	return []byte("01234567890123456789")
}
