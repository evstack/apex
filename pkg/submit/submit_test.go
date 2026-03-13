package submit

import (
	"encoding/json"
	"testing"

	"github.com/evstack/apex/pkg/types"
)

func TestDecodeRequest(t *testing.T) {
	ns := testNamespace(7)
	blobsRaw, err := json.Marshal([]map[string]any{{
		"namespace":     ns[:],
		"data":          []byte("hello"),
		"share_version": 1,
		"commitment":    []byte("commitment"),
		"signer":        []byte("signer"),
		"index":         -1,
	}})
	if err != nil {
		t.Fatalf("marshal blobs: %v", err)
	}
	optsRaw, err := json.Marshal(map[string]any{
		"gas_price":        0.25,
		"is_gas_price_set": true,
		"max_gas_price":    1.5,
		"gas":              1234,
		"tx_priority":      int(PriorityHigh),
		"signer_address":   "celestia1test",
	})
	if err != nil {
		t.Fatalf("marshal options: %v", err)
	}

	req, err := DecodeRequest(blobsRaw, optsRaw)
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	if len(req.Blobs) != 1 {
		t.Fatalf("got %d blobs, want 1", len(req.Blobs))
	}
	if req.Blobs[0].Namespace != ns {
		t.Fatalf("namespace = %x, want %x", req.Blobs[0].Namespace, ns)
	}
	if req.Blobs[0].ShareVersion != 1 {
		t.Fatalf("share version = %d, want 1", req.Blobs[0].ShareVersion)
	}
	if req.Options == nil {
		t.Fatal("options = nil, want decoded options")
	}
	if req.Options.TxPriority != PriorityHigh {
		t.Fatalf("priority = %d, want %d", req.Options.TxPriority, PriorityHigh)
	}
	if req.Options.SignerAddress != "celestia1test" {
		t.Fatalf("signer address = %q, want %q", req.Options.SignerAddress, "celestia1test")
	}
}

func TestDecodeRequestNilOptions(t *testing.T) {
	ns := testNamespace(1)
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

	req, err := DecodeRequest(blobsRaw, json.RawMessage("null"))
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	if req.Options != nil {
		t.Fatalf("options = %#v, want nil", req.Options)
	}
}

func TestDecodeRequestRejectsInvalidNamespace(t *testing.T) {
	blobsRaw := json.RawMessage(`[{"namespace":"AQI=","data":"aGVsbG8=","share_version":0,"commitment":"YzE=","index":0}]`)

	_, err := DecodeRequest(blobsRaw, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestMarshalResult(t *testing.T) {
	raw, err := MarshalResult(&Result{Height: 42})
	if err != nil {
		t.Fatalf("MarshalResult: %v", err)
	}
	if string(raw) != "42" {
		t.Fatalf("raw = %s, want 42", raw)
	}
}

func testNamespace(b byte) types.Namespace {
	var ns types.Namespace
	ns[types.NamespaceSize-1] = b
	return ns
}
