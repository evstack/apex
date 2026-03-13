package submit

import (
	"encoding/json"
	"testing"

	"github.com/evstack/apex/pkg/types"
)

const testSignerAddress = "celestia1test"

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
	assertDecodedBlob(t, req, ns)
	assertDecodedOptions(t, req.Options)
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

func TestMarshalResultRejectsNil(t *testing.T) {
	if _, err := MarshalResult(nil); err == nil {
		t.Fatal("expected error for nil result")
	}
}

func testNamespace(b byte) types.Namespace {
	var ns types.Namespace
	ns[types.NamespaceSize-1] = b
	return ns
}

func assertDecodedBlob(t *testing.T, req *Request, wantNamespace types.Namespace) {
	t.Helper()

	if len(req.Blobs) != 1 {
		t.Fatalf("got %d blobs, want 1", len(req.Blobs))
	}
	if req.Blobs[0].Namespace != wantNamespace {
		t.Fatalf("namespace = %x, want %x", req.Blobs[0].Namespace, wantNamespace)
	}
	if string(req.Blobs[0].Data) != "hello" {
		t.Fatalf("data = %q, want %q", req.Blobs[0].Data, "hello")
	}
	if req.Blobs[0].ShareVersion != 1 {
		t.Fatalf("share version = %d, want 1", req.Blobs[0].ShareVersion)
	}
	if string(req.Blobs[0].Commitment) != "commitment" {
		t.Fatalf("commitment = %q, want %q", req.Blobs[0].Commitment, "commitment")
	}
	if string(req.Blobs[0].Signer) != "signer" {
		t.Fatalf("signer = %q, want %q", req.Blobs[0].Signer, "signer")
	}
	if req.Blobs[0].Index != -1 {
		t.Fatalf("index = %d, want -1", req.Blobs[0].Index)
	}
}

func assertDecodedOptions(t *testing.T, opts *TxConfig) {
	t.Helper()

	if opts == nil {
		t.Fatal("options = nil, want decoded options")
	}
	if opts.GasPrice != 0.25 || !opts.IsGasPriceSet {
		t.Fatalf("gas config = %#v, want gas price override", opts)
	}
	if opts.MaxGasPrice != 1.5 || opts.Gas != 1234 {
		t.Fatalf("gas bounds = %#v, want max gas price and explicit gas", opts)
	}
	if opts.TxPriority != PriorityHigh {
		t.Fatalf("priority = %d, want %d", opts.TxPriority, PriorityHigh)
	}
	if opts.SignerAddress != testSignerAddress {
		t.Fatalf("signer address = %q, want %q", opts.SignerAddress, testSignerAddress)
	}
}
