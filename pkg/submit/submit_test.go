package submit

import (
	"encoding/json"
	"testing"

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

func TestMarshalResultRejectsNil(t *testing.T) {
	t.Parallel()

	if _, err := MarshalResult(nil); err == nil {
		t.Fatal("expected error for nil result")
	}
}

func testNamespace(b byte) types.Namespace {
	var ns types.Namespace
	ns[types.NamespaceSize-1] = b
	return ns
}
