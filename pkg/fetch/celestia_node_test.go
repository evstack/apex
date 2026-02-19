package fetch

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/evstack/apex/pkg/types"
)

func TestMapHeader(t *testing.T) {
	raw := json.RawMessage(`{
		"header": {
			"height": "12345",
			"time": "2025-06-15T10:30:00.123456789Z",
			"data_hash": "AABBCCDD"
		},
		"commit": {
			"block_id": {
				"hash": "11223344"
			}
		}
	}`)

	hdr, err := mapHeader(raw)
	if err != nil {
		t.Fatalf("mapHeader: %v", err)
	}

	if hdr.Height != 12345 {
		t.Errorf("Height = %d, want 12345", hdr.Height)
	}
	wantTime := time.Date(2025, 6, 15, 10, 30, 0, 123456789, time.UTC)
	if !hdr.Time.Equal(wantTime) {
		t.Errorf("Time = %v, want %v", hdr.Time, wantTime)
	}
	if len(hdr.Hash) != 4 {
		t.Errorf("Hash len = %d, want 4", len(hdr.Hash))
	}
	if hdr.Hash[0] != 0x11 || hdr.Hash[1] != 0x22 {
		t.Errorf("Hash = %x, want 11223344", hdr.Hash)
	}
	if len(hdr.DataHash) != 4 {
		t.Errorf("DataHash len = %d, want 4", len(hdr.DataHash))
	}
	if hdr.DataHash[0] != 0xAA || hdr.DataHash[1] != 0xBB {
		t.Errorf("DataHash = %x, want AABBCCDD", hdr.DataHash)
	}
	if len(hdr.RawHeader) == 0 {
		t.Error("RawHeader is empty")
	}
}

func TestMapHeaderNumericHeight(t *testing.T) {
	// Some nodes may return height as a number instead of string.
	raw := json.RawMessage(`{
		"header": {
			"height": 42,
			"time": "2025-01-01T00:00:00Z",
			"data_hash": ""
		},
		"commit": {
			"block_id": {
				"hash": ""
			}
		}
	}`)

	hdr, err := mapHeader(raw)
	if err != nil {
		t.Fatalf("mapHeader: %v", err)
	}
	if hdr.Height != 42 {
		t.Errorf("Height = %d, want 42", hdr.Height)
	}
}

func TestMapBlobs(t *testing.T) {
	// Construct a 29-byte namespace, base64-encoded.
	var ns types.Namespace
	ns[0] = 0x00
	ns[types.NamespaceSize-1] = 0x01
	nsJSON, _ := json.Marshal(ns[:])

	raw := json.RawMessage(`[
		{
			"namespace": ` + string(nsJSON) + `,
			"data": "aGVsbG8=",
			"share_version": 0,
			"commitment": "Y29tbWl0",
			"index": 0
		},
		{
			"namespace": ` + string(nsJSON) + `,
			"data": "d29ybGQ=",
			"share_version": 1,
			"commitment": "Y29tbWl0Mg==",
			"index": 1
		}
	]`)

	blobs, err := mapBlobs(raw, 100)
	if err != nil {
		t.Fatalf("mapBlobs: %v", err)
	}
	if len(blobs) != 2 {
		t.Fatalf("got %d blobs, want 2", len(blobs))
	}

	if blobs[0].Height != 100 {
		t.Errorf("blob[0].Height = %d, want 100", blobs[0].Height)
	}
	if blobs[0].Namespace != ns {
		t.Errorf("blob[0].Namespace = %v, want %v", blobs[0].Namespace, ns)
	}
	if string(blobs[0].Data) != "hello" {
		t.Errorf("blob[0].Data = %q, want %q", blobs[0].Data, "hello")
	}
	if blobs[0].Index != 0 {
		t.Errorf("blob[0].Index = %d, want 0", blobs[0].Index)
	}
	if blobs[1].Index != 1 {
		t.Errorf("blob[1].Index = %d, want 1", blobs[1].Index)
	}
	if blobs[1].ShareVersion != 1 {
		t.Errorf("blob[1].ShareVersion = %d, want 1", blobs[1].ShareVersion)
	}
}

func TestMapBlobsNull(t *testing.T) {
	for _, input := range []string{"null", "[]", ""} {
		blobs, err := mapBlobs(json.RawMessage(input), 1)
		if err != nil {
			t.Errorf("mapBlobs(%q): %v", input, err)
		}
		if blobs != nil {
			t.Errorf("mapBlobs(%q) = %v, want nil", input, blobs)
		}
	}
}

func TestIsNotFoundErr(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("something else"), false},
		{errors.New("blob: not found"), true},
		{errors.New("header: not found"), true},
		{errors.New("rpc error: blob: not found at height 100"), true},
	}
	for _, tt := range tests {
		got := isNotFoundErr(tt.err)
		if got != tt.want {
			t.Errorf("isNotFoundErr(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestNamespacesToBytes(t *testing.T) {
	var ns1, ns2 types.Namespace
	ns1[0] = 1
	ns2[0] = 2

	out := namespacesToBytes([]types.Namespace{ns1, ns2})
	if len(out) != 2 {
		t.Fatalf("got %d, want 2", len(out))
	}
	if len(out[0]) != types.NamespaceSize {
		t.Errorf("out[0] len = %d, want %d", len(out[0]), types.NamespaceSize)
	}
	if out[0][0] != 1 || out[1][0] != 2 {
		t.Errorf("values mismatch: %v, %v", out[0], out[1])
	}
}

func TestJsonInt64(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{`"12345"`, 12345},
		{`42`, 42},
		{`"0"`, 0},
		{`0`, 0},
	}
	for _, tt := range tests {
		var got jsonInt64
		if err := json.Unmarshal([]byte(tt.input), &got); err != nil {
			t.Errorf("unmarshal %s: %v", tt.input, err)
			continue
		}
		if int64(got) != tt.want {
			t.Errorf("unmarshal %s = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestHexBytes(t *testing.T) {
	tests := []struct {
		input string
		want  []byte
	}{
		{`"AABB"`, []byte{0xAA, 0xBB}},
		{`"aabb"`, []byte{0xAA, 0xBB}},
		{`""`, nil},
	}
	for _, tt := range tests {
		var got hexBytes
		if err := json.Unmarshal([]byte(tt.input), &got); err != nil {
			t.Errorf("unmarshal %s: %v", tt.input, err)
			continue
		}
		if len(got) != len(tt.want) {
			t.Errorf("unmarshal %s: len = %d, want %d", tt.input, len(got), len(tt.want))
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("unmarshal %s: byte %d = %02x, want %02x", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}
