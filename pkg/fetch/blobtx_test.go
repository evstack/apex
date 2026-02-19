package fetch

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/evstack/apex/pkg/types"
)

// buildBlobProto encodes a rawBlob as a protobuf message.
func buildBlobProto(b rawBlob) []byte {
	var out []byte
	if len(b.Namespace) > 0 {
		out = protowire.AppendTag(out, 1, protowire.BytesType)
		out = protowire.AppendBytes(out, b.Namespace)
	}
	if len(b.Data) > 0 {
		out = protowire.AppendTag(out, 2, protowire.BytesType)
		out = protowire.AppendBytes(out, b.Data)
	}
	if b.ShareVersion > 0 {
		out = protowire.AppendTag(out, 3, protowire.VarintType)
		out = protowire.AppendVarint(out, uint64(b.ShareVersion))
	}
	if b.NamespaceVersion > 0 {
		out = protowire.AppendTag(out, 4, protowire.VarintType)
		out = protowire.AppendVarint(out, uint64(b.NamespaceVersion))
	}
	if len(b.ShareCommitment) > 0 {
		out = protowire.AppendTag(out, 5, protowire.BytesType)
		out = protowire.AppendBytes(out, b.ShareCommitment)
	}
	return out
}

// buildBlobTx constructs a valid BlobTx wire-format message.
func buildBlobTx(innerTx []byte, blobs ...rawBlob) []byte {
	var out []byte
	// Length-prefixed inner tx.
	out = protowire.AppendBytes(out, innerTx)
	// Length-prefixed blob protos.
	for _, b := range blobs {
		blobProto := buildBlobProto(b)
		out = protowire.AppendBytes(out, blobProto)
	}
	// Trailing BlobTx type byte.
	out = append(out, blobTxTypeID)
	return out
}

func testNS(b byte) []byte {
	ns := make([]byte, types.NamespaceSize)
	ns[types.NamespaceSize-1] = b
	return ns
}

func TestParseBlobTxSingleBlob(t *testing.T) {
	ns := testNS(1)
	tx := buildBlobTx([]byte("sdk-tx"), rawBlob{
		Namespace:       ns,
		Data:            []byte("hello"),
		ShareVersion:    0,
		ShareCommitment: []byte("commit1"),
	})

	blobs, err := parseBlobTx(tx)
	if err != nil {
		t.Fatalf("parseBlobTx: %v", err)
	}
	if len(blobs) != 1 {
		t.Fatalf("got %d blobs, want 1", len(blobs))
	}
	if string(blobs[0].Data) != "hello" {
		t.Errorf("Data = %q, want %q", blobs[0].Data, "hello")
	}
	if string(blobs[0].ShareCommitment) != "commit1" {
		t.Errorf("ShareCommitment = %q, want %q", blobs[0].ShareCommitment, "commit1")
	}
}

func TestParseBlobTxMultiBlob(t *testing.T) {
	tx := buildBlobTx([]byte("sdk-tx"),
		rawBlob{Namespace: testNS(1), Data: []byte("a"), ShareCommitment: []byte("c1")},
		rawBlob{Namespace: testNS(2), Data: []byte("b"), ShareCommitment: []byte("c2")},
	)

	blobs, err := parseBlobTx(tx)
	if err != nil {
		t.Fatalf("parseBlobTx: %v", err)
	}
	if len(blobs) != 2 {
		t.Fatalf("got %d blobs, want 2", len(blobs))
	}
	if string(blobs[0].Data) != "a" {
		t.Errorf("blob[0].Data = %q", blobs[0].Data)
	}
	if string(blobs[1].Data) != "b" {
		t.Errorf("blob[1].Data = %q", blobs[1].Data)
	}
}

func TestParseBlobTxNotBlobTx(t *testing.T) {
	_, err := parseBlobTx([]byte{0x01, 0x02, 0x03})
	if err == nil {
		t.Fatal("expected error for non-BlobTx")
	}
}

func TestParseBlobTxEmpty(t *testing.T) {
	_, err := parseBlobTx(nil)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestExtractBlobsFromBlock(t *testing.T) {
	ns1 := testNS(1)
	ns2 := testNS(2)
	ns3 := testNS(3)

	var nsType1 types.Namespace
	copy(nsType1[:], ns1)
	var nsType2 types.Namespace
	copy(nsType2[:], ns2)

	tx1 := buildBlobTx([]byte("tx1"),
		rawBlob{Namespace: ns1, Data: []byte("d1"), ShareCommitment: []byte("c1")},
		rawBlob{Namespace: ns3, Data: []byte("d3"), ShareCommitment: []byte("c3")},
	)
	tx2 := buildBlobTx([]byte("tx2"),
		rawBlob{Namespace: ns2, Data: []byte("d2"), ShareCommitment: []byte("c2")},
	)
	// Non-BlobTx should be skipped.
	regularTx := []byte("regular-cosmos-tx")

	blobs, err := extractBlobsFromBlock(
		[][]byte{tx1, regularTx, tx2},
		[]types.Namespace{nsType1, nsType2},
		100,
	)
	if err != nil {
		t.Fatalf("extractBlobsFromBlock: %v", err)
	}
	if len(blobs) != 2 {
		t.Fatalf("got %d blobs, want 2", len(blobs))
	}

	// First matching blob: ns1
	if string(blobs[0].Data) != "d1" {
		t.Errorf("blobs[0].Data = %q, want %q", blobs[0].Data, "d1")
	}
	if blobs[0].Height != 100 {
		t.Errorf("blobs[0].Height = %d, want 100", blobs[0].Height)
	}
	if string(blobs[0].Commitment) != "c1" {
		t.Errorf("blobs[0].Commitment = %q, want %q", blobs[0].Commitment, "c1")
	}

	// Second matching blob: ns2
	if string(blobs[1].Data) != "d2" {
		t.Errorf("blobs[1].Data = %q, want %q", blobs[1].Data, "d2")
	}
}

func TestExtractBlobsFromBlockNoMatch(t *testing.T) {
	ns1 := testNS(1)
	ns99 := testNS(99)
	var nsType99 types.Namespace
	copy(nsType99[:], ns99)

	tx := buildBlobTx([]byte("tx"),
		rawBlob{Namespace: ns1, Data: []byte("d1")},
	)

	blobs, err := extractBlobsFromBlock([][]byte{tx}, []types.Namespace{nsType99}, 50)
	if err != nil {
		t.Fatalf("extractBlobsFromBlock: %v", err)
	}
	if len(blobs) != 0 {
		t.Fatalf("got %d blobs, want 0", len(blobs))
	}
}
