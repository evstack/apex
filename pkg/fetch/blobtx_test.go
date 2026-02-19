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
	if len(b.Signer) > 0 {
		out = protowire.AppendTag(out, 5, protowire.BytesType)
		out = protowire.AppendBytes(out, b.Signer)
	}
	return out
}

// buildMsgPayForBlobs encodes a MsgPayForBlobs proto.
// field 1: signer (string/bytes), field 4: share_commitments (repeated bytes)
func buildMsgPayForBlobs(signer string, commitments [][]byte) []byte {
	var out []byte
	if signer != "" {
		out = protowire.AppendTag(out, 1, protowire.BytesType)
		out = protowire.AppendBytes(out, []byte(signer))
	}
	for _, c := range commitments {
		out = protowire.AppendTag(out, 4, protowire.BytesType)
		out = protowire.AppendBytes(out, c)
	}
	return out
}

// buildAny encodes a google.protobuf.Any proto.
func buildAny(typeURL string, value []byte) []byte {
	var out []byte
	out = protowire.AppendTag(out, 1, protowire.BytesType)
	out = protowire.AppendBytes(out, []byte(typeURL))
	out = protowire.AppendTag(out, 2, protowire.BytesType)
	out = protowire.AppendBytes(out, value)
	return out
}

// buildTxBody encodes a TxBody with messages.
func buildTxBody(messages ...[]byte) []byte {
	var out []byte
	for _, msg := range messages {
		out = protowire.AppendTag(out, 1, protowire.BytesType)
		out = protowire.AppendBytes(out, msg)
	}
	return out
}

// buildTx encodes a Cosmos SDK Tx with a body.
func buildTx(body []byte) []byte {
	var out []byte
	out = protowire.AppendTag(out, 1, protowire.BytesType)
	out = protowire.AppendBytes(out, body)
	return out
}

// buildInnerSDKTx constructs a valid inner SDK tx containing MsgPayForBlobs.
func buildInnerSDKTx(signer string, commitments [][]byte) []byte {
	pfb := buildMsgPayForBlobs(signer, commitments)
	any := buildAny(msgPayForBlobsTypeURL, pfb)
	body := buildTxBody(any)
	return buildTx(body)
}

// buildBlobTx constructs a valid BlobTx wire-format message with proper
// MsgPayForBlobs containing commitments and signer.
func buildBlobTx(signer string, commitments [][]byte, blobs ...rawBlob) []byte {
	innerTx := buildInnerSDKTx(signer, commitments)
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
	tx := buildBlobTx("celestia1abc", [][]byte{[]byte("commit1")},
		rawBlob{Namespace: ns, Data: []byte("hello")},
	)

	parsed, err := parseBlobTx(tx)
	if err != nil {
		t.Fatalf("parseBlobTx: %v", err)
	}
	if len(parsed.Blobs) != 1 {
		t.Fatalf("got %d blobs, want 1", len(parsed.Blobs))
	}
	if string(parsed.Blobs[0].Data) != "hello" {
		t.Errorf("Data = %q, want %q", parsed.Blobs[0].Data, "hello")
	}
	if string(parsed.PFB.Signer) != "celestia1abc" {
		t.Errorf("Signer = %q, want %q", parsed.PFB.Signer, "celestia1abc")
	}
	if len(parsed.PFB.ShareCommitments) != 1 {
		t.Fatalf("got %d commitments, want 1", len(parsed.PFB.ShareCommitments))
	}
	if string(parsed.PFB.ShareCommitments[0]) != "commit1" {
		t.Errorf("Commitment = %q, want %q", parsed.PFB.ShareCommitments[0], "commit1")
	}
}

func TestParseBlobTxMultiBlob(t *testing.T) {
	tx := buildBlobTx("signer", [][]byte{[]byte("c1"), []byte("c2")},
		rawBlob{Namespace: testNS(1), Data: []byte("a")},
		rawBlob{Namespace: testNS(2), Data: []byte("b")},
	)

	parsed, err := parseBlobTx(tx)
	if err != nil {
		t.Fatalf("parseBlobTx: %v", err)
	}
	if len(parsed.Blobs) != 2 {
		t.Fatalf("got %d blobs, want 2", len(parsed.Blobs))
	}
	if string(parsed.Blobs[0].Data) != "a" {
		t.Errorf("blob[0].Data = %q", parsed.Blobs[0].Data)
	}
	if string(parsed.Blobs[1].Data) != "b" {
		t.Errorf("blob[1].Data = %q", parsed.Blobs[1].Data)
	}
	if len(parsed.PFB.ShareCommitments) != 2 {
		t.Fatalf("got %d commitments, want 2", len(parsed.PFB.ShareCommitments))
	}
	if string(parsed.PFB.ShareCommitments[0]) != "c1" {
		t.Errorf("commitment[0] = %q", parsed.PFB.ShareCommitments[0])
	}
	if string(parsed.PFB.ShareCommitments[1]) != "c2" {
		t.Errorf("commitment[1] = %q", parsed.PFB.ShareCommitments[1])
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

	tx1 := buildBlobTx("signer1", [][]byte{[]byte("c1"), []byte("c3")},
		rawBlob{Namespace: ns1, Data: []byte("d1")},
		rawBlob{Namespace: ns3, Data: []byte("d3")},
	)
	tx2 := buildBlobTx("signer2", [][]byte{[]byte("c2")},
		rawBlob{Namespace: ns2, Data: []byte("d2")},
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

	// First matching blob: ns1 from tx1
	if string(blobs[0].Data) != "d1" {
		t.Errorf("blobs[0].Data = %q, want %q", blobs[0].Data, "d1")
	}
	if blobs[0].Height != 100 {
		t.Errorf("blobs[0].Height = %d, want 100", blobs[0].Height)
	}
	if string(blobs[0].Commitment) != "c1" {
		t.Errorf("blobs[0].Commitment = %q, want %q", blobs[0].Commitment, "c1")
	}
	if string(blobs[0].Signer) != "signer1" {
		t.Errorf("blobs[0].Signer = %q, want %q", blobs[0].Signer, "signer1")
	}

	// Second matching blob: ns2 from tx2
	if string(blobs[1].Data) != "d2" {
		t.Errorf("blobs[1].Data = %q, want %q", blobs[1].Data, "d2")
	}
	if string(blobs[1].Commitment) != "c2" {
		t.Errorf("blobs[1].Commitment = %q, want %q", blobs[1].Commitment, "c2")
	}
	if string(blobs[1].Signer) != "signer2" {
		t.Errorf("blobs[1].Signer = %q, want %q", blobs[1].Signer, "signer2")
	}
}

func TestExtractBlobsFromBlockNoMatch(t *testing.T) {
	ns1 := testNS(1)
	ns99 := testNS(99)
	var nsType99 types.Namespace
	copy(nsType99[:], ns99)

	tx := buildBlobTx("s", [][]byte{[]byte("c")},
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

func TestParsePFBFromTx(t *testing.T) {
	innerTx := buildInnerSDKTx("celestia1xyz", [][]byte{
		[]byte("commit_a"),
		[]byte("commit_b"),
	})

	pfb, err := parsePFBFromTx(innerTx)
	if err != nil {
		t.Fatalf("parsePFBFromTx: %v", err)
	}
	if string(pfb.Signer) != "celestia1xyz" {
		t.Errorf("Signer = %q, want %q", pfb.Signer, "celestia1xyz")
	}
	if len(pfb.ShareCommitments) != 2 {
		t.Fatalf("got %d commitments, want 2", len(pfb.ShareCommitments))
	}
	if string(pfb.ShareCommitments[0]) != "commit_a" {
		t.Errorf("commitment[0] = %q", pfb.ShareCommitments[0])
	}
	if string(pfb.ShareCommitments[1]) != "commit_b" {
		t.Errorf("commitment[1] = %q", pfb.ShareCommitments[1])
	}
}
