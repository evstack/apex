package fetch

import (
	"testing"

	"github.com/evstack/apex/pkg/submit"
	"github.com/evstack/apex/pkg/types"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestParseBlobTxFromSubmitMarshaller(t *testing.T) {
	t.Parallel()

	blob := submit.Blob{
		Namespace:    testNamespaceType(3),
		Data:         []byte("payload"),
		ShareVersion: 1,
		Commitment:   []byte("commitment"),
		Signer:       []byte("blob-signer"),
	}
	pfb, err := submit.BuildMsgPayForBlobs("celestia1submitter", []submit.Blob{blob})
	if err != nil {
		t.Fatalf("BuildMsgPayForBlobs: %v", err)
	}
	msg, err := submit.MarshalMsgPayForBlobsAny(pfb)
	if err != nil {
		t.Fatalf("MarshalMsgPayForBlobsAny: %v", err)
	}
	bodyBytes, err := submit.MarshalTxBody([]*anypb.Any{msg}, "", 0)
	if err != nil {
		t.Fatalf("MarshalTxBody: %v", err)
	}
	innerTx, err := submit.MarshalTxRaw(bodyBytes, []byte("auth"), []byte("signature"))
	if err != nil {
		t.Fatalf("MarshalTxRaw: %v", err)
	}
	raw, err := submit.MarshalBlobTx(innerTx, []submit.Blob{blob})
	if err != nil {
		t.Fatalf("MarshalBlobTx: %v", err)
	}

	parsed, err := parseBlobTx(raw)
	if err != nil {
		t.Fatalf("parseBlobTx: %v", err)
	}
	if len(parsed.Blobs) != 1 {
		t.Fatalf("got %d blobs, want 1", len(parsed.Blobs))
	}
	if string(parsed.Blobs[0].Data) != "payload" {
		t.Fatalf("data = %q", parsed.Blobs[0].Data)
	}
	if string(parsed.PFB.Signer) != "celestia1submitter" {
		t.Fatalf("signer = %q", parsed.PFB.Signer)
	}
	if string(parsed.PFB.ShareCommitments[0]) != "commitment" {
		t.Fatalf("commitment = %q", parsed.PFB.ShareCommitments[0])
	}
}

func testNamespaceType(b byte) types.Namespace {
	var ns types.Namespace
	ns[types.NamespaceSize-1] = b
	return ns
}
