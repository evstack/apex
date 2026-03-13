package submit

import (
	"testing"

	blobv1 "github.com/evstack/apex/pkg/api/grpc/gen/celestia/blob/v1"
	secp256k1pb "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/crypto/secp256k1"
	txv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/tx/v1beta1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestBuildMsgPayForBlobs(t *testing.T) {
	ns1 := testNamespace(1)
	ns2 := testNamespace(2)
	msg, err := BuildMsgPayForBlobs("celestia1test", []Blob{
		{Namespace: ns1, Data: []byte("abc"), Commitment: []byte("c1"), ShareVersion: 0},
		{Namespace: ns2, Data: []byte("hello"), Commitment: []byte("c2"), ShareVersion: 1},
	})
	if err != nil {
		t.Fatalf("BuildMsgPayForBlobs: %v", err)
	}
	if got, want := len(msg.GetNamespaces()), 2; got != want {
		t.Fatalf("namespaces = %d, want %d", got, want)
	}
	if msg.GetBlobSizes()[0] != 3 || msg.GetBlobSizes()[1] != 5 {
		t.Fatalf("blob sizes = %v", msg.GetBlobSizes())
	}
	if msg.GetShareVersions()[1] != 1 {
		t.Fatalf("share versions = %v", msg.GetShareVersions())
	}
}

func TestMarshalMsgPayForBlobsAny(t *testing.T) {
	anyMsg, err := MarshalMsgPayForBlobsAny(&blobv1.MsgPayForBlobs{Signer: "celestia1test"})
	if err != nil {
		t.Fatalf("MarshalMsgPayForBlobsAny: %v", err)
	}
	if anyMsg.GetTypeUrl() != MsgPayForBlobsTypeURL {
		t.Fatalf("type URL = %q", anyMsg.GetTypeUrl())
	}
}

func TestBuildSignerInfoAndTxEncoding(t *testing.T) {
	signerInfo, err := BuildSignerInfo([]byte{1, 2, 3}, 9)
	if err != nil {
		t.Fatalf("BuildSignerInfo: %v", err)
	}
	if signerInfo.GetSequence() != 9 {
		t.Fatalf("sequence = %d", signerInfo.GetSequence())
	}
	pubKey := &secp256k1pb.PubKey{}
	if err := anypb.UnmarshalTo(signerInfo.GetPublicKey(), pubKey, proto.UnmarshalOptions{}); err != nil {
		t.Fatalf("unmarshal pubkey any: %v", err)
	}
	if got := pubKey.GetKey(); len(got) != 3 {
		t.Fatalf("pubkey = %v", got)
	}

	authInfo, err := BuildAuthInfo(signerInfo, "utia", "123", 456, "celestia1test", "")
	if err != nil {
		t.Fatalf("BuildAuthInfo: %v", err)
	}
	authInfoBytes, err := MarshalAuthInfo(authInfo)
	if err != nil {
		t.Fatalf("MarshalAuthInfo: %v", err)
	}

	bodyBytes, err := MarshalTxBody(nil, "memo", 99)
	if err != nil {
		t.Fatalf("MarshalTxBody: %v", err)
	}
	signDocBytes, err := BuildSignDoc(bodyBytes, authInfoBytes, "mocha-4", 7)
	if err != nil {
		t.Fatalf("BuildSignDoc: %v", err)
	}
	signDoc := &txv1beta1.SignDoc{}
	if err := proto.Unmarshal(signDocBytes, signDoc); err != nil {
		t.Fatalf("unmarshal sign doc: %v", err)
	}
	if signDoc.GetChainId() != "mocha-4" || signDoc.GetAccountNumber() != 7 {
		t.Fatalf("sign doc = %#v", signDoc)
	}

	txRawBytes, err := MarshalTxRaw(bodyBytes, authInfoBytes, []byte("signature"))
	if err != nil {
		t.Fatalf("MarshalTxRaw: %v", err)
	}
	txRaw := &txv1beta1.TxRaw{}
	if err := proto.Unmarshal(txRawBytes, txRaw); err != nil {
		t.Fatalf("unmarshal tx raw: %v", err)
	}
	if got, want := len(txRaw.GetSignatures()), 1; got != want {
		t.Fatalf("signatures = %d, want %d", got, want)
	}
}

func TestFeeAmountFromGasPrice(t *testing.T) {
	fee, err := FeeAmountFromGasPrice(1000, 0.001)
	if err != nil {
		t.Fatalf("FeeAmountFromGasPrice: %v", err)
	}
	if fee != "1" {
		t.Fatalf("fee = %q, want 1", fee)
	}
}
