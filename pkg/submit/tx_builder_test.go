package submit

import (
	"bytes"
	"testing"

	blobv1 "github.com/evstack/apex/pkg/api/grpc/gen/celestia/blob/v1"
	secp256k1pb "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/crypto/secp256k1"
	txv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/tx/v1beta1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const testSignerAddress = "celestia1test"

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
	if !bytes.Equal(msg.GetNamespaces()[0], ns1[:]) || !bytes.Equal(msg.GetNamespaces()[1], ns2[:]) {
		t.Fatalf("namespaces = %x, want [%x %x]", msg.GetNamespaces(), ns1, ns2)
	}
	if msg.GetBlobSizes()[0] != 3 || msg.GetBlobSizes()[1] != 5 {
		t.Fatalf("blob sizes = %v", msg.GetBlobSizes())
	}
	if !bytes.Equal(msg.GetShareCommitments()[0], []byte("c1")) || !bytes.Equal(msg.GetShareCommitments()[1], []byte("c2")) {
		t.Fatalf("commitments = %q", msg.GetShareCommitments())
	}
	if msg.GetShareVersions()[1] != 1 {
		t.Fatalf("share versions = %v", msg.GetShareVersions())
	}
}

func TestMarshalMsgPayForBlobsAny(t *testing.T) {
	anyMsg, err := MarshalMsgPayForBlobsAny(&blobv1.MsgPayForBlobs{Signer: testSignerAddress})
	if err != nil {
		t.Fatalf("MarshalMsgPayForBlobsAny: %v", err)
	}
	if anyMsg.GetTypeUrl() != MsgPayForBlobsTypeURL {
		t.Fatalf("type URL = %q", anyMsg.GetTypeUrl())
	}
	var decoded blobv1.MsgPayForBlobs
	if err := proto.Unmarshal(anyMsg.GetValue(), &decoded); err != nil {
		t.Fatalf("unmarshal Any payload: %v", err)
	}
	if decoded.GetSigner() != testSignerAddress {
		t.Fatalf("decoded signer = %q, want %q", decoded.GetSigner(), testSignerAddress)
	}
}

func TestBuildSignerInfoAndTxEncoding(t *testing.T) {
	signerInfo, err := BuildSignerInfo([]byte{1, 2, 3}, 9)
	if err != nil {
		t.Fatalf("BuildSignerInfo: %v", err)
	}
	assertSignerInfo(t, signerInfo)

	authInfo, err := BuildAuthInfo(signerInfo, "utia", "123", 456, testSignerAddress, "")
	if err != nil {
		t.Fatalf("BuildAuthInfo: %v", err)
	}
	authInfoBytes, err := MarshalAuthInfo(authInfo)
	if err != nil {
		t.Fatalf("MarshalAuthInfo: %v", err)
	}
	assertAuthInfoBytes(t, authInfoBytes)

	bodyBytes, err := MarshalTxBody(nil, "memo", 99)
	if err != nil {
		t.Fatalf("MarshalTxBody: %v", err)
	}
	assertTxBodyBytes(t, bodyBytes)
	signDocBytes, err := BuildSignDoc(bodyBytes, authInfoBytes, "mocha-4", 7)
	if err != nil {
		t.Fatalf("BuildSignDoc: %v", err)
	}
	assertSignDocBytes(t, signDocBytes, bodyBytes, authInfoBytes)

	txRawBytes, err := MarshalTxRaw(bodyBytes, authInfoBytes, []byte("signature"))
	if err != nil {
		t.Fatalf("MarshalTxRaw: %v", err)
	}
	assertTxRawBytes(t, txRawBytes, bodyBytes, authInfoBytes)
}

func TestFeeAmountFromGasPrice(t *testing.T) {
	fee, err := FeeAmountFromGasPrice(1001, 0.001)
	if err != nil {
		t.Fatalf("FeeAmountFromGasPrice: %v", err)
	}
	if fee != "2" {
		t.Fatalf("fee = %q, want 2", fee)
	}
}

func assertSignerInfo(t *testing.T, signerInfo *txv1beta1.SignerInfo) {
	t.Helper()

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
}

func assertAuthInfoBytes(t *testing.T, authInfoBytes []byte) {
	t.Helper()

	authInfoProto := &txv1beta1.AuthInfo{}
	if err := proto.Unmarshal(authInfoBytes, authInfoProto); err != nil {
		t.Fatalf("unmarshal auth info: %v", err)
	}
	if authInfoProto.GetFee().GetGasLimit() != 456 {
		t.Fatalf("gas limit = %d, want 456", authInfoProto.GetFee().GetGasLimit())
	}
	if authInfoProto.GetFee().GetPayer() != testSignerAddress {
		t.Fatalf("payer = %q, want %q", authInfoProto.GetFee().GetPayer(), testSignerAddress)
	}
	if got := authInfoProto.GetFee().GetAmount(); len(got) != 1 || got[0].GetDenom() != "utia" || got[0].GetAmount() != "123" {
		t.Fatalf("fee amount = %#v", got)
	}
}

func assertTxBodyBytes(t *testing.T, bodyBytes []byte) {
	t.Helper()

	body := &txv1beta1.TxBody{}
	if err := proto.Unmarshal(bodyBytes, body); err != nil {
		t.Fatalf("unmarshal tx body: %v", err)
	}
	if body.GetMemo() != "memo" || body.GetTimeoutHeight() != 99 {
		t.Fatalf("tx body = %#v", body)
	}
}

func assertSignDocBytes(t *testing.T, signDocBytes, bodyBytes, authInfoBytes []byte) {
	t.Helper()

	signDoc := &txv1beta1.SignDoc{}
	if err := proto.Unmarshal(signDocBytes, signDoc); err != nil {
		t.Fatalf("unmarshal sign doc: %v", err)
	}
	if signDoc.GetChainId() != "mocha-4" || signDoc.GetAccountNumber() != 7 {
		t.Fatalf("sign doc = %#v", signDoc)
	}
	if !bytes.Equal(signDoc.GetBodyBytes(), bodyBytes) || !bytes.Equal(signDoc.GetAuthInfoBytes(), authInfoBytes) {
		t.Fatalf("sign doc bytes do not match input payloads")
	}
}

func assertTxRawBytes(t *testing.T, txRawBytes, bodyBytes, authInfoBytes []byte) {
	t.Helper()

	txRaw := &txv1beta1.TxRaw{}
	if err := proto.Unmarshal(txRawBytes, txRaw); err != nil {
		t.Fatalf("unmarshal tx raw: %v", err)
	}
	if got, want := len(txRaw.GetSignatures()), 1; got != want {
		t.Fatalf("signatures = %d, want %d", got, want)
	}
	if !bytes.Equal(txRaw.GetBodyBytes(), bodyBytes) || !bytes.Equal(txRaw.GetAuthInfoBytes(), authInfoBytes) {
		t.Fatalf("tx raw bytes do not match input payloads")
	}
	if !bytes.Equal(txRaw.GetSignatures()[0], []byte("signature")) {
		t.Fatalf("signature = %q, want %q", txRaw.GetSignatures()[0], "signature")
	}
}
