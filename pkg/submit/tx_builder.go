package submit

import (
	"errors"
	"fmt"
	"math"

	blobv1 "github.com/evstack/apex/pkg/api/grpc/gen/celestia/blob/v1"
	basev1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/base/v1beta1"
	secp256k1pb "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/crypto/secp256k1"
	signingv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/tx/signing/v1beta1"
	txv1beta1 "github.com/evstack/apex/pkg/api/grpc/gen/cosmos/tx/v1beta1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	// MsgPayForBlobsTypeURL is the protobuf Any type URL for Celestia blob txs.
	MsgPayForBlobsTypeURL = "/celestia.blob.v1.MsgPayForBlobs"
	// Secp256k1PubKeyTypeURL is the protobuf Any type URL for Cosmos secp256k1 pubkeys.
	Secp256k1PubKeyTypeURL = "/cosmos.crypto.secp256k1.PubKey"
)

// BuildMsgPayForBlobs converts submitted blobs into the Celestia
// MsgPayForBlobs request shape used inside the signed Cosmos SDK tx.
func BuildMsgPayForBlobs(signer string, blobs []Blob) (*blobv1.MsgPayForBlobs, error) {
	if signer == "" {
		return nil, errors.New("signer is required")
	}
	if len(blobs) == 0 {
		return nil, errors.New("at least one blob is required")
	}

	namespaces := make([][]byte, len(blobs))
	blobSizes := make([]uint32, len(blobs))
	commitments := make([][]byte, len(blobs))
	shareVersions := make([]uint32, len(blobs))
	for i := range blobs {
		if len(blobs[i].Commitment) == 0 {
			return nil, fmt.Errorf("blob %d commitment is required", i)
		}
		namespaces[i] = blobs[i].Namespace[:]
		blobSizes[i] = uint32(len(blobs[i].Data))
		commitments[i] = blobs[i].Commitment
		shareVersions[i] = uint32(blobs[i].ShareVersion)
	}

	return &blobv1.MsgPayForBlobs{
		Signer:           signer,
		Namespaces:       namespaces,
		BlobSizes:        blobSizes,
		ShareCommitments: commitments,
		ShareVersions:    shareVersions,
	}, nil
}

// MarshalMsgPayForBlobsAny packs MsgPayForBlobs into a protobuf Any suitable
// for inclusion in cosmos.tx.v1beta1.TxBody.
func MarshalMsgPayForBlobsAny(msg *blobv1.MsgPayForBlobs) (*anypb.Any, error) {
	if msg == nil {
		return nil, errors.New("pay-for-blobs message is required")
	}
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal pay-for-blobs message: %w", err)
	}
	return &anypb.Any{
		TypeUrl: MsgPayForBlobsTypeURL,
		Value:   msgBytes,
	}, nil
}

// BuildSignerInfo constructs a direct-signing signer record using a raw
// secp256k1 public key.
func BuildSignerInfo(pubKey []byte, sequence uint64) (*txv1beta1.SignerInfo, error) {
	if len(pubKey) == 0 {
		return nil, errors.New("public key is required")
	}
	pubKeyBytes, err := proto.Marshal(&secp256k1pb.PubKey{Key: pubKey})
	if err != nil {
		return nil, fmt.Errorf("marshal secp256k1 pubkey: %w", err)
	}
	return &txv1beta1.SignerInfo{
		PublicKey: &anypb.Any{
			TypeUrl: Secp256k1PubKeyTypeURL,
			Value:   pubKeyBytes,
		},
		ModeInfo: &txv1beta1.ModeInfo{
			Sum: &txv1beta1.ModeInfo_Single_{
				Single: &txv1beta1.ModeInfo_Single{
					Mode: signingv1beta1.SignMode_SIGN_MODE_DIRECT,
				},
			},
		},
		Sequence: sequence,
	}, nil
}

// BuildAuthInfo creates an AuthInfo for a single direct signer and fee payer.
func BuildAuthInfo(signerInfo *txv1beta1.SignerInfo, feeDenom string, feeAmount string, gasLimit uint64, payer, granter string) (*txv1beta1.AuthInfo, error) {
	if signerInfo == nil {
		return nil, errors.New("signer info is required")
	}
	if gasLimit == 0 {
		return nil, errors.New("gas limit must be positive")
	}

	fee := &txv1beta1.Fee{
		GasLimit: gasLimit,
		Payer:    payer,
		Granter:  granter,
	}
	if feeDenom != "" || feeAmount != "" {
		if feeDenom == "" || feeAmount == "" {
			return nil, errors.New("fee denom and amount must be provided together")
		}
		fee.Amount = []*basev1beta1.Coin{{
			Denom:  feeDenom,
			Amount: feeAmount,
		}}
	}

	return &txv1beta1.AuthInfo{
		SignerInfos: []*txv1beta1.SignerInfo{signerInfo},
		Fee:         fee,
	}, nil
}

func MarshalTxBody(messages []*anypb.Any, memo string, timeoutHeight uint64) ([]byte, error) {
	body := &txv1beta1.TxBody{
		Messages:      messages,
		Memo:          memo,
		TimeoutHeight: timeoutHeight,
	}
	raw, err := proto.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal tx body: %w", err)
	}
	return raw, nil
}

func MarshalAuthInfo(authInfo *txv1beta1.AuthInfo) ([]byte, error) {
	if authInfo == nil {
		return nil, errors.New("auth info is required")
	}
	raw, err := proto.Marshal(authInfo)
	if err != nil {
		return nil, fmt.Errorf("marshal auth info: %w", err)
	}
	return raw, nil
}

func BuildSignDoc(bodyBytes, authInfoBytes []byte, chainID string, accountNumber uint64) ([]byte, error) {
	if chainID == "" {
		return nil, errors.New("chain id is required")
	}
	doc := &txv1beta1.SignDoc{
		BodyBytes:     bodyBytes,
		AuthInfoBytes: authInfoBytes,
		ChainId:       chainID,
		AccountNumber: accountNumber,
	}
	raw, err := proto.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal sign doc: %w", err)
	}
	return raw, nil
}

// MarshalTxRaw creates the final raw transaction bytes to broadcast.
func MarshalTxRaw(bodyBytes, authInfoBytes, signature []byte) ([]byte, error) {
	if len(signature) == 0 {
		return nil, errors.New("signature is required")
	}
	raw, err := proto.Marshal(&txv1beta1.TxRaw{
		BodyBytes:     bodyBytes,
		AuthInfoBytes: authInfoBytes,
		Signatures:    [][]byte{signature},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal tx raw: %w", err)
	}
	return raw, nil
}

// FeeAmountFromGasPrice computes the integer fee amount using a gas limit and
// decimal gas price. The result is rounded up to avoid underpaying.
func FeeAmountFromGasPrice(gasLimit uint64, gasPrice float64) (string, error) {
	if gasLimit == 0 {
		return "", errors.New("gas limit must be positive")
	}
	if gasPrice < 0 {
		return "", errors.New("gas price must be non-negative")
	}
	fee := math.Ceil(float64(gasLimit) * gasPrice)
	return fmt.Sprintf("%.0f", fee), nil
}
