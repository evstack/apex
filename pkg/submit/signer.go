package submit

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/cosmos/btcutil/bech32"
	secp256k1 "github.com/decred/dcrd/dcrec/secp256k1/v4"
	ecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"

	"github.com/evstack/apex/pkg/types"
)

const defaultAddressPrefix = "celestia"

// Signer owns the local secp256k1 key material for direct blob submission.
type Signer struct {
	address    string
	privateKey *secp256k1.PrivateKey
	publicKey  []byte
}

// LoadSigner parses a hex-encoded secp256k1 private key and derives the
// corresponding Cosmos-style address and compressed public key.
func LoadSigner(privateKeyHex string) (*Signer, error) {
	privKeyBytes, err := decodePrivateKeyHex(privateKeyHex)
	if err != nil {
		return nil, err
	}

	var scalar secp256k1.ModNScalar
	if scalar.SetByteSlice(privKeyBytes) {
		return nil, errors.New("submission private key exceeds the secp256k1 curve order")
	}
	if scalar.IsZero() {
		return nil, errors.New("submission private key must not be zero")
	}

	privateKey := secp256k1.NewPrivateKey(&scalar)
	publicKey := privateKey.PubKey().SerializeCompressed()
	address, err := pubKeyAddress(publicKey, defaultAddressPrefix)
	if err != nil {
		return nil, err
	}

	return &Signer{
		address:    address,
		privateKey: privateKey,
		publicKey:  publicKey,
	}, nil
}

func (s *Signer) Address() string {
	return s.address
}

func (s *Signer) PublicKey() []byte {
	return append([]byte(nil), s.publicKey...)
}

func (s *Signer) Sign(message []byte) ([]byte, error) {
	if s == nil || s.privateKey == nil {
		return nil, errors.New("signer is required")
	}

	hash := sha256.Sum256(message)
	signature := ecdsa.Sign(s.privateKey, hash[:])
	return marshalSignature(signature), nil
}

func decodePrivateKeyHex(raw string) ([]byte, error) {
	raw = strings.TrimSpace(types.StripHexPrefix(raw))
	if raw == "" {
		return nil, errors.New("submission private key is required")
	}

	privKeyBytes, err := hex.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("decode submission private key hex: %w", err)
	}
	if len(privKeyBytes) != secp256k1.PrivKeyBytesLen {
		return nil, fmt.Errorf("submission private key must be %d bytes, got %d", secp256k1.PrivKeyBytesLen, len(privKeyBytes))
	}
	return privKeyBytes, nil
}

func pubKeyAddress(publicKey []byte, prefix string) (string, error) {
	fiveBit, err := bech32.ConvertBits(btcutil.Hash160(publicKey), 8, 5, true)
	if err != nil {
		return "", fmt.Errorf("convert address bits: %w", err)
	}
	address, err := bech32.Encode(prefix, fiveBit)
	if err != nil {
		return "", fmt.Errorf("bech32 encode address: %w", err)
	}
	return address, nil
}

func marshalSignature(signature *ecdsa.Signature) []byte {
	var raw [64]byte
	r := signature.R()
	s := signature.S()
	r.PutBytesUnchecked(raw[:32])
	s.PutBytesUnchecked(raw[32:])
	return raw[:]
}
