package submit

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"

	secp256k1 "github.com/decred/dcrd/dcrec/secp256k1/v4"
	ecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
)

const testSignerKeyHex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

func TestLoadSigner(t *testing.T) {
	t.Parallel()

	signer, err := LoadSigner(writeSignerKey(t, testSignerKeyHex+"\n"))
	if err != nil {
		t.Fatalf("LoadSigner: %v", err)
	}
	if got := signer.Address(); !strings.HasPrefix(got, "celestia1") {
		t.Fatalf("address = %q, want celestia bech32 prefix", got)
	}
	pubKeyBytes := signer.PublicKey()
	if got := len(pubKeyBytes); got != 33 {
		t.Fatalf("public key length = %d, want 33", got)
	}
	pubKey, err := secp256k1.ParsePubKey(pubKeyBytes)
	if err != nil {
		t.Fatalf("ParsePubKey: %v", err)
	}

	message := []byte("hello")
	signature, err := signer.Sign(message)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if len(signature) != 64 {
		t.Fatalf("signature length = %d, want 64", len(signature))
	}
	hash := sha256.Sum256(message)
	if !parseCompactSignature(t, signature).Verify(hash[:], pubKey) {
		t.Fatal("signature did not verify against the derived public key")
	}
}

func TestLoadSignerRejectsWrongLength(t *testing.T) {
	t.Parallel()

	_, err := LoadSigner(writeSignerKey(t, "abcd"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestLoadSignerRejectsZeroKey(t *testing.T) {
	t.Parallel()

	_, err := LoadSigner(writeSignerKey(t, strings.Repeat("0", secp256k1.PrivKeyBytesLen*2)))
	if err == nil {
		t.Fatal("expected error for zero private key")
	}
}

func TestLoadSignerRejectsMissingFile(t *testing.T) {
	t.Parallel()

	_, err := LoadSigner(filepath.Join(t.TempDir(), "missing.key"))
	if err == nil {
		t.Fatal("expected error for missing signer key file")
	}
}

func TestDecodePrivateKeyHexStripsPrefix(t *testing.T) {
	t.Parallel()

	raw, err := decodePrivateKeyHex("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	if err != nil {
		t.Fatalf("decodePrivateKeyHex: %v", err)
	}
	if len(raw) != secp256k1.PrivKeyBytesLen {
		t.Fatalf("private key length = %d", len(raw))
	}
}

func parseCompactSignature(t *testing.T, raw []byte) *ecdsa.Signature {
	t.Helper()

	if len(raw) != 64 {
		t.Fatalf("compact signature length = %d, want 64", len(raw))
	}

	var r secp256k1.ModNScalar
	if r.SetByteSlice(raw[:32]) {
		t.Fatal("signature r overflowed secp256k1 scalar")
	}
	var s secp256k1.ModNScalar
	if s.SetByteSlice(raw[32:]) {
		t.Fatal("signature s overflowed secp256k1 scalar")
	}
	return ecdsa.NewSignature(&r, &s)
}

func writeSignerKey(t *testing.T, signerKeyHex string) string {
	t.Helper()

	keyPath := filepath.Join(t.TempDir(), "submission.key")
	if err := os.WriteFile(keyPath, []byte(signerKeyHex), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return keyPath
}
