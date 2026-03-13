package submit

import (
	"testing"

	secp256k1 "github.com/decred/dcrd/dcrec/secp256k1/v4"
)

func TestLoadSigner(t *testing.T) {
	t.Parallel()

	signer, err := LoadSigner("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	if err != nil {
		t.Fatalf("LoadSigner: %v", err)
	}
	if got := signer.Address(); got == "" {
		t.Fatal("address is empty")
	}
	if got := signer.PublicKey(); len(got) != 33 {
		t.Fatalf("public key length = %d, want 33", len(got))
	}

	signature, err := signer.Sign([]byte("hello"))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if len(signature) != 64 {
		t.Fatalf("signature length = %d, want 64", len(signature))
	}
}

func TestLoadSignerRejectsWrongLength(t *testing.T) {
	t.Parallel()

	_, err := LoadSigner("abcd")
	if err == nil {
		t.Fatal("expected error, got nil")
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
