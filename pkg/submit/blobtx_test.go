package submit

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

func TestEstimateGas(t *testing.T) {
	t.Parallel()

	gas, err := EstimateGas([]Blob{{
		Namespace:  testNamespace(1),
		Data:       make([]byte, 600),
		Commitment: []byte("c1"),
	}}, 0)
	if err != nil {
		t.Fatalf("EstimateGas: %v", err)
	}
	if gas != 83192 {
		t.Fatalf("gas = %d, want 83192", gas)
	}
}

func TestMarshalBlobTx(t *testing.T) {
	t.Parallel()

	raw, err := MarshalBlobTx([]byte("inner"), []Blob{{
		Namespace:    testNamespace(2),
		Data:         []byte("payload"),
		ShareVersion: 1,
		Signer:       []byte("signer"),
	}})
	if err != nil {
		t.Fatalf("MarshalBlobTx: %v", err)
	}
	if raw[len(raw)-1] != blobTxTypeID {
		t.Fatalf("trailing type byte = 0x%02x", raw[len(raw)-1])
	}

	inner, n := protowire.ConsumeBytes(raw[:len(raw)-1])
	if n < 0 {
		t.Fatal("failed to decode inner tx")
	}
	if string(inner) != "inner" {
		t.Fatalf("inner tx = %q", inner)
	}

	blobBytes, bn := protowire.ConsumeBytes(raw[n : len(raw)-1])
	if bn < 0 {
		t.Fatal("failed to decode blob payload")
	}
	if len(blobBytes) == 0 {
		t.Fatal("blob payload is empty")
	}
}
