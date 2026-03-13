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
	inner, blobs, typeID := decodeBlobTxEnvelope(t, raw)
	if string(inner) != "inner" {
		t.Fatalf("inner tx = %q", inner)
	}
	if len(blobs) != 1 {
		t.Fatalf("blob count = %d, want 1", len(blobs))
	}
	if len(blobs[0]) == 0 {
		t.Fatal("blob payload is empty")
	}
	if typeID != protoBlobTxTypeID {
		t.Fatalf("type_id = %q, want %q", typeID, protoBlobTxTypeID)
	}
}

func decodeBlobTxEnvelope(t *testing.T, raw []byte) ([]byte, [][]byte, string) {
	t.Helper()

	var (
		innerTx []byte
		blobs   [][]byte
		typeID  string
	)

	data := raw
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			t.Fatal("failed to decode BlobTx tag")
		}
		data = data[n:]

		if typ != protowire.BytesType {
			t.Fatalf("unexpected wire type %d for field %d", typ, num)
		}

		value, n := protowire.ConsumeBytes(data)
		if n < 0 {
			t.Fatalf("failed to decode BlobTx field %d", num)
		}
		data = data[n:]

		switch num {
		case 1:
			innerTx = value
		case 2:
			blobs = append(blobs, value)
		case 3:
			typeID = string(value)
		default:
			t.Fatalf("unexpected BlobTx field %d", num)
		}
	}

	return innerTx, blobs, typeID
}
