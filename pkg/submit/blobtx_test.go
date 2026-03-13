package submit

import (
	"bytes"
	"testing"

	"github.com/evstack/apex/pkg/types"
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

	blob := Blob{
		Namespace:    testNamespace(2),
		Data:         []byte("payload"),
		ShareVersion: 1,
		Signer:       testBlobSigner(),
	}
	raw, err := MarshalBlobTx([]byte("inner"), []Blob{blob})
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
	decoded := decodeBlobProto(t, blobs[0])
	if !bytes.Equal(decoded.namespaceID, blob.Namespace[1:]) {
		t.Fatalf("namespace id = %x, want %x", decoded.namespaceID, blob.Namespace[1:])
	}
	if decoded.namespaceVersion != uint64(blob.Namespace[0]) {
		t.Fatalf("namespace version = %d, want %d", decoded.namespaceVersion, blob.Namespace[0])
	}
	if !bytes.Equal(decoded.data, blob.Data) {
		t.Fatalf("data = %q, want %q", decoded.data, blob.Data)
	}
	if decoded.shareVersion != uint64(blob.ShareVersion) {
		t.Fatalf("share version = %d, want %d", decoded.shareVersion, blob.ShareVersion)
	}
	if !bytes.Equal(decoded.signer, blob.Signer) {
		t.Fatalf("signer = %q, want %q", decoded.signer, blob.Signer)
	}
	if typeID != types.ProtoBlobTxTypeID {
		t.Fatalf("type_id = %q, want %q", typeID, types.ProtoBlobTxTypeID)
	}
}

func TestMarshalBlobTxRejectsMissingInput(t *testing.T) {
	t.Parallel()

	if _, err := MarshalBlobTx(nil, []Blob{{Data: []byte("payload")}}); err == nil {
		t.Fatal("expected error when inner tx is missing")
	}
	if _, err := MarshalBlobTx([]byte("inner"), nil); err == nil {
		t.Fatal("expected error when blobs are missing")
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

type blobProtoFields struct {
	namespaceID      []byte
	data             []byte
	shareVersion     uint64
	namespaceVersion uint64
	signer           []byte
}

func decodeBlobProto(t *testing.T, raw []byte) blobProtoFields {
	t.Helper()

	var fields blobProtoFields

	data := raw
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			t.Fatal("failed to decode blob proto tag")
		}
		data = data[n:]

		switch typ {
		case protowire.BytesType:
			value, n := protowire.ConsumeBytes(data)
			if n < 0 {
				t.Fatalf("failed to decode blob proto field %d", num)
			}
			data = data[n:]
			switch num {
			case 1:
				fields.namespaceID = value
			case 2:
				fields.data = value
			case 5:
				fields.signer = value
			default:
				t.Fatalf("unexpected blob proto bytes field %d", num)
			}
		case protowire.VarintType:
			value, n := protowire.ConsumeVarint(data)
			if n < 0 {
				t.Fatalf("failed to decode blob proto field %d", num)
			}
			data = data[n:]
			switch num {
			case 3:
				fields.shareVersion = value
			case 4:
				fields.namespaceVersion = value
			default:
				t.Fatalf("unexpected blob proto varint field %d", num)
			}
		default:
			t.Fatalf("unexpected blob proto wire type %d for field %d", typ, num)
		}
	}

	return fields
}
