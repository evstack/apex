package fetch

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/evstack/apex/pkg/types"
)

// blobTxTypeID is the trailing byte that identifies a BlobTx.
// Defined in celestia-app as 0x62 ('b').
const blobTxTypeID = 0x62

// rawBlob holds the fields parsed from a BlobTx blob proto message.
type rawBlob struct {
	Namespace        []byte
	Data             []byte
	ShareVersion     uint32
	NamespaceVersion uint32
	ShareCommitment  []byte
}

// parseBlobTx decodes a Celestia BlobTx envelope.
//
// BlobTx wire format:
//
//	inner_tx (length-prefixed) || blob1 (length-prefixed) || blob2 ... || 0x62
//
// Each blob is a protobuf message with fields:
//
//	1: namespace (bytes), 2: data (bytes), 3: share_version (uint32),
//	4: namespace_version (uint32), 5: share_commitment (bytes)
func parseBlobTx(raw []byte) ([]rawBlob, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty BlobTx")
	}
	if raw[len(raw)-1] != blobTxTypeID {
		return nil, fmt.Errorf("not a BlobTx: trailing byte 0x%02x, want 0x%02x", raw[len(raw)-1], blobTxTypeID)
	}

	// Strip trailing type byte.
	data := raw[:len(raw)-1]

	// Skip inner SDK tx (length-prefixed).
	_, n := protowire.ConsumeBytes(data)
	if n < 0 {
		return nil, fmt.Errorf("decode inner tx: invalid length prefix")
	}
	data = data[n:]

	// Remaining bytes are length-prefixed blob proto messages.
	var blobs []rawBlob
	for len(data) > 0 {
		blobBytes, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, fmt.Errorf("decode blob %d: invalid length prefix", len(blobs))
		}
		data = data[n:]

		b, err := parseRawBlob(blobBytes)
		if err != nil {
			return nil, fmt.Errorf("parse blob %d: %w", len(blobs), err)
		}
		blobs = append(blobs, b)
	}

	return blobs, nil
}

// parseRawBlob decodes a single blob protobuf message.
func parseRawBlob(data []byte) (rawBlob, error) {
	var b rawBlob
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return rawBlob{}, fmt.Errorf("invalid proto tag")
		}
		data = data[n:]

		switch typ {
		case protowire.BytesType:
			val, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return rawBlob{}, fmt.Errorf("field %d: invalid bytes", num)
			}
			data = data[n:]
			switch num {
			case 1: // namespace
				b.Namespace = append([]byte(nil), val...)
			case 2: // data
				b.Data = append([]byte(nil), val...)
			case 5: // share_commitment
				b.ShareCommitment = append([]byte(nil), val...)
			}
		case protowire.VarintType:
			val, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return rawBlob{}, fmt.Errorf("field %d: invalid varint", num)
			}
			data = data[n:]
			switch num {
			case 3: // share_version
				b.ShareVersion = uint32(val)
			case 4: // namespace_version
				b.NamespaceVersion = uint32(val)
			}
		default:
			return rawBlob{}, fmt.Errorf("field %d: unsupported wire type %d", num, typ)
		}
	}
	return b, nil
}

// extractBlobsFromBlock iterates over block transactions, parses BlobTxs,
// and returns blobs matching any of the given namespaces.
func extractBlobsFromBlock(txs [][]byte, namespaces []types.Namespace, height uint64) ([]types.Blob, error) {
	nsSet := make(map[types.Namespace]struct{}, len(namespaces))
	for _, ns := range namespaces {
		nsSet[ns] = struct{}{}
	}

	var result []types.Blob
	blobIndex := 0

	for _, tx := range txs {
		if len(tx) == 0 || tx[len(tx)-1] != blobTxTypeID {
			continue
		}

		rawBlobs, err := parseBlobTx(tx)
		if err != nil {
			// Skip malformed BlobTxs rather than failing the whole block.
			continue
		}

		for _, rb := range rawBlobs {
			if len(rb.Namespace) != types.NamespaceSize {
				continue
			}
			var ns types.Namespace
			copy(ns[:], rb.Namespace)

			if _, ok := nsSet[ns]; !ok {
				blobIndex++
				continue
			}

			result = append(result, types.Blob{
				Height:       height,
				Namespace:    ns,
				Data:         rb.Data,
				Commitment:   rb.ShareCommitment,
				ShareVersion: rb.ShareVersion,
				Index:        blobIndex,
			})
			blobIndex++
		}
	}

	return result, nil
}
