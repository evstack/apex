package submit

import (
	"errors"
	"fmt"

	"github.com/evstack/apex/pkg/types"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	protoBlobTxTypeID                = "BLOB"
	shareSizeBytes                   = 512
	firstSparseShareContentSize      = 478
	continuationSparseShareDataBytes = 482
	baseBlobTxGas                    = 65_000
	firstSequenceGasOverhead         = 10_000
	gasPerBlobByte                   = 8
)

// MarshalBlobTx wraps a signed Cosmos SDK tx together with its blob payloads
// using Celestia's BlobTx wire format.
func MarshalBlobTx(innerTx []byte, blobs []Blob) ([]byte, error) {
	if len(innerTx) == 0 {
		return nil, errors.New("inner tx is required")
	}
	if len(blobs) == 0 {
		return nil, errors.New("at least one blob is required")
	}

	out := protowire.AppendTag(nil, 1, protowire.BytesType)
	out = protowire.AppendBytes(out, innerTx)
	for i := range blobs {
		blobBytes, err := marshalBlobProto(blobs[i])
		if err != nil {
			return nil, fmt.Errorf("marshal blob %d: %w", i, err)
		}
		out = protowire.AppendTag(out, 2, protowire.BytesType)
		out = protowire.AppendBytes(out, blobBytes)
	}
	out = protowire.AppendTag(out, 3, protowire.BytesType)
	out = protowire.AppendBytes(out, []byte(protoBlobTxTypeID))
	return out, nil
}

// EstimateGas returns the deterministic gas limit used for direct celestia-app
// submission. It mirrors the public Celestia guidance for PayForBlobs:
// a fixed base cost plus a share-count-based byte charge.
func EstimateGas(blobs []Blob, sequence uint64) (uint64, error) {
	if len(blobs) == 0 {
		return 0, errors.New("at least one blob is required")
	}

	gas := uint64(baseBlobTxGas)
	if sequence == 0 {
		gas += firstSequenceGasOverhead
	}

	for i := range blobs {
		shares, err := blobShareCount(len(blobs[i].Data))
		if err != nil {
			return 0, fmt.Errorf("estimate gas for blob %d: %w", i, err)
		}
		gas += uint64(shares * shareSizeBytes * gasPerBlobByte)
	}

	return gas, nil
}

func marshalBlobProto(blob Blob) ([]byte, error) {
	if len(blob.Data) == 0 {
		return nil, errors.New("blob data is required")
	}
	namespaceVersion := blob.Namespace[0]
	namespaceID := blob.Namespace[1:]
	if len(namespaceID) != types.NamespaceSize-1 {
		return nil, fmt.Errorf("invalid namespace size: got %d, want %d", len(blob.Namespace), types.NamespaceSize)
	}

	out := protowire.AppendTag(nil, 1, protowire.BytesType)
	out = protowire.AppendBytes(out, namespaceID)
	out = protowire.AppendTag(out, 2, protowire.BytesType)
	out = protowire.AppendBytes(out, blob.Data)
	out = protowire.AppendTag(out, 3, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(blob.ShareVersion))
	out = protowire.AppendTag(out, 4, protowire.VarintType)
	out = protowire.AppendVarint(out, uint64(namespaceVersion))
	if len(blob.Signer) > 0 {
		out = protowire.AppendTag(out, 5, protowire.BytesType)
		out = protowire.AppendBytes(out, blob.Signer)
	}
	return out, nil
}

func blobShareCount(dataLen int) (int, error) {
	if dataLen <= 0 {
		return 0, errors.New("blob data is required")
	}
	if dataLen <= firstSparseShareContentSize {
		return 1, nil
	}

	remaining := dataLen - firstSparseShareContentSize
	shares := 1 + ((remaining + continuationSparseShareDataBytes - 1) / continuationSparseShareDataBytes)
	return shares, nil
}
