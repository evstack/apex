package submit

import (
	"fmt"

	"github.com/celestiaorg/go-square/merkle"
	"github.com/celestiaorg/go-square/v3/inclusion"
	share "github.com/celestiaorg/go-square/v3/share"
	"github.com/evstack/apex/pkg/types"
)

const subtreeRootThreshold = 64

// BuildBlob validates a Celestia blob payload, computes its commitment, and
// returns the Apex-owned submission shape.
func BuildBlob(namespace types.Namespace, data []byte, shareVersion uint32, signer []byte) (Blob, error) {
	blob := Blob{
		Namespace:    namespace,
		Data:         data,
		ShareVersion: shareVersion,
		Signer:       signer,
		Index:        -1,
	}

	squareBlob, err := convertSquareBlob(blob)
	if err != nil {
		return Blob{}, err
	}

	commitment, err := inclusion.CreateCommitment(squareBlob, merkle.HashFromByteSlices, subtreeRootThreshold)
	if err != nil {
		return Blob{}, fmt.Errorf("create blob commitment: %w", err)
	}

	blob.Commitment = commitment
	return blob, nil
}

func convertSquareBlob(blob Blob) (*share.Blob, error) {
	namespace, err := share.NewNamespaceFromBytes(blob.Namespace[:])
	if err != nil {
		return nil, fmt.Errorf("build namespace: %w", err)
	}
	if err := namespace.ValidateForBlob(); err != nil {
		return nil, fmt.Errorf("invalid blob namespace: %w", err)
	}
	if blob.ShareVersion > uint32(share.MaxShareVersion) {
		return nil, fmt.Errorf("unsupported share version %d", blob.ShareVersion)
	}

	signer := blob.Signer
	if len(signer) == 0 {
		signer = nil
	}

	squareBlob, err := share.NewBlob(namespace, blob.Data, uint8(blob.ShareVersion), signer)
	if err != nil {
		return nil, fmt.Errorf("build celestia blob: %w", err)
	}
	return squareBlob, nil
}

func convertSquareBlobs(blobs []Blob) ([]*share.Blob, error) {
	converted := make([]*share.Blob, len(blobs))
	for i := range blobs {
		blob, err := convertSquareBlob(blobs[i])
		if err != nil {
			return nil, fmt.Errorf("blob %d: %w", i, err)
		}
		converted[i] = blob
	}
	return converted, nil
}
