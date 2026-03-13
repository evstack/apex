package submit

import (
	"fmt"

	share "github.com/celestiaorg/go-square/v3/share"
)

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
