package submit

import (
	"errors"

	share "github.com/celestiaorg/go-square/v3/share"
	celestiatx "github.com/celestiaorg/go-square/v3/tx"
)

const (
	baseBlobTxGas            = 65_000
	firstSequenceGasOverhead = 10_000
	gasPerBlobByte           = 8
)

// MarshalBlobTx wraps a signed Cosmos SDK tx together with its blob payloads
// using Celestia's BlobTx wire format.
func MarshalBlobTx(innerTx []byte, blobs []Blob) ([]byte, error) {
	if len(innerTx) == 0 {
		return nil, errors.New("inner tx is required")
	}
	squareBlobs, err := convertSquareBlobs(blobs)
	if err != nil {
		return nil, err
	}
	return celestiatx.MarshalBlobTx(innerTx, squareBlobs...)
}

// EstimateGas returns the deterministic gas limit used for direct celestia-app
// submission. It mirrors the public Celestia guidance for PayForBlobs:
// a fixed base cost plus a share-count-based byte charge.
func EstimateGas(blobs []Blob, sequence uint64) (uint64, error) {
	squareBlobs, err := convertSquareBlobs(blobs)
	if err != nil {
		return 0, err
	}

	gas := uint64(baseBlobTxGas)
	if sequence == 0 {
		gas += firstSequenceGasOverhead
	}

	for _, blob := range squareBlobs {
		shares := share.SparseSharesNeeded(uint32(blob.DataLen()), blob.HasSigner())
		gas += uint64(shares * share.ShareSize * gasPerBlobByte)
	}

	return gas, nil
}
