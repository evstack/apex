package submit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/evstack/apex/pkg/types"
)

// ErrDisabled reports that no submission backend is configured.
var ErrDisabled = errors.New("blob submission not configured")

// Priority mirrors celestia-node's JSON tx priority values.
type Priority int

const (
	PriorityLow Priority = iota + 1
	PriorityMedium
	PriorityHigh
)

// Blob is the Apex-owned wire shape for blob submission requests.
// It intentionally matches celestia-node's JSON-RPC payload fields without
// depending on celestia-node or cosmos-sdk types.
type Blob struct {
	Namespace    types.Namespace
	Data         []byte
	ShareVersion uint8
	Commitment   []byte
	Signer       []byte
	Index        int
}

// TxConfig mirrors celestia-node's JSON submit options.
type TxConfig struct {
	GasPrice          float64  `json:"gas_price,omitempty"`
	IsGasPriceSet     bool     `json:"is_gas_price_set,omitempty"`
	MaxGasPrice       float64  `json:"max_gas_price"`
	Gas               uint64   `json:"gas,omitempty"`
	TxPriority        Priority `json:"tx_priority,omitempty"`
	KeyName           string   `json:"key_name,omitempty"`
	SignerAddress     string   `json:"signer_address,omitempty"`
	FeeGranterAddress string   `json:"fee_granter_address,omitempty"`
}

// Request is the typed input for a blob submission.
type Request struct {
	Blobs   []Blob
	Options *TxConfig
}

// Result is the typed result for a confirmed blob submission.
type Result struct {
	Height uint64
}

// Submitter is the Apex-owned submission boundary. Implementations may be
// dependency-free or backed by a separate module, but callers only see these
// local types.
type Submitter interface {
	Submit(ctx context.Context, req *Request) (*Result, error)
}

type jsonBlob struct {
	Namespace    []byte `json:"namespace"`
	Data         []byte `json:"data"`
	ShareVersion uint8  `json:"share_version"`
	Commitment   []byte `json:"commitment"`
	Signer       []byte `json:"signer,omitempty"`
	Index        int    `json:"index"`
}

// DecodeRequest converts JSON-RPC blob.Submit params into Apex-owned types.
func DecodeRequest(blobsRaw, optionsRaw json.RawMessage) (*Request, error) {
	blobs, err := decodeBlobs(blobsRaw)
	if err != nil {
		return nil, err
	}
	opts, err := decodeOptions(optionsRaw)
	if err != nil {
		return nil, err
	}
	return &Request{
		Blobs:   blobs,
		Options: opts,
	}, nil
}

// MarshalResult encodes a submission result using the JSON-RPC-compatible
// return shape expected by blob.Submit.
func MarshalResult(result *Result) (json.RawMessage, error) {
	if result == nil {
		return nil, errors.New("submission result is required")
	}
	raw, err := json.Marshal(result.Height)
	if err != nil {
		return nil, fmt.Errorf("marshal submit result: %w", err)
	}
	return raw, nil
}

func decodeBlobs(raw json.RawMessage) ([]Blob, error) {
	var blobsJSON []jsonBlob
	if err := json.Unmarshal(raw, &blobsJSON); err != nil {
		return nil, fmt.Errorf("decode submit blobs: %w", err)
	}

	blobs := make([]Blob, len(blobsJSON))
	for i := range blobsJSON {
		ns, err := namespaceFromBytes(blobsJSON[i].Namespace)
		if err != nil {
			return nil, fmt.Errorf("decode submit blob %d namespace: %w", i, err)
		}
		blobs[i] = Blob{
			Namespace:    ns,
			Data:         blobsJSON[i].Data,
			ShareVersion: blobsJSON[i].ShareVersion,
			Commitment:   blobsJSON[i].Commitment,
			Signer:       blobsJSON[i].Signer,
			Index:        blobsJSON[i].Index,
		}
	}

	return blobs, nil
}

func decodeOptions(raw json.RawMessage) (*TxConfig, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}

	var cfg TxConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("decode submit options: %w", err)
	}
	return &cfg, nil
}

func namespaceFromBytes(b []byte) (types.Namespace, error) {
	return types.NamespaceFromBytes(b)
}
