package fetch

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/evstack/apex/pkg/types"
)

const (
	// legacyBlobTxTypeID is the trailing byte used by older BlobTx encodings.
	legacyBlobTxTypeID = 0x62
	// protoBlobTxTypeID is the current protobuf type identifier used by Celestia.
	protoBlobTxTypeID = "BLOB"
	// msgPayForBlobsTypeURL is the protobuf Any type URL for MsgPayForBlobs.
	msgPayForBlobsTypeURL = "/celestia.blob.v1.MsgPayForBlobs"
)

// rawBlob holds the fields parsed from a BlobTx blob proto message.
type rawBlob struct {
	Namespace        []byte
	Data             []byte
	ShareVersion     uint32
	NamespaceVersion uint32
	Signer           []byte
}

// pfbData holds the fields extracted from MsgPayForBlobs.
type pfbData struct {
	Signer           []byte
	ShareCommitments [][]byte
}

// parsedBlobTx is the result of fully parsing a BlobTx: blobs + their
// commitments and signer from MsgPayForBlobs in the inner SDK tx.
type parsedBlobTx struct {
	Blobs []rawBlob
	PFB   pfbData
}

// parseBlobTx decodes either the current protobuf BlobTx envelope or the older
// legacy envelope and extracts both the blobs and the MsgPayForBlobs data
// (commitments, signer) from the inner tx.
func parseBlobTx(raw []byte) (*parsedBlobTx, error) {
	if len(raw) == 0 {
		return nil, errors.New("empty BlobTx")
	}

	parsed, protoErr := parseProtoBlobTx(raw)
	if protoErr == nil {
		return parsed, nil
	}

	parsed, legacyErr := parseLegacyBlobTx(raw)
	if legacyErr == nil {
		return parsed, nil
	}

	return nil, fmt.Errorf("decode blob tx: %w", errors.Join(protoErr, legacyErr))
}

func parseProtoBlobTx(raw []byte) (*parsedBlobTx, error) {
	envelope := protoBlobTxEnvelope{}

	data := raw
	for len(data) > 0 {
		next, err := envelope.consume(data)
		if err != nil {
			return nil, err
		}
		data = next
	}

	if err := envelope.validate(); err != nil {
		return nil, err
	}

	pfb, err := parsePFBFromTx(envelope.innerTx)
	if err != nil {
		return nil, fmt.Errorf("parse inner tx: %w", err)
	}

	return &parsedBlobTx{Blobs: envelope.blobs, PFB: pfb}, nil
}

type protoBlobTxEnvelope struct {
	innerTx []byte
	blobs   []rawBlob
	typeID  string
}

func (e *protoBlobTxEnvelope) consume(data []byte) ([]byte, error) {
	num, typ, n := protowire.ConsumeTag(data)
	if n < 0 {
		return nil, errors.New("invalid proto tag")
	}
	data = data[n:]

	switch {
	case num == 1 && typ == protowire.BytesType:
		val, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, errors.New("decode inner tx: invalid bytes")
		}
		e.innerTx = append([]byte(nil), val...)
		return data[n:], nil
	case num == 2 && typ == protowire.BytesType:
		val, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, fmt.Errorf("decode blob %d: invalid bytes", len(e.blobs))
		}

		b, err := parseRawBlob(val)
		if err != nil {
			return nil, fmt.Errorf("parse blob %d: %w", len(e.blobs), err)
		}
		e.blobs = append(e.blobs, b)
		return data[n:], nil
	case num == 3 && typ == protowire.BytesType:
		val, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, errors.New("decode BlobTx type_id: invalid bytes")
		}
		e.typeID = string(val)
		return data[n:], nil
	default:
		n = protowire.ConsumeFieldValue(num, typ, data)
		if n < 0 {
			return nil, fmt.Errorf("field %d: invalid value for wire type %d", num, typ)
		}
		return data[n:], nil
	}
}

func (e protoBlobTxEnvelope) validate() error {
	if e.typeID != protoBlobTxTypeID {
		return fmt.Errorf("not a proto BlobTx: type_id %q", e.typeID)
	}
	if len(e.innerTx) == 0 {
		return errors.New("BlobTx has no inner tx")
	}
	if len(e.blobs) == 0 {
		return errors.New("BlobTx has no blobs")
	}
	return nil
}

// parseLegacyBlobTx decodes the older BlobTx wire format:
//
//	inner_tx (length-prefixed) || blob1 (length-prefixed) || blob2 ... || 0x62
func parseLegacyBlobTx(raw []byte) (*parsedBlobTx, error) {
	if raw[len(raw)-1] != legacyBlobTxTypeID {
		return nil, fmt.Errorf("not a legacy BlobTx: trailing byte 0x%02x, want 0x%02x", raw[len(raw)-1], legacyBlobTxTypeID)
	}

	data := raw[:len(raw)-1]

	innerTx, n := protowire.ConsumeBytes(data)
	if n < 0 {
		return nil, errors.New("decode inner tx: invalid length prefix")
	}
	data = data[n:]

	pfb, err := parsePFBFromTx(innerTx)
	if err != nil {
		return nil, fmt.Errorf("parse inner tx: %w", err)
	}

	// Remaining bytes are length-prefixed blob proto messages.
	var blobs []rawBlob
	for len(data) > 0 {
		blobBytes, bn := protowire.ConsumeBytes(data)
		if bn < 0 {
			return nil, fmt.Errorf("decode blob %d: invalid length prefix", len(blobs))
		}
		data = data[bn:]

		b, err := parseRawBlob(blobBytes)
		if err != nil {
			return nil, fmt.Errorf("parse blob %d: %w", len(blobs), err)
		}
		blobs = append(blobs, b)
	}

	return &parsedBlobTx{Blobs: blobs, PFB: pfb}, nil
}

// parsePFBFromTx extracts MsgPayForBlobs data from a Cosmos SDK tx.
//
// Proto chain:
//
//	cosmos.tx.v1beta1.Tx        → field 1: body (bytes)
//	cosmos.tx.v1beta1.TxBody    → field 1: messages (repeated bytes/Any)
//	google.protobuf.Any         → field 1: type_url (string), field 2: value (bytes)
//	celestia.blob.v1.MsgPayForBlobs → field 1: signer (string), field 4: share_commitments (repeated bytes)
func parsePFBFromTx(txBytes []byte) (pfbData, error) {
	// Extract TxBody (field 1) from Tx.
	bodyBytes, err := extractBytesField(txBytes, 1)
	if err != nil {
		return pfbData{}, fmt.Errorf("extract tx body: %w", err)
	}
	if bodyBytes == nil {
		return pfbData{}, errors.New("tx has no body")
	}

	// Iterate messages (field 1, repeated) in TxBody to find MsgPayForBlobs.
	messages := extractRepeatedBytesField(bodyBytes, 1)
	for _, msgAny := range messages {
		typeURL, value, err := parseAny(msgAny)
		if err != nil {
			continue
		}
		if typeURL != msgPayForBlobsTypeURL {
			continue
		}
		return parseMsgPayForBlobs(value)
	}

	return pfbData{}, errors.New("no MsgPayForBlobs found in tx")
}

// parseMsgPayForBlobs extracts signer and share_commitments from MsgPayForBlobs.
//
//	field 1: signer (string)
//	field 4: share_commitments (repeated bytes)
func parseMsgPayForBlobs(data []byte) (pfbData, error) {
	var result pfbData
	buf := data
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return pfbData{}, errors.New("invalid tag in MsgPayForBlobs")
		}
		buf = buf[n:]

		switch typ {
		case protowire.BytesType:
			val, n := protowire.ConsumeBytes(buf)
			if n < 0 {
				return pfbData{}, fmt.Errorf("field %d: invalid bytes", num)
			}
			buf = buf[n:]
			switch num {
			case 1: // signer (string, but wire type is bytes)
				result.Signer = append([]byte(nil), val...)
			case 4: // share_commitments (repeated bytes)
				result.ShareCommitments = append(result.ShareCommitments, append([]byte(nil), val...))
			default:
			}
		case protowire.VarintType:
			_, n := protowire.ConsumeVarint(buf)
			if n < 0 {
				return pfbData{}, fmt.Errorf("field %d: invalid varint", num)
			}
			buf = buf[n:]
		default:
			// Skip unknown wire types for forward compatibility.
			n = protowire.ConsumeFieldValue(num, typ, buf)
			if n < 0 {
				return pfbData{}, fmt.Errorf("field %d: invalid value for wire type %d", num, typ)
			}
			buf = buf[n:]
		}
	}
	return result, nil
}

// parseAny decodes a google.protobuf.Any message.
//
//	field 1: type_url (string), field 2: value (bytes)
func parseAny(data []byte) (typeURL string, value []byte, err error) {
	buf := data
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return "", nil, errors.New("invalid tag")
		}
		buf = buf[n:]

		if typ != protowire.BytesType {
			// Skip non-bytes fields.
			n = protowire.ConsumeFieldValue(num, typ, buf)
			if n < 0 {
				return "", nil, fmt.Errorf("field %d: invalid value", num)
			}
			buf = buf[n:]
			continue
		}

		val, n := protowire.ConsumeBytes(buf)
		if n < 0 {
			return "", nil, fmt.Errorf("field %d: invalid bytes", num)
		}
		buf = buf[n:]

		switch num {
		case 1:
			typeURL = string(val)
		case 2:
			value = val
		default:
		}
	}
	return typeURL, value, nil
}

// parseRawBlob decodes a single blob protobuf message from BlobTx.
//
// Blob fields: 1: namespace (bytes), 2: data (bytes), 3: share_version (uint32),
// 4: namespace_version (uint32), 5: signer (bytes, celestia-app v2+)
func parseRawBlob(data []byte) (rawBlob, error) {
	var b rawBlob
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return rawBlob{}, errors.New("invalid proto tag")
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
			case 5: // signer (celestia-app v2+)
				b.Signer = append([]byte(nil), val...)
			default:
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
			default:
			}
		default:
			// Skip unknown wire types for forward compatibility.
			n = protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return rawBlob{}, fmt.Errorf("field %d: invalid value for wire type %d", num, typ)
			}
			data = data[n:]
		}
	}
	return b, nil
}

// extractBlobsFromBlock iterates over block transactions, parses BlobTxs,
// and returns blobs matching any of the given namespaces. Commitments and
// signer are sourced from MsgPayForBlobs in the inner SDK tx.
func extractBlobsFromBlock(txs [][]byte, namespaces []types.Namespace, height uint64) ([]types.Blob, error) {
	nsSet := make(map[types.Namespace]struct{}, len(namespaces))
	for _, ns := range namespaces {
		nsSet[ns] = struct{}{}
	}

	var result []types.Blob
	blobIndex := 0

	for _, tx := range txs {
		if len(tx) == 0 {
			continue
		}

		parsed, err := parseBlobTx(tx)
		if err != nil {
			// Skip malformed BlobTxs rather than failing the whole block.
			continue
		}

		for i, rb := range parsed.Blobs {
			ns, ok := namespaceFromRawBlob(rb)
			if !ok {
				blobIndex++
				continue
			}

			if _, ok := nsSet[ns]; !ok {
				blobIndex++
				continue
			}

			// Commitment comes from MsgPayForBlobs, matched by index.
			var commitment []byte
			if i < len(parsed.PFB.ShareCommitments) {
				commitment = parsed.PFB.ShareCommitments[i]
			}

			// Signer: prefer MsgPayForBlobs.signer, fall back to blob-level signer (v2+).
			signer := parsed.PFB.Signer
			if len(signer) == 0 {
				signer = rb.Signer
			}

			result = append(result, types.Blob{
				Height:       height,
				Namespace:    ns,
				Data:         rb.Data,
				Commitment:   commitment,
				ShareVersion: rb.ShareVersion,
				Signer:       signer,
				Index:        blobIndex,
			})
			blobIndex++
		}
	}

	return result, nil
}

// ExtractBlobsFromBlock parses BlobTx transactions and returns blobs matching
// the requested namespaces. It is used by both RPC and DB backfill paths.
func ExtractBlobsFromBlock(txs [][]byte, namespaces []types.Namespace, height uint64) ([]types.Blob, error) {
	return extractBlobsFromBlock(txs, namespaces, height)
}

func namespaceFromRawBlob(blob rawBlob) (types.Namespace, bool) {
	switch len(blob.Namespace) {
	case types.NamespaceSize:
		var ns types.Namespace
		copy(ns[:], blob.Namespace)
		return ns, true
	case types.NamespaceSize - 1:
		if blob.NamespaceVersion > 0xff {
			return types.Namespace{}, false
		}
		var ns types.Namespace
		ns[0] = byte(blob.NamespaceVersion)
		copy(ns[1:], blob.Namespace)
		return ns, true
	default:
		return types.Namespace{}, false
	}
}

// extractBytesField returns the first occurrence of a bytes-typed field.
func extractBytesField(data []byte, target protowire.Number) ([]byte, error) {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, errors.New("invalid tag")
		}
		data = data[n:]

		if typ == protowire.BytesType {
			val, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, fmt.Errorf("field %d: invalid bytes", num)
			}
			data = data[n:]
			if num == target {
				return val, nil
			}
			continue
		}

		n = protowire.ConsumeFieldValue(num, typ, data)
		if n < 0 {
			return nil, fmt.Errorf("field %d: invalid value", num)
		}
		data = data[n:]
	}
	return nil, nil
}

// extractRepeatedBytesField returns all occurrences of a bytes-typed field.
func extractRepeatedBytesField(data []byte, target protowire.Number) [][]byte {
	var result [][]byte
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return result
		}
		data = data[n:]

		if typ == protowire.BytesType {
			val, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return result
			}
			data = data[n:]
			if num == target {
				result = append(result, val)
			}
			continue
		}

		n = protowire.ConsumeFieldValue(num, typ, data)
		if n < 0 {
			return result
		}
		data = data[n:]
	}
	return result
}
