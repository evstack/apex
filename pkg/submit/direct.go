package submit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	defaultFeeDenom        = "utia"
	defaultPollInterval    = time.Second
	maxSequenceRetryRounds = 2
)

var errSequenceMismatch = errors.New("account sequence mismatch")

// DirectConfig contains the fixed submission settings Apex owns for direct
// celestia-app writes.
type DirectConfig struct {
	ChainID             string
	GasPrice            float64
	MaxGasPrice         float64
	ConfirmationTimeout time.Duration
}

// DirectSubmitter signs and submits Celestia BlobTx payloads directly to
// celestia-app over Cosmos SDK gRPC.
type DirectSubmitter struct {
	app                 AppClient
	signer              *Signer
	chainID             string
	gasPrice            float64
	maxGasPrice         float64
	confirmationTimeout time.Duration
	pollInterval        time.Duration
	feeDenom            string
	mu                  sync.Mutex
}

// NewDirectSubmitter builds a concrete single-account submitter.
func NewDirectSubmitter(app AppClient, signer *Signer, cfg DirectConfig) (*DirectSubmitter, error) {
	if app == nil {
		return nil, errors.New("submission app client is required")
	}
	if signer == nil {
		return nil, errors.New("submission signer is required")
	}
	if cfg.ChainID == "" {
		return nil, errors.New("submission chain id is required")
	}
	if cfg.ConfirmationTimeout <= 0 {
		return nil, errors.New("submission confirmation timeout must be positive")
	}
	if cfg.GasPrice < 0 {
		return nil, errors.New("submission gas price must be non-negative")
	}
	if cfg.MaxGasPrice < 0 {
		return nil, errors.New("submission max gas price must be non-negative")
	}
	if cfg.MaxGasPrice > 0 && cfg.GasPrice > cfg.MaxGasPrice {
		return nil, errors.New("submission gas price must not exceed the max gas price")
	}

	return &DirectSubmitter{
		app:                 app,
		signer:              signer,
		chainID:             cfg.ChainID,
		gasPrice:            cfg.GasPrice,
		maxGasPrice:         cfg.MaxGasPrice,
		confirmationTimeout: cfg.ConfirmationTimeout,
		pollInterval:        defaultPollInterval,
		feeDenom:            defaultFeeDenom,
	}, nil
}

func (s *DirectSubmitter) Close() error {
	if s == nil || s.app == nil {
		return nil
	}
	return s.app.Close()
}

// Submit serializes submissions for the configured signer so sequence handling
// stays bounded and explicit in v1.
func (s *DirectSubmitter) Submit(ctx context.Context, req *Request) (*Result, error) {
	if err := validateSubmitRequest(req); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var lastErr error
	for range maxSequenceRetryRounds {
		result, err := s.submitOnce(ctx, req)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !errors.Is(err, errSequenceMismatch) {
			return nil, err
		}
	}

	return nil, lastErr
}

func validateSubmitRequest(req *Request) error {
	if req == nil {
		return errors.New("submission request is required")
	}
	if len(req.Blobs) == 0 {
		return errors.New("at least one blob is required")
	}
	for i := range req.Blobs {
		if _, err := convertSquareBlob(req.Blobs[i]); err != nil {
			return fmt.Errorf("validate submission blob %d: %w", i, err)
		}
	}
	return nil
}

func (s *DirectSubmitter) submitOnce(ctx context.Context, req *Request) (*Result, error) {
	account, err := s.app.AccountInfo(ctx, s.signer.Address())
	if err != nil {
		return nil, fmt.Errorf("query submission account: %w", err)
	}
	if account == nil {
		return nil, errors.New("query submission account: empty response")
	}

	txBytes, err := s.buildBlobTx(req, account)
	if err != nil {
		return nil, err
	}

	broadcast, err := s.app.BroadcastTx(ctx, txBytes)
	if err != nil {
		if isSequenceMismatchText(err.Error()) {
			return nil, fmt.Errorf("%w: %w", errSequenceMismatch, err)
		}
		return nil, fmt.Errorf("broadcast blob tx: %w", err)
	}
	if err := checkTxStatus("broadcast", broadcast); err != nil {
		return nil, err
	}

	return s.waitForConfirmation(ctx, broadcast.Hash)
}

func (s *DirectSubmitter) buildBlobTx(req *Request, account *AccountInfo) ([]byte, error) {
	pfb, err := BuildMsgPayForBlobs(s.signer.Address(), req.Blobs)
	if err != nil {
		return nil, fmt.Errorf("build pay-for-blobs message: %w", err)
	}
	msg, err := MarshalMsgPayForBlobsAny(pfb)
	if err != nil {
		return nil, err
	}

	gasLimit, gasPrice, err := s.resolveFees(req.Blobs, req.Options, account.Sequence)
	if err != nil {
		return nil, err
	}
	feeAmount, err := FeeAmountFromGasPrice(gasLimit, gasPrice)
	if err != nil {
		return nil, err
	}

	signerInfo, err := BuildSignerInfo(s.signer.PublicKey(), account.Sequence)
	if err != nil {
		return nil, err
	}
	authInfo, err := BuildAuthInfo(signerInfo, s.feeDenom, feeAmount, gasLimit, "", feeGranter(req.Options))
	if err != nil {
		return nil, err
	}
	authInfoBytes, err := MarshalAuthInfo(authInfo)
	if err != nil {
		return nil, err
	}
	bodyBytes, err := MarshalTxBody([]*anypb.Any{msg}, "", 0)
	if err != nil {
		return nil, err
	}
	signDocBytes, err := BuildSignDoc(bodyBytes, authInfoBytes, s.chainID, account.AccountNumber)
	if err != nil {
		return nil, err
	}
	signature, err := s.signer.Sign(signDocBytes)
	if err != nil {
		return nil, fmt.Errorf("sign pay-for-blobs tx: %w", err)
	}

	innerTx, err := MarshalTxRaw(bodyBytes, authInfoBytes, signature)
	if err != nil {
		return nil, err
	}
	return MarshalBlobTx(innerTx, req.Blobs)
}

func (s *DirectSubmitter) resolveFees(blobs []Blob, opts *TxConfig, sequence uint64) (uint64, float64, error) {
	if err := s.validateOptions(opts); err != nil {
		return 0, 0, err
	}

	gasPrice, err := resolveGasPrice(s.gasPrice, opts)
	if err != nil {
		return 0, 0, err
	}

	effectiveMaxGasPrice, err := resolveMaxGasPrice(s.maxGasPrice, opts)
	if err != nil {
		return 0, 0, err
	}
	if effectiveMaxGasPrice > 0 && gasPrice > effectiveMaxGasPrice {
		return 0, 0, fmt.Errorf("submission gas price %.6f exceeds the max gas price %.6f", gasPrice, effectiveMaxGasPrice)
	}

	if opts != nil && opts.Gas > 0 {
		return opts.Gas, gasPrice, nil
	}

	gasLimit, err := EstimateGas(blobs, sequence)
	if err != nil {
		return 0, 0, err
	}
	return gasLimit, gasPrice, nil
}

func (s *DirectSubmitter) validateOptions(opts *TxConfig) error {
	if opts == nil {
		return nil
	}
	if opts.KeyName != "" {
		return errors.New("submission tx option key_name is not supported by the direct submitter")
	}
	if opts.TxPriority != 0 {
		return errors.New("submission tx option tx_priority is not supported by the direct submitter")
	}
	if opts.SignerAddress != "" && opts.SignerAddress != s.signer.Address() {
		return fmt.Errorf("submission signer_address %q does not match configured signer %q", opts.SignerAddress, s.signer.Address())
	}
	return nil
}

func resolveGasPrice(defaultGasPrice float64, opts *TxConfig) (float64, error) {
	gasPrice := defaultGasPrice
	if opts != nil && (opts.IsGasPriceSet || opts.GasPrice > 0) {
		gasPrice = opts.GasPrice
	}
	if gasPrice <= 0 {
		return 0, errors.New("submission gas price must be configured or provided per request")
	}
	return gasPrice, nil
}

func resolveMaxGasPrice(defaultMaxGasPrice float64, opts *TxConfig) (float64, error) {
	effectiveMaxGasPrice := defaultMaxGasPrice
	if opts == nil {
		return effectiveMaxGasPrice, nil
	}
	if opts.MaxGasPrice < 0 {
		return 0, errors.New("submission tx option max_gas_price must be non-negative")
	}
	if opts.MaxGasPrice > 0 && (effectiveMaxGasPrice == 0 || opts.MaxGasPrice < effectiveMaxGasPrice) {
		effectiveMaxGasPrice = opts.MaxGasPrice
	}
	return effectiveMaxGasPrice, nil
}

func feeGranter(opts *TxConfig) string {
	if opts == nil {
		return ""
	}
	return opts.FeeGranterAddress
}

func (s *DirectSubmitter) waitForConfirmation(parent context.Context, hash string) (*Result, error) {
	if hash == "" {
		return nil, errors.New("broadcast returned an empty tx hash")
	}

	ctx, cancel := context.WithTimeout(parent, s.confirmationTimeout)
	defer cancel()

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		tx, err := s.app.GetTx(ctx, hash)
		if err == nil {
			if err := checkTxStatus("confirm", tx); err != nil {
				return nil, err
			}
			if tx.Height <= 0 {
				return nil, fmt.Errorf("confirm tx %s returned an invalid height %d", hash, tx.Height)
			}
			return &Result{Height: uint64(tx.Height)}, nil
		}
		if !isTxNotFound(err) {
			return nil, fmt.Errorf("confirm blob tx %s: %w", hash, err)
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("confirm blob tx %s: %w", hash, ctx.Err())
		case <-ticker.C:
		}
	}
}

func checkTxStatus(stage string, tx *TxStatus) error {
	if tx == nil {
		return fmt.Errorf("%s blob tx returned an empty response", stage)
	}
	if tx.Code == 0 {
		return nil
	}

	if isSequenceMismatchText(tx.RawLog) {
		return fmt.Errorf("%w: %s", errSequenceMismatch, tx.RawLog)
	}
	if tx.Codespace != "" {
		return fmt.Errorf("%s blob tx failed with code %d (%s): %s", stage, tx.Code, tx.Codespace, tx.RawLog)
	}
	return fmt.Errorf("%s blob tx failed with code %d: %s", stage, tx.Code, tx.RawLog)
}

func isSequenceMismatchText(text string) bool {
	text = strings.ToLower(text)
	return strings.Contains(text, "account sequence mismatch") || strings.Contains(text, "incorrect account sequence")
}

func isTxNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}
