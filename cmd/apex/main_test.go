package main

import (
	"testing"

	"github.com/evstack/apex/config"
)

func TestOpenBlobSubmitterDisabled(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.Submission.Enabled = false

	submitter, err := openBlobSubmitter(&cfg)
	if err != nil {
		t.Fatalf("openBlobSubmitter: %v", err)
	}
	if submitter != nil {
		t.Fatalf("expected nil submitter when submission disabled, got %T", submitter)
	}
}

func TestOpenBlobSubmitterCreatesSubmitter(t *testing.T) {
	t.Parallel()

	cfg := submissionConfigWithAddr("127.0.0.1:65535")
	submitter, err := openBlobSubmitter(cfg)
	if err != nil {
		t.Fatalf("openBlobSubmitter: %v", err)
	}
	if submitter == nil {
		t.Fatal("expected submitter but got nil")
	}
	if err := submitter.Close(); err != nil {
		t.Fatalf("closing submitter: %v", err)
	}
}

func TestOpenBlobSubmitterRejectsBadSignerKey(t *testing.T) {
	t.Parallel()

	cfg := submissionConfigWithAddr("127.0.0.1:65535")
	cfg.Submission.SignerPrivateKey = "zzzz"

	_, err := openBlobSubmitter(cfg)
	if err == nil {
		t.Fatal("expected error when signer key is invalid")
	}
}

func submissionConfigWithAddr(addr string) *config.Config {
	cfg := config.DefaultConfig()
	cfg.Submission.Enabled = true
	cfg.Submission.CelestiaAppGRPCAddr = addr
	cfg.Submission.ChainID = "mocha-4"
	cfg.Submission.GasPrice = 0.001
	cfg.Submission.ConfirmationTimeout = 30
	cfg.Submission.SignerPrivateKey = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	return &cfg
}
