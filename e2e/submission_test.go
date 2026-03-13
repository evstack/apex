package e2e

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/celestiaorg/go-square/merkle"
	"github.com/celestiaorg/go-square/v3/inclusion"
	gsquare "github.com/celestiaorg/go-square/v3/share"
	tastoradocker "github.com/celestiaorg/tastora/framework/docker"
	tastoracontainer "github.com/celestiaorg/tastora/framework/docker/container"
	tastoracosmos "github.com/celestiaorg/tastora/framework/docker/cosmos"
	sdkcrypto "github.com/cosmos/cosmos-sdk/crypto"
	"github.com/cosmos/cosmos-sdk/types/module/testutil"
	auth "github.com/cosmos/cosmos-sdk/x/auth"
	bank "github.com/cosmos/cosmos-sdk/x/bank"
	govmodule "github.com/cosmos/cosmos-sdk/x/gov"
	ibctransfer "github.com/cosmos/ibc-go/v8/modules/apps/transfer"
	"github.com/evstack/apex/pkg/types"
)

const (
	submissionPollLimit      = 60
	submissionPollInterval   = time.Second
	commitmentThreshold      = 64
	celestiaAppImage         = "ghcr.io/celestiaorg/celestia-app"
	celestiaAppVersion       = "v5.0.10"
	celestiaAppUser          = "10001:10001"
	submissionGasPrice       = 0.1
	submissionConfirmTimeout = 30
	chainStartupTimeout      = 10 * time.Minute
	apexReadyTimeout         = 60 * time.Second
)

func TestSubmissionViaJSONRPC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Docker-backed e2e submission test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), chainStartupTimeout)
	defer cancel()

	grpcAddr, chainID, signerKeyHex, signerAddress := startSubmissionTestChain(t, ctx)
	namespace := testNamespace(t, []byte("apex-e2e"))
	data := []byte("apex tastora submission e2e")
	commitment := mustBlobCommitment(t, namespace, data)

	apexBinary := buildApexBinary(t)
	apexRPCAddr := reserveTCPAddr(t)
	apexGRPCAddr := reserveTCPAddr(t)
	keyPath := writeSignerKey(t, signerKeyHex)
	configPath := writeApexConfig(t, apexConfig{
		Namespace:       namespace,
		DataGRPCAddr:    grpcAddr,
		SubmissionGRPC:  grpcAddr,
		ChainID:         chainID,
		SignerKeyPath:   keyPath,
		StoragePath:     filepath.Join(t.TempDir(), "apex.db"),
		RPCListenAddr:   apexRPCAddr,
		GRPCListenAddr:  apexGRPCAddr,
		GasPrice:        submissionGasPrice,
		MaxGasPrice:     submissionGasPrice,
		ConfirmTimeoutS: submissionConfirmTimeout,
	})

	proc := startApexProcess(t, apexBinary, configPath)
	defer proc.Stop(t)

	waitForApexHTTP(t, proc, apexRPCAddr)

	resp := doRPC(
		t,
		proc,
		apexRPCAddr,
		"blob.Submit",
		[]map[string]any{{
			"namespace":     namespace,
			"data":          data,
			"share_version": 0,
			"commitment":    commitment,
			"index":         -1,
		}},
		map[string]any{
			"gas_price":        submissionGasPrice,
			"is_gas_price_set": true,
		},
	)
	if resp.Error != nil {
		t.Fatalf("blob.Submit RPC error: %s", resp.Error.Message)
	}

	height := decodeSubmitHeight(t, resp.Result)
	if height == 0 {
		t.Fatal("submission height must be positive")
	}

	waitForIndexedBlob(t, proc, apexRPCAddr, commitment, data, namespace, signerAddress)
}

type apexConfig struct {
	Namespace       []byte
	DataGRPCAddr    string
	SubmissionGRPC  string
	ChainID         string
	SignerKeyPath   string
	StoragePath     string
	RPCListenAddr   string
	GRPCListenAddr  string
	GasPrice        float64
	MaxGasPrice     float64
	ConfirmTimeoutS int
}

func startSubmissionTestChain(t *testing.T, ctx context.Context) (grpcAddr string, chainID string, signerKeyHex string, signerAddress string) {
	t.Helper()

	dockerClient, networkID := tastoradocker.Setup(t)
	encodingConfig := testutil.MakeTestEncodingConfig(
		auth.AppModuleBasic{},
		bank.AppModuleBasic{},
		ibctransfer.AppModuleBasic{},
		govmodule.AppModuleBasic{},
	)

	builder := tastoracosmos.NewChainBuilderWithTestName(t, t.Name()).
		WithDockerClient(dockerClient).
		WithDockerNetworkID(networkID).
		WithImage(tastoracontainer.NewImage(celestiaAppImage, celestiaAppVersion, celestiaAppUser)).
		WithEncodingConfig(&encodingConfig).
		WithAdditionalStartArgs(
			"--force-no-bbr",
			"--grpc.enable",
			"--grpc.address", "0.0.0.0:9090",
			"--rpc.grpc_laddr=tcp://0.0.0.0:9098",
			"--timeout-commit", "1s",
			"--minimum-gas-prices", "0utia",
		).
		WithNode(tastoracosmos.NewChainNodeConfigBuilder().Build())

	chain, err := builder.Build(ctx)
	if err != nil {
		t.Fatalf("build Tastora chain: %v", err)
	}
	if err := chain.Start(ctx); err != nil {
		t.Fatalf("start Tastora chain: %v", err)
	}

	networkInfo, err := chain.GetNetworkInfo(ctx)
	if err != nil {
		t.Fatalf("get chain network info: %v", err)
	}

	faucetWallet := chain.GetFaucetWallet()
	return networkInfo.External.GRPCAddress(), chain.GetChainID(), exportWalletPrivateKeyHex(t, chain, faucetWallet.GetKeyName()), faucetWallet.GetFormattedAddress()
}

func exportWalletPrivateKeyHex(t *testing.T, chain *tastoracosmos.Chain, keyName string) string {
	t.Helper()

	keyring, err := chain.GetNode().GetKeyring()
	if err != nil {
		t.Fatalf("get keyring: %v", err)
	}

	armoredKey, err := keyring.ExportPrivKeyArmor(keyName, "")
	if err != nil {
		t.Fatalf("export private key: %v", err)
	}

	privateKey, _, err := sdkcrypto.UnarmorDecryptPrivKey(armoredKey, "")
	if err != nil {
		t.Fatalf("decrypt private key: %v", err)
	}

	return hex.EncodeToString(privateKey.Bytes())
}

func buildApexBinary(t *testing.T) string {
	t.Helper()

	binaryPath := filepath.Join(t.TempDir(), "apex")
	cmd := exec.Command("go", "build", "-C", "..", "-o", binaryPath, "./cmd/apex")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build apex binary: %v\n%s", err, output)
	}
	return binaryPath
}

func writeSignerKey(t *testing.T, signerKeyHex string) string {
	t.Helper()

	keyPath := filepath.Join(t.TempDir(), "submission.key")
	if err := os.WriteFile(keyPath, []byte(signerKeyHex), 0o600); err != nil {
		t.Fatalf("write signer key: %v", err)
	}
	return keyPath
}

func writeApexConfig(t *testing.T, cfg apexConfig) string {
	t.Helper()

	configPath := filepath.Join(t.TempDir(), "apex.yaml")
	configYAML := fmt.Sprintf(`data_source:
  type: "app"
  celestia_app_grpc_addr: "%s"
  backfill_source: "rpc"
  namespaces:
    - "%s"

submission:
  enabled: true
  app_grpc_addr: "%s"
  chain_id: "%s"
  signer_key: "%s"
  gas_price: %.6f
  max_gas_price: %.6f
  confirmation_timeout: %d

storage:
  type: "sqlite"
  db_path: "%s"

rpc:
  listen_addr: "%s"
  grpc_listen_addr: "%s"
  read_timeout: 30
  write_timeout: 30

sync:
  start_height: 1
  batch_size: 8
  concurrency: 1

subscription:
  buffer_size: 16
  max_subscribers: 32

metrics:
  enabled: false

profiling:
  enabled: false

log:
  level: "info"
  format: "console"
`,
		cfg.DataGRPCAddr,
		hex.EncodeToString(cfg.Namespace),
		cfg.SubmissionGRPC,
		cfg.ChainID,
		cfg.SignerKeyPath,
		cfg.GasPrice,
		cfg.MaxGasPrice,
		cfg.ConfirmTimeoutS,
		cfg.StoragePath,
		cfg.RPCListenAddr,
		cfg.GRPCListenAddr,
	)

	if err := os.WriteFile(configPath, []byte(configYAML), 0o600); err != nil {
		t.Fatalf("write apex config: %v", err)
	}
	return configPath
}

type apexProcess struct {
	cmd  *exec.Cmd
	done chan error
	logs *bytes.Buffer
}

func startApexProcess(t *testing.T, binaryPath string, configPath string) *apexProcess {
	t.Helper()

	cmd := exec.Command(binaryPath, "--config", configPath, "start")
	cmd.Dir = ".."

	var logs bytes.Buffer
	cmd.Stdout = &logs
	cmd.Stderr = &logs

	if err := cmd.Start(); err != nil {
		t.Fatalf("start apex process: %v", err)
	}

	proc := &apexProcess{
		cmd:  cmd,
		done: make(chan error, 1),
		logs: &logs,
	}
	go func() {
		proc.done <- cmd.Wait()
	}()
	return proc
}

func (p *apexProcess) Stop(t *testing.T) {
	t.Helper()

	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}

	_ = p.cmd.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out stopping apex process\n%s", p.logs.String())
	}
}

func waitForApexHTTP(t *testing.T, proc *apexProcess, rpcAddr string) {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	url := "http://" + rpcAddr + "/health"

	deadline := time.Now().Add(apexReadyTimeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-proc.done:
			t.Fatalf("apex exited before becoming ready: %v\n%s", err, proc.logs.String())
		default:
		}

		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(submissionPollInterval)
	}

	t.Fatalf("apex HTTP endpoint did not become reachable at %s\n%s", url, proc.logs.String())
}

func mustBlobCommitment(t *testing.T, namespace []byte, data []byte) []byte {
	t.Helper()

	blobNamespace, err := gsquare.NewNamespaceFromBytes(namespace)
	if err != nil {
		t.Fatalf("build blob namespace: %v", err)
	}
	if err := blobNamespace.ValidateForBlob(); err != nil {
		t.Fatalf("validate blob namespace: %v", err)
	}

	blob, err := gsquare.NewV0Blob(blobNamespace, data)
	if err != nil {
		t.Fatalf("build blob: %v", err)
	}

	commitment, err := inclusion.CreateCommitment(blob, merkle.HashFromByteSlices, commitmentThreshold)
	if err != nil {
		t.Fatalf("create blob commitment: %v", err)
	}
	return commitment
}

func waitForIndexedBlob(t *testing.T, proc *apexProcess, rpcAddr string, commitment []byte, wantData []byte, wantNamespace []byte, wantSigner string) {
	t.Helper()

	for range submissionPollLimit {
		select {
		case err := <-proc.done:
			t.Fatalf("apex exited before blob was indexed: %v\n%s", err, proc.logs.String())
		default:
		}

		resp := doRPC(t, proc, rpcAddr, "blob.GetByCommitment", commitment)
		if resp.Error == nil && len(resp.Result) > 0 && string(resp.Result) != "null" {
			var blob rpcBlob
			if err := json.Unmarshal(resp.Result, &blob); err != nil {
				t.Fatalf("decode blob result: %v", err)
			}
			if !bytes.Equal(blob.Data, wantData) {
				t.Fatalf("blob data = %q, want %q", blob.Data, wantData)
			}
			if !bytes.Equal(blob.Namespace, wantNamespace) {
				t.Fatalf("blob namespace = %x, want %x", blob.Namespace, wantNamespace)
			}
			if !bytes.Equal(blob.Commitment, commitment) {
				t.Fatalf("blob commitment = %x, want %x", blob.Commitment, commitment)
			}
			if string(blob.Signer) != wantSigner {
				t.Fatalf("blob signer = %q, want %q", blob.Signer, wantSigner)
			}
			return
		}
		time.Sleep(submissionPollInterval)
	}

	t.Fatalf("blob with commitment %x was not indexed by apex\n%s", commitment, proc.logs.String())
}

func decodeSubmitHeight(t *testing.T, raw json.RawMessage) uint64 {
	t.Helper()

	var height uint64
	if err := json.Unmarshal(raw, &height); err != nil {
		t.Fatalf("decode submit height: %v", err)
	}
	return height
}

type jsonRPCRequest struct {
	Jsonrpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
	ID      int    `json:"id"`
}

type jsonRPCResponse struct {
	Jsonrpc string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *jsonRPCError   `json:"error,omitempty"`
	ID      int             `json:"id"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcBlob struct {
	Namespace    []byte `json:"namespace"`
	Data         []byte `json:"data"`
	ShareVersion uint32 `json:"share_version"`
	Commitment   []byte `json:"commitment"`
	Signer       []byte `json:"signer"`
	Index        int    `json:"index"`
}

func doRPC(t *testing.T, proc *apexProcess, rpcAddr string, method string, params ...any) jsonRPCResponse {
	t.Helper()

	reqBody, err := json.Marshal(jsonRPCRequest{
		Jsonrpc: "2.0",
		Method:  method,
		Params:  params,
		ID:      1,
	})
	if err != nil {
		t.Fatalf("marshal RPC request: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, "http://"+rpcAddr+"/", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("create RPC request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("perform RPC request: %v\n%s", err, proc.logs.String())
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read RPC response: %v", err)
	}

	var rpcResp jsonRPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		t.Fatalf("decode RPC response: %v (body: %s)", err, body)
	}
	return rpcResp
}

func reserveTCPAddr(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP port: %v", err)
	}
	defer lis.Close() //nolint:errcheck
	return lis.Addr().String()
}

func testNamespace(t *testing.T, subID []byte) []byte {
	t.Helper()

	namespace, err := gsquare.NewV0Namespace(subID)
	if err != nil {
		t.Fatalf("build namespace: %v", err)
	}
	if err := namespace.ValidateForBlob(); err != nil {
		t.Fatalf("validate namespace: %v", err)
	}
	namespaceBytes := append([]byte(nil), namespace.Bytes()...)
	if len(namespaceBytes) != types.NamespaceSize {
		t.Fatalf("namespace size = %d, want %d", len(namespaceBytes), types.NamespaceSize)
	}
	return namespaceBytes
}
