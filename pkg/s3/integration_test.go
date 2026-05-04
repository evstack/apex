package s3_test

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	gsquare "github.com/celestiaorg/go-square/v3/share"
	"github.com/rs/zerolog"

	apexs3 "github.com/evstack/apex/pkg/s3"
	"github.com/evstack/apex/pkg/store"
	"github.com/evstack/apex/pkg/submit"
	"github.com/evstack/apex/pkg/types"
)

// mockBlobSubmitter records submissions without talking to Celestia.
type mockBlobSubmitter struct {
	calls int
}

func (m *mockBlobSubmitter) Submit(_ context.Context, req *submit.Request) (*submit.Result, error) {
	m.calls++
	if req == nil || len(req.Blobs) != 1 {
		return nil, errors.New("expected a single blob")
	}
	return &submit.Result{Height: uint64(100 + m.calls)}, nil
}

func testNamespace() types.Namespace {
	namespace := gsquare.MustNewV0Namespace([]byte("apexs3x"))
	var ns types.Namespace
	copy(ns[:], namespace.Bytes())
	return ns
}

// setupIntegrationServer wires a real SQLite store + mock submitter into the S3 HTTP server.
func setupIntegrationServer(t *testing.T) *httptest.Server {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "integration.db")
	sqliteStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })

	objStore := store.NewObjectStore(sqliteStore)
	sub := &mockBlobSubmitter{}
	svc := apexs3.NewService(objStore, sub, testNamespace())
	log := zerolog.New(io.Discard)
	srv := apexs3.NewServer(svc, "us-east-1", "", "", log)

	return httptest.NewServer(srv)
}

func TestIntegration_FullObjectLifecycle(t *testing.T) {
	ts := setupIntegrationServer(t)
	defer ts.Close()

	client := ts.Client()
	base := ts.URL

	// 1. List buckets — should be empty.
	resp := doReq(t, client, http.MethodGet, base+"/") //nolint:bodyclose // closed in readBody
	assertStatus(t, resp, http.StatusOK)
	body := readBody(t, resp)
	if strings.Contains(body, "<Bucket>") {
		t.Errorf("expected no buckets, got: %s", body)
	}

	// 2. Create bucket.
	resp = doReq(t, client, http.MethodPut, base+"/test-bucket") //nolint:bodyclose // closed in closeBody
	assertStatus(t, resp, http.StatusOK)
	closeBody(t, resp)

	// 3. Head bucket — should exist.
	resp = doReq(t, client, http.MethodHead, base+"/test-bucket") //nolint:bodyclose // closed in closeBody
	assertStatus(t, resp, http.StatusOK)
	closeBody(t, resp)

	// 4. Put object.
	resp = doPutObject(t, client, base+"/test-bucket/hello.txt", "hello world", "text/plain")
	assertStatus(t, resp, http.StatusOK)
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Error("expected ETag on PUT response")
	}
	closeBody(t, resp)

	// 5. Get object — verify data roundtrip.
	resp = doReq(t, client, http.MethodGet, base+"/test-bucket/hello.txt") //nolint:bodyclose // closed in readBody
	assertStatus(t, resp, http.StatusOK)
	gotBody := readBody(t, resp)
	if gotBody != "hello world" {
		t.Errorf("expected 'hello world', got %q", gotBody)
	}

	// 6. Head object.
	resp = doReq(t, client, http.MethodHead, base+"/test-bucket/hello.txt")
	assertStatus(t, resp, http.StatusOK)
	if resp.Header.Get("Content-Length") != "11" {
		t.Errorf("expected Content-Length 11, got %s", resp.Header.Get("Content-Length"))
	}
	closeBody(t, resp)

	// 7. List objects.
	resp = doReq(t, client, http.MethodGet, base+"/test-bucket?list-type=2") //nolint:bodyclose // closed in readBody
	assertStatus(t, resp, http.StatusOK)
	listBody := readBody(t, resp)
	if !strings.Contains(listBody, "hello.txt") {
		t.Errorf("expected hello.txt in list response, got: %s", listBody)
	}

	// 8. Put second object, list again.
	resp = doPutObject(t, client, base+"/test-bucket/docs/readme.md", "# README", "text/markdown") //nolint:bodyclose // closed in closeBody
	assertStatus(t, resp, http.StatusOK)
	closeBody(t, resp)

	// 9. List with prefix.
	resp = doReq(t, client, http.MethodGet, base+"/test-bucket?prefix=docs/") //nolint:bodyclose // closed in readBody
	assertStatus(t, resp, http.StatusOK)
	prefixBody := readBody(t, resp)
	if !strings.Contains(prefixBody, "readme.md") {
		t.Errorf("expected readme.md in prefix list, got: %s", prefixBody)
	}
	if strings.Contains(prefixBody, "hello.txt") {
		t.Errorf("hello.txt should not appear in docs/ prefix list")
	}

	// 10. Delete object.
	resp = doReq(t, client, http.MethodDelete, base+"/test-bucket/hello.txt") //nolint:bodyclose // closed in closeBody
	assertStatus(t, resp, http.StatusNoContent)
	closeBody(t, resp)

	// 11. Verify deleted.
	resp = doReq(t, client, http.MethodGet, base+"/test-bucket/hello.txt") //nolint:bodyclose // closed in readBody
	assertStatus(t, resp, http.StatusNotFound)
	errBody := readBody(t, resp)
	if !strings.Contains(errBody, "NoSuchKey") {
		t.Errorf("expected NoSuchKey error, got: %s", errBody)
	}

	// 12. Delete second object then bucket.
	resp = doReq(t, client, http.MethodDelete, base+"/test-bucket/docs/readme.md") //nolint:bodyclose // closed in closeBody
	assertStatus(t, resp, http.StatusNoContent)
	closeBody(t, resp)

	resp = doReq(t, client, http.MethodDelete, base+"/test-bucket") //nolint:bodyclose // closed in closeBody
	assertStatus(t, resp, http.StatusNoContent)
	closeBody(t, resp)

	// 13. Verify bucket gone.
	resp = doReq(t, client, http.MethodHead, base+"/test-bucket") //nolint:bodyclose // closed in closeBody
	assertStatus(t, resp, http.StatusNotFound)
	closeBody(t, resp)

	// 14. List buckets — should be empty again.
	resp = doReq(t, client, http.MethodGet, base+"/") //nolint:bodyclose // closed in readBody
	assertStatus(t, resp, http.StatusOK)
	finalBody := readBody(t, resp)
	if strings.Contains(finalBody, "test-bucket") {
		t.Errorf("expected no buckets after deletion, got: %s", finalBody)
	}
}

func TestIntegration_DeleteNonEmptyBucket(t *testing.T) {
	ts := setupIntegrationServer(t)
	defer ts.Close()

	client := ts.Client()
	base := ts.URL

	resp := doReq(t, client, http.MethodPut, base+"/bucket") //nolint:bodyclose // closed in closeBody
	closeBody(t, resp)
	resp = doPutObject(t, client, base+"/bucket/file.txt", "data", "text/plain") //nolint:bodyclose // closed in closeBody
	closeBody(t, resp)

	resp = doReq(t, client, http.MethodDelete, base+"/bucket") //nolint:bodyclose // closed in readBody
	assertStatus(t, resp, http.StatusConflict)
	body := readBody(t, resp)
	if !strings.Contains(body, "BucketNotEmpty") {
		t.Errorf("expected BucketNotEmpty error, got: %s", body)
	}
}

func TestIntegration_XMLStructure(t *testing.T) {
	ts := setupIntegrationServer(t)
	defer ts.Close()

	client := ts.Client()
	base := ts.URL

	resp := doReq(t, client, http.MethodPut, base+"/xml-bucket") //nolint:bodyclose // closed in closeBody
	closeBody(t, resp)
	resp = doPutObject(t, client, base+"/xml-bucket/file.txt", "content", "text/plain") //nolint:bodyclose // closed in closeBody
	closeBody(t, resp)

	resp = doReq(t, client, http.MethodGet, base+"/xml-bucket?list-type=2") //nolint:bodyclose // closed in readBody
	assertStatus(t, resp, http.StatusOK)

	var result struct {
		XMLName  xml.Name `xml:"ListBucketResult"`
		Name     string   `xml:"Name"`
		Contents []struct {
			Key  string `xml:"Key"`
			Size int64  `xml:"Size"`
			ETag string `xml:"ETag"`
		} `xml:",any"`
	}
	body := readBody(t, resp)
	if err := xml.Unmarshal([]byte(body), &result); err != nil {
		t.Fatalf("XML parse failed: %v\nbody: %s", err, body)
	}
	if result.Name != "xml-bucket" {
		t.Errorf("expected bucket name 'xml-bucket', got %s", result.Name)
	}
}

func TestIntegration_EmptyObject(t *testing.T) {
	ts := setupIntegrationServer(t)
	defer ts.Close()

	client := ts.Client()
	base := ts.URL

	resp := doReq(t, client, http.MethodPut, base+"/bucket") //nolint:bodyclose // closed in closeBody
	closeBody(t, resp)
	resp = doPutObject(t, client, base+"/bucket/empty.txt", "", "text/plain") //nolint:bodyclose // closed in closeBody
	assertStatus(t, resp, http.StatusBadRequest)
	body := readBody(t, resp)
	if !strings.Contains(body, "InvalidRequest") {
		t.Errorf("expected InvalidRequest error, got: %s", body)
	}
}

func TestIntegration_MissingBucketObjectOpsReturnNoSuchBucket(t *testing.T) {
	ts := setupIntegrationServer(t)
	defer ts.Close()

	client := ts.Client()
	base := ts.URL

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodDelete} {
		resp := doReq(t, client, method, base+"/missing-bucket/key.txt") //nolint:bodyclose // closed below
		assertStatus(t, resp, http.StatusNotFound)
		if method == http.MethodHead {
			closeBody(t, resp)
			continue
		}
		body := readBody(t, resp)
		if !strings.Contains(body, "NoSuchBucket") {
			t.Fatalf("%s expected NoSuchBucket, got: %s", method, body)
		}
	}
}

func TestIntegration_ListObjectsV2DelimiterPagination(t *testing.T) {
	ts := setupIntegrationServer(t)
	defer ts.Close()

	client := ts.Client()
	base := ts.URL

	resp := doReq(t, client, http.MethodPut, base+"/bucket") //nolint:bodyclose // closed in closeBody
	closeBody(t, resp)

	resp = doPutObject(t, client, base+"/bucket/photos/2024/jan.jpg", "jan", "image/jpeg") //nolint:bodyclose // closed in closeBody
	closeBody(t, resp)
	resp = doPutObject(t, client, base+"/bucket/photos/2024/feb.jpg", "feb", "image/jpeg") //nolint:bodyclose // closed in closeBody
	closeBody(t, resp)
	resp = doPutObject(t, client, base+"/bucket/photos/2025/mar.jpg", "mar", "image/jpeg") //nolint:bodyclose // closed in closeBody
	closeBody(t, resp)

	type commonPrefix struct {
		Prefix string `xml:"Prefix"`
	}
	type listBucketResultV2 struct {
		XMLName               xml.Name       `xml:"ListBucketResult"`
		IsTruncated           bool           `xml:"IsTruncated"`
		KeyCount              int            `xml:"KeyCount"`
		NextContinuationToken string         `xml:"NextContinuationToken"`
		CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
	}

	page1 := doReq(t, client, http.MethodGet, base+"/bucket?list-type=2&prefix=photos/&delimiter=/&max-keys=1") //nolint:bodyclose // closed in readBody
	assertStatus(t, page1, http.StatusOK)
	body1 := readBody(t, page1)
	var out1 listBucketResultV2
	if err := xml.Unmarshal([]byte(body1), &out1); err != nil {
		t.Fatalf("unmarshal page1: %v\nbody: %s", err, body1)
	}
	if !out1.IsTruncated {
		t.Fatal("expected page1 to be truncated")
	}
	if out1.KeyCount != 1 {
		t.Fatalf("page1 KeyCount = %d, want 1", out1.KeyCount)
	}
	if len(out1.CommonPrefixes) != 1 || out1.CommonPrefixes[0].Prefix != "photos/2024/" {
		t.Fatalf("page1 common prefixes = %v, want [photos/2024/]", out1.CommonPrefixes)
	}
	if out1.NextContinuationToken == "" {
		t.Fatal("expected page1 next continuation token")
	}

	page2 := doReq(t, client, http.MethodGet, base+"/bucket?list-type=2&prefix=photos/&delimiter=/&max-keys=1&continuation-token="+out1.NextContinuationToken) //nolint:bodyclose // closed in readBody
	assertStatus(t, page2, http.StatusOK)
	body2 := readBody(t, page2)
	var out2 listBucketResultV2
	if err := xml.Unmarshal([]byte(body2), &out2); err != nil {
		t.Fatalf("unmarshal page2: %v\nbody: %s", err, body2)
	}
	if out2.IsTruncated {
		t.Fatal("expected page2 not to be truncated")
	}
	if len(out2.CommonPrefixes) != 1 || out2.CommonPrefixes[0].Prefix != "photos/2025/" {
		t.Fatalf("page2 common prefixes = %v, want [photos/2025/]", out2.CommonPrefixes)
	}
}

// --- helpers ---

func doReq(t *testing.T, client *http.Client, method, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, url, nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s failed: %v", method, url, err)
	}
	return resp
}

func doPutObject(t *testing.T, client *http.Client, url, body, contentType string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("create PUT request: %v", err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT %s failed: %v", url, err)
	}
	return resp
}

func assertStatus(t *testing.T, resp *http.Response, expected int) {
	t.Helper()
	if resp.StatusCode != expected {
		body := readBody(t, resp)
		t.Fatalf("expected status %d, got %d. Body: %s", expected, resp.StatusCode, body)
	}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	resp.Body.Close() //nolint:errcheck
	return string(data)
}

func closeBody(t *testing.T, resp *http.Response) {
	t.Helper()
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close() //nolint:errcheck
}
