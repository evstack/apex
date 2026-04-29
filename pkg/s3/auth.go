package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	authorizationAlgorithm = "AWS4-HMAC-SHA256"
	emptyPayloadSHA256     = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

type authConfig struct {
	accessKeyID     string
	secretAccessKey string
}

type authHeader struct {
	credential    string
	signedHeaders []string
	signature     string
}

type authError struct {
	code    string
	message string
}

func (e *authError) Error() string {
	if e == nil {
		return ""
	}
	return e.code + ": " + e.message
}

func (s *Server) authenticateRequest(r *http.Request) *authError {
	if s.auth == nil {
		return nil
	}

	raw := strings.TrimSpace(r.Header.Get("Authorization"))
	if raw == "" {
		return &authError{code: "AccessDenied", message: "AWS Signature Version 4 authorization is required"}
	}

	authz, err := parseAuthHeader(raw)
	if err != nil {
		return &authError{code: "AccessDenied", message: err.Error()}
	}

	credentialParts := strings.Split(authz.credential, "/")
	if len(credentialParts) != 5 || credentialParts[4] != "aws4_request" {
		return &authError{code: "AccessDenied", message: "invalid credential scope"}
	}
	if credentialParts[0] != s.auth.accessKeyID {
		return &authError{code: "InvalidAccessKeyId", message: "The AWS Access Key Id you provided does not exist in our records."}
	}
	if credentialParts[3] != "s3" {
		return &authError{code: "AccessDenied", message: "invalid service scope"}
	}
	if s.region != "" && credentialParts[2] != s.region {
		return &authError{code: "SignatureDoesNotMatch", message: "credential scope region does not match the configured S3 region"}
	}

	amzDate := strings.TrimSpace(r.Header.Get("X-Amz-Date"))
	if amzDate == "" {
		return &authError{code: "AccessDenied", message: "missing X-Amz-Date header"}
	}
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return &authError{code: "AccessDenied", message: "invalid X-Amz-Date header"}
	}
	if skew := time.Since(t); skew > 15*time.Minute || skew < -15*time.Minute {
		return &authError{code: "RequestTimeTooSkewed", message: "The difference between the request time and the current time is too large."}
	}

	payloadHash := strings.TrimSpace(r.Header.Get("X-Amz-Content-Sha256"))
	if payloadHash == "" {
		payloadHash = emptyPayloadSHA256
	}

	canonicalRequest, err := buildCanonicalRequest(r, authz.signedHeaders, payloadHash)
	if err != nil {
		return &authError{code: "AccessDenied", message: err.Error()}
	}

	scope := strings.Join(credentialParts[1:], "/")
	stringToSign := strings.Join([]string{
		authorizationAlgorithm,
		amzDate,
		scope,
		hashHex([]byte(canonicalRequest)),
	}, "\n")

	signingKey := deriveSigningKey(s.auth.secretAccessKey, credentialParts[1], credentialParts[2], credentialParts[3])
	expectedSignature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	if subtle.ConstantTimeCompare([]byte(expectedSignature), []byte(authz.signature)) != 1 {
		return &authError{code: "SignatureDoesNotMatch", message: "The request signature we calculated does not match the signature you provided."}
	}

	return nil
}

func parseAuthHeader(raw string) (*authHeader, error) {
	if !strings.HasPrefix(raw, authorizationAlgorithm+" ") {
		return nil, errors.New("unsupported authorization algorithm")
	}

	fields := strings.Split(strings.TrimPrefix(raw, authorizationAlgorithm+" "), ",")
	values := make(map[string]string, len(fields))
	for _, field := range fields {
		part := strings.TrimSpace(field)
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("invalid authorization field %q", part)
		}
		values[key] = value
	}

	credential := values["Credential"]
	signedHeaders := values["SignedHeaders"]
	signature := values["Signature"]
	if credential == "" || signedHeaders == "" || signature == "" {
		return nil, errors.New("authorization header is missing required fields")
	}

	headers := strings.Split(signedHeaders, ";")
	for i := range headers {
		headers[i] = strings.TrimSpace(strings.ToLower(headers[i]))
		if headers[i] == "" {
			return nil, errors.New("authorization header contains an empty signed header")
		}
	}

	return &authHeader{
		credential:    credential,
		signedHeaders: headers,
		signature:     strings.TrimSpace(signature),
	}, nil
}

func buildCanonicalRequest(r *http.Request, signedHeaders []string, payloadHash string) (string, error) {
	canonicalURI := r.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	canonicalQuery := canonicalQueryString(r.URL.Query())

	var headerBuilder strings.Builder
	for _, headerName := range signedHeaders {
		value, ok := canonicalHeaderValue(r, headerName)
		if !ok {
			return "", fmt.Errorf("missing signed header %q", headerName)
		}
		headerBuilder.WriteString(headerName)
		headerBuilder.WriteByte(':')
		headerBuilder.WriteString(value)
		headerBuilder.WriteByte('\n')
	}

	return strings.Join([]string{
		r.Method,
		canonicalURI,
		canonicalQuery,
		headerBuilder.String(),
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n"), nil
}

func canonicalQueryString(values url.Values) string {
	if len(values) == 0 {
		return ""
	}

	type pair struct {
		key   string
		value string
	}

	pairs := make([]pair, 0, len(values))
	for key, vals := range values {
		if len(vals) == 0 {
			pairs = append(pairs, pair{key: key, value: ""})
			continue
		}
		for _, value := range vals {
			pairs = append(pairs, pair{key: key, value: value})
		}
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})

	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = awsEncode(p.key) + "=" + awsEncode(p.value)
	}
	return strings.Join(parts, "&")
}

func canonicalHeaderValue(r *http.Request, name string) (string, bool) {
	if name == "host" {
		if r.Host == "" {
			return "", false
		}
		return normalizeHeaderValue(r.Host), true
	}

	values, ok := r.Header[http.CanonicalHeaderKey(name)]
	if !ok || len(values) == 0 {
		return "", false
	}

	normalized := make([]string, len(values))
	for i := range values {
		normalized[i] = normalizeHeaderValue(values[i])
	}
	return strings.Join(normalized, ","), true
}

func normalizeHeaderValue(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func awsEncode(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}

func hashHex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
