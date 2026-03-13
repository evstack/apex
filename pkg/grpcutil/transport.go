package grpcutil

import (
	"crypto/tls"
	"net"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TransportCredentials returns plaintext credentials for loopback targets and
// explicit insecure opt-ins, and TLS credentials for non-local targets.
func TransportCredentials(grpcAddr string, allowInsecure bool) credentials.TransportCredentials {
	if allowInsecure || isLocalTarget(grpcAddr) {
		return insecure.NewCredentials()
	}
	return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
}

func isLocalTarget(grpcAddr string) bool {
	target := strings.TrimSpace(grpcAddr)
	if target == "" {
		return false
	}
	if strings.HasPrefix(target, "/") || strings.HasPrefix(target, "unix://") || strings.HasPrefix(target, "unix:") {
		return true
	}
	if idx := strings.LastIndex(target, "://"); idx >= 0 {
		target = target[idx+3:]
	}
	target = strings.TrimLeft(target, "/")
	if idx := strings.LastIndex(target, "/"); idx >= 0 {
		target = target[idx+1:]
	}
	if strings.HasPrefix(target, ":") {
		return true
	}

	host := target
	if parsedHost, _, err := net.SplitHostPort(target); err == nil {
		host = parsedHost
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return true
	}

	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
