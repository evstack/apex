package main

import (
	"testing"

	"github.com/evstack/apex/pkg/submit"
)

func TestIsLoopbackBindAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addr string
		want bool
	}{
		{addr: "127.0.0.1:8333", want: true},
		{addr: "[::1]:8333", want: true},
		{addr: "localhost:8333", want: true},
		{addr: "api.localhost:8333", want: true},
		{addr: "unix:///tmp/apex.sock", want: true},
		{addr: "/tmp/apex.sock", want: true},
		{addr: ":8333", want: false},
		{addr: "0.0.0.0:8333", want: false},
		{addr: "[::]:8333", want: false},
		{addr: "apex.example.com:8333", want: false},
		{addr: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			t.Parallel()

			if got := isLoopbackBindAddr(tt.addr); got != tt.want {
				t.Fatalf("isLoopbackBindAddr(%q) = %v, want %v", tt.addr, got, tt.want)
			}
		})
	}
}

func TestNormalizeBlobSubmitter(t *testing.T) {
	t.Parallel()

	var directSubmitter *submit.DirectSubmitter
	if got := normalizeBlobSubmitter(directSubmitter); got != nil {
		t.Fatalf("normalizeBlobSubmitter(nil) = %#v, want nil", got)
	}
}
