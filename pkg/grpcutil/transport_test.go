package grpcutil

import "testing"

func TestTransportCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		addr          string
		allowInsecure bool
		wantProtocol  string
	}{
		{
			name:         "loopback host uses insecure",
			addr:         "localhost:9090",
			wantProtocol: "insecure",
		},
		{
			name:         "loopback ip uses insecure",
			addr:         "127.0.0.1:9090",
			wantProtocol: "insecure",
		},
		{
			name:         "remote host uses tls",
			addr:         "celestia.example.com:9090",
			wantProtocol: "tls",
		},
		{
			name:          "remote host can opt into insecure",
			addr:          "celestia.example.com:9090",
			allowInsecure: true,
			wantProtocol:  "insecure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			creds := TransportCredentials(tt.addr, tt.allowInsecure)
			if got := creds.Info().SecurityProtocol; got != tt.wantProtocol {
				t.Fatalf("security protocol = %q, want %q", got, tt.wantProtocol)
			}
		})
	}
}
