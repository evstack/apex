package types

import (
	"fmt"

	share "github.com/celestiaorg/go-square/v3/share"
)

// ValidateForBlob checks whether the namespace is valid for user-specified
// Celestia blob data.
func (n Namespace) ValidateForBlob() error {
	ns, err := share.NewNamespaceFromBytes(n[:])
	if err != nil {
		return fmt.Errorf("invalid namespace: %w", err)
	}
	if err := ns.ValidateForBlob(); err != nil {
		return fmt.Errorf("invalid blob namespace: %w", err)
	}
	return nil
}
