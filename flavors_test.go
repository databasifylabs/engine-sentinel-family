package sentinel

import (
	"testing"

	"github.com/databasifylabs/engine-sdk/contract"
)

func TestBothFlavorsServeNodesButNotAccounts(t *testing.T) {
	for _, flavor := range []Flavor{Valkey, Redis} {
		engine := New(flavor)
		_, nodesUnsupported := engine.Nodes().(contract.Unsupported)
		_, accountsUnsupported := engine.Accounts().(contract.Unsupported)
		if engine.Name() != flavor.Name || nodesUnsupported || !accountsUnsupported {
			t.Fatalf("%s: name = %s, nodes unsupported = %v, accounts unsupported = %v", flavor.Name, engine.Name(), nodesUnsupported, accountsUnsupported)
		}
	}
}
