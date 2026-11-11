package sentinel

import (
	"reflect"
	"testing"

	"github.com/databasifylabs/engine-sdk/contract"
)

func TestHostProfileTunesTheHostForValkeyAndRedis(t *testing.T) {
	want := contract.HostProfile{
		Settings:                    map[string]string{"vm.overcommit_memory": "1"},
		Floors:                      map[string]int64{"net.core.somaxconn": 1024},
		DisableTransparentHugepages: true,
	}
	for _, flavor := range []Flavor{Valkey, Redis} {
		if got := New(flavor).HostProfile(); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: host profile = %+v", flavor.Name, got)
		}
	}
}
