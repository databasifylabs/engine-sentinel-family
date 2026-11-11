package sentinel

import (
	"reflect"
	"testing"

	"github.com/databasifylabs/engine-sdk/dtos"
)

func TestCapabilitiesListTheSentinelPorts(t *testing.T) {
	want := dtos.Ports{Client: 25061, Peers: []int{6379, 26379}}
	for _, flavor := range []Flavor{Valkey, Redis} {
		if got := New(flavor).Capabilities().Ports; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: ports = %+v", flavor.Name, got)
		}
	}
}
