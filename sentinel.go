package sentinel

import (
	"github.com/databasifylabs/engine-sdk/contract"
	"github.com/databasifylabs/engine-sdk/ctl"
	"github.com/databasifylabs/engine-sdk/dtos"
	"github.com/databasifylabs/engine-sdk/layout"
	"github.com/databasifylabs/engine-sdk/requests"
	"github.com/databasifylabs/engine-sdk/units"
)

const (
	dataPort       = 6379
	tlsPort        = 25061
	sentinelPort   = 26379
	localhost      = "127.0.0.1"
	masterName     = "main"
	persistenceKey = "appendonly"
)

type Flavor struct {
	Name         string
	Cli          string
	Server       string
	CheckRdb     string
	VersionField string
}

var (
	Valkey = Flavor{Name: "valkey", Cli: "valkey-cli", Server: "valkey-server", CheckRdb: "valkey-check-rdb", VersionField: "valkey_version"}
	Redis  = Flavor{Name: "redis", Cli: "redis-cli", Server: "redis-server", CheckRdb: "redis-check-rdb", VersionField: "redis_version"}
)

type Supervisor interface {
	Do(verb string, units ...string) error
	Kill(signal string, units ...string) error
	EnableNow(units ...string) error
	DisableNow(units ...string) error
	Run(transient ctl.Transient) error
	Active(unit string) bool
	Property(unit, property string) string
}

type Engine struct {
	flavor     Flavor
	server     Server
	supervisor Supervisor
	root       string
}

func New(flavor Flavor) Engine {
	root := "/srv/" + flavor.Name

	return Engine{
		flavor:     flavor,
		server:     respServer{passwordFile: root + "/conf/password"},
		supervisor: units.Control{Socket: layout.FromEnv().Socket()},
		root:       root,
	}
}

func (e Engine) Name() string { return e.flavor.Name }

func (Engine) Capabilities() contract.Capabilities {
	return contract.Capabilities{Ports: dtos.Ports{Client: tlsPort, Peers: []int{dataPort, sentinelPort}}}
}

func (Engine) HostProfile() contract.HostProfile {
	return contract.HostProfile{
		Settings:                    map[string]string{"vm.overcommit_memory": "1"},
		Floors:                      map[string]int64{"net.core.somaxconn": 1024},
		DisableTransparentHugepages: true,
	}
}

type nodes struct{ Engine }

func (e Engine) Nodes() contract.Nodes { return nodes{e} }

func (Engine) Accounts() contract.Accounts { return contract.UnsupportedAccounts{} }

func (Engine) Archive() contract.Archive { return contract.UnsupportedArchive{} }

func (e Engine) serverUnit() string { return e.flavor.Name + "-server" }

func (e Engine) sentinelUnit() string { return e.flavor.Name + "-sentinel" }

func persistent(request requests.NodeConfigure) bool {
	return request.Settings[persistenceKey] != "no"
}

func (Engine) Pools() contract.Pools { return contract.UnsupportedPools{} }
