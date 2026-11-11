package sentinel

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/databasifylabs/engine-sdk/ctl"
	"github.com/databasifylabs/engine-sdk/dtos"
	"github.com/databasifylabs/engine-sdk/requests"
)

type fakeSupervisor struct {
	calls  *[]string
	active bool
}

func (f fakeSupervisor) record(call string) error {
	*f.calls = append(*f.calls, call)

	return nil
}

func (f fakeSupervisor) Do(verb string, units ...string) error {
	return f.record(verb + " " + strings.Join(units, " "))
}

func (f fakeSupervisor) Kill(signal string, units ...string) error {
	return f.record("kill " + signal + " " + strings.Join(units, " "))
}

func (f fakeSupervisor) EnableNow(units ...string) error {
	return f.record("enable " + strings.Join(units, " "))
}

func (f fakeSupervisor) DisableNow(units ...string) error {
	return f.record("disable " + strings.Join(units, " "))
}

func (f fakeSupervisor) Run(transient ctl.Transient) error {
	return f.record("run " + transient.Unit + " " + strings.Join(transient.Argv, " "))
}

func (f fakeSupervisor) Active(string) bool { return f.active }

func (fakeSupervisor) Property(string, string) string { return "0" }

func secret(value string) *string { return &value }

func clusteredRequest(ip string, primary string) requests.NodeConfigure {
	return requests.NodeConfigure{
		Clustered: true, Ip: ip, DatabaseId: "db1", Primary: secret(primary),
		Members:  []dtos.Member{{Ip: "10.0.0.1", Position: 1, Voting: true}, {Ip: "10.0.0.2", Position: 2, Voting: true}, {Ip: "10.0.0.3", Position: 3, Voting: true}},
		Settings: map[string]string{"maxmemory-policy": "allkeys-lru", "pubsub-output-buffer-limit": "64", "notify-keyspace-events": ""},
		Secrets:  dtos.Secrets{Admin: secret(`p"w`)},
	}
}

func testEngine(t *testing.T, server fakeServer, active bool) (Engine, *[]string) {
	t.Helper()
	calls := []string{}

	return Engine{flavor: Valkey, server: server, supervisor: fakeSupervisor{calls: &calls, active: active}, root: t.TempDir()}, &calls
}

func configure(t *testing.T, engine Engine, request requests.NodeConfigure) {
	t.Helper()
	if _, err := engine.Nodes().Configure(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func TestServerConfGolden(t *testing.T) {
	engine, _ := testEngine(t, fakeServer{replies: map[string]Reply{}}, false)
	engine.root = "/srv/valkey"
	conf := serverConfOf(engine, clusteredRequest("10.0.0.2", "10.0.0.1"), "10.0.0.1")
	golden, err := os.ReadFile(filepath.Join("testdata", "valkey.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if conf != string(golden) {
		t.Fatalf("conf differs from testdata/valkey.conf:\n%s", conf)
	}
}

func TestPubsubLimitBecomesClientOutputBufferLimit(t *testing.T) {
	if got := directiveOf("pubsub-output-buffer-limit", "64"); got != "client-output-buffer-limit pubsub 64mb 16mb 60" {
		t.Fatalf("directive = %q", got)
	}
}

func TestConfigureRefusesAnEmptyPrimaryWhilePeersHoldKeys(t *testing.T) {
	server := fakeServer{replies: map[string]Reply{
		"10.0.0.2:6379 DBSIZE": {Integer: 42},
		"10.0.0.3:6379 DBSIZE": {Integer: 0},
	}}
	engine, _ := testEngine(t, server, false)
	request := clusteredRequest("10.0.0.1", "10.0.0.1")
	request.Settings = map[string]string{"appendonly": "no"}

	configure(t, engine, request)

	if !engine.exists(startRefused) {
		t.Fatal("start-refused was not written")
	}
	if err := engine.Nodes().Start(context.Background()); err == nil || !strings.Contains(err.Error(), "empty primary") {
		t.Fatalf("start err = %v", err)
	}
}

func TestConfigureFollowsThePrimaryPeersReport(t *testing.T) {
	server := fakeServer{replies: map[string]Reply{
		"10.0.0.1:26379 SENTINEL get-master-addr-by-name main": {Items: []Reply{{Text: "10.0.0.3"}, {Text: "6379"}}},
	}}
	engine, _ := testEngine(t, server, false)

	configure(t, engine, clusteredRequest("10.0.0.2", "10.0.0.1"))

	if engine.replicaOf() != "10.0.0.3" {
		t.Fatalf("replicaof = %q", engine.replicaOf())
	}
}

func TestStartHoldsAReplicaThatLostItsData(t *testing.T) {
	engine, calls := testEngine(t, fakeServer{replies: map[string]Reply{}}, false)
	configure(t, engine, clusteredRequest("10.0.0.2", "10.0.0.1"))

	if err := engine.Nodes().Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(engine.conf(), "\nreplica-priority 0\n") || !slices.Contains(*calls, "start valkey-server") {
		t.Fatalf("conf = %s, calls = %v", engine.conf(), *calls)
	}
}

func TestReadyMovesToSentinelsPrimary(t *testing.T) {
	sent := []string{}
	server := fakeServer{sent: &sent, replies: map[string]Reply{
		"INFO replication": {Text: "role:slave\r\nmaster_host:10.0.0.9\r\nmaster_link_status:up\r\n"},
		"127.0.0.1:26379 SENTINEL get-master-addr-by-name main": {Items: []Reply{{Text: "10.0.0.1"}, {Text: "6379"}}},
	}}
	engine, _ := testEngine(t, server, true)
	configure(t, engine, clusteredRequest("10.0.0.2", "10.0.0.9"))
	request, _ := engine.saved()

	synced, err := nodes{Engine: engine}.synced(context.Background(), request, "role:slave\r\nmaster_host:10.0.0.9\r\nmaster_link_status:up\r\n", func(string) {})

	if err != nil || synced || !slices.Contains(sent, "REPLICAOF 10.0.0.1 6379") || engine.replicaOf() != "10.0.0.1" {
		t.Fatalf("synced = %v, err = %v, sent = %v, replicaof = %q", synced, err, sent, engine.replicaOf())
	}
}

func TestApplyResetsRemovedKeys(t *testing.T) {
	sent := []string{}
	engine, _ := testEngine(t, fakeServer{sent: &sent, replies: map[string]Reply{}}, true)
	request := clusteredRequest("10.0.0.1", "10.0.0.1")
	request.Settings = map[string]string{"maxmemory-policy": "allkeys-lru"}
	configure(t, engine, request)
	applied, _ := json.Marshal([]string{"maxmemory-policy", "timeout"})
	if _, err := engine.write(appliedState, string(applied), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.write(originalsState, `{"timeout":"0"}`, 0o600); err != nil {
		t.Fatal(err)
	}
	server := fakeServer{sent: &sent, replies: map[string]Reply{
		"CONFIG GET maxmemory-policy":             {Items: []Reply{{Text: "maxmemory-policy"}, {Text: "noeviction"}}},
		"CONFIG GET maxclients":                   {Items: []Reply{{Text: "maxclients"}, {Text: "10000"}}},
		"CONFIG GET maxmemory":                    {Items: []Reply{{Text: "maxmemory"}, {Text: "0"}}},
		"CONFIG SET maxclients 65504":             {Text: "OK"},
		"CONFIG SET maxmemory 716mb":              {Text: "OK"},
		"CONFIG SET appendonly yes":               {Text: "OK"},
		"CONFIG SET maxmemory-policy allkeys-lru": {Text: "OK"},
		"CONFIG SET save 3600 1":                  {Text: "OK"},
		"CONFIG SET timeout 0":                    {Text: "OK"},
	}}
	engine.server = server

	if err := engine.Nodes().ApplyConfig(context.Background()); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"CONFIG SET appendonly yes", "CONFIG SET maxmemory-policy allkeys-lru", "CONFIG SET save 3600 1", "CONFIG SET timeout 0", "CONFIG SET maxmemory 716mb"} {
		if !slices.Contains(sent, want) {
			t.Errorf("sent lacks %q: %q", want, sent)
		}
	}
	if slices.ContainsFunc(sent, func(command string) bool { return strings.HasPrefix(command, "CONFIG SET io-threads") }) {
		t.Fatal("a restart-only setting was set on the running server")
	}
	if !strings.Contains(engine.read(originalsState), `"maxmemory-policy":"noeviction"`) {
		t.Fatalf("originals = %s", engine.read(originalsState))
	}
}

func TestMetricsSumKeyspace(t *testing.T) {
	info := "connected_clients:7\r\nused_memory:2097152\r\nkeyspace_hits:5\r\ndb0:keys=10,expires=0\r\ndb3:keys=5,expires=1\r\n"
	engine, _ := testEngine(t, fakeServer{replies: map[string]Reply{"INFO all": {Text: info}}}, true)

	metrics, err := engine.Nodes().Metrics(context.Background())

	if err != nil || metrics.Connections != 7 || metrics.DataMb != 2 || *metrics.Keys != 15 || *metrics.Hits != 5 {
		t.Fatalf("metrics = %+v, err = %v", metrics, err)
	}
}

func TestRestartRestoresRestartOnlySettingsAConfigRewriteReverted(t *testing.T) {
	engine, calls := testEngine(t, fakeServer{}, true)
	request := clusteredRequest("10.0.0.1", "10.0.0.1")
	request.Settings["databases"] = "32"
	configure(t, engine, request)
	if _, err := engine.write(engine.confPath(), strings.Replace(engine.conf(), "databases 32\n", "databases 16\n", 1), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := engine.Nodes().Restart(context.Background()); err != nil {
		t.Fatal(err)
	}

	conf := engine.conf()
	if !strings.Contains(conf, "\ndatabases 32\n") || strings.Contains(conf, "databases 16") {
		t.Fatalf("conf =\n%s", conf)
	}
	if (*calls)[len(*calls)-1] != "restart valkey-server" {
		t.Fatalf("calls = %q", *calls)
	}
}

func TestServerConfTurnsAppendOnlyOffOnceWithoutPersistence(t *testing.T) {
	for _, flavor := range []Flavor{Valkey, Redis} {
		engine, _ := testEngine(t, fakeServer{replies: map[string]Reply{}}, false)
		engine.flavor = flavor
		request := clusteredRequest("10.0.0.2", "10.0.0.1")
		request.Settings = map[string]string{"appendonly": "no"}

		if count := strings.Count(serverConfOf(engine, request, "10.0.0.1"), "appendonly no\n"); count != 1 {
			t.Fatalf("%s: appendonly no appears %d times", flavor.Name, count)
		}
	}
}
