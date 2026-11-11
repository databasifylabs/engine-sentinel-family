package sentinel

import (
	"testing"

	"github.com/databasifylabs/engine-sdk/dtos"
)

func restartOf(engine Engine, key string) bool {
	for _, field := range engine.catalog().Fields {
		if field.Key == key {
			return field.Restart
		}
	}

	return false
}

func TestIoThreadsAreLiveOnlyOnValkeyNine(t *testing.T) {
	t.Setenv("DATABASIFY_LINE", "9")
	if restartOf(Engine{flavor: Valkey}, "io-threads") || !restartOf(Engine{flavor: Redis}, "io-threads") {
		t.Fatal("io-threads is live on Valkey 9 and restart-only on Redis")
	}
	for _, line := range []string{"7", "8"} {
		t.Setenv("DATABASIFY_LINE", line)
		if !restartOf(Engine{flavor: Valkey}, "io-threads") {
			t.Fatalf("io-threads is restart-only on Valkey %s", line)
		}
	}
	t.Setenv("DATABASIFY_LINE", "10")
	if restartOf(Engine{flavor: Valkey}, "io-threads") {
		t.Fatal("io-threads stays live on Valkey 10")
	}
}

func TestOnlyRealValkeyLimitsAreEnforced(t *testing.T) {
	engine := Engine{flavor: Valkey}
	if err := engine.catalog().Check(map[string]string{"maxclients": "4294967295", "timeout": "2147483647", "io-threads": "256", "maxmemory": "4gb", "appendonly": "no"}); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{"io-threads": "257", "databases": "100000", "maxmemory-policy": "random", "active-expire-effort": "11", "save": ""} {
		if engine.catalog().Check(map[string]string{key: value}) == nil {
			t.Errorf("%s=%s was accepted", key, value)
		}
	}
	if (Engine{flavor: Redis}).catalog().Check(map[string]string{"io-threads": "129"}) == nil {
		t.Fatal("Redis allows at most 128 I/O threads")
	}
}

func TestValkeyTuningFollowsTheNodeSize(t *testing.T) {
	tuned := tunedFor(dtos.Member{Cpus: 8, MemoryMb: 16384})

	if tuned["maxmemory"] != "11468mb" || tuned["io-threads"] != "4" || tuned["maxclients"] != "65504" {
		t.Fatalf("tuned = %v", tuned)
	}
}
