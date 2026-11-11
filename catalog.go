package sentinel

import (
	"os"
	"strconv"

	"github.com/databasifylabs/engine-sdk/version"

	"github.com/databasifylabs/engine-sdk/dtos"
	"github.com/databasifylabs/engine-sdk/settings"
)

const (
	maxInt32          = 2147483647
	maxInt64          = 9223372036854775807
	maxUnsigned32     = 4294967295
	maxDatabases      = 65536
	openFilesLimit    = 65536
	reservedFiles     = 32
	memoryPattern     = `[0-9]{1,15}([bB]|[kK][bB]?|[mM][bB]?|[gG][bB]?)?`
	keyspacePattern   = `[KEglshzxeAtmdn]{0,32}`
	liveIoThreadsFrom = "9"
)

var evictionPolicies = []string{"noeviction", "allkeys-lru", "allkeys-lfu", "allkeys-random", "volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl"}

func (e Engine) catalog() settings.Catalog {
	threads := settings.Integer("io-threads", "I/O threads", "Threads that read and write client sockets.", 1, e.maxIoThreads(), "1").TunedFromServer()
	if e.flavor.Name != Valkey.Name || version.NewerThan(liveIoThreadsFrom, os.Getenv("DATABASIFY_LINE")) {
		threads = threads.Restarting()
	}

	return settings.Catalog{Tune: tunedFor, Fields: settings.Fields(
		settings.Choice("appendonly", "Persistence", "Keep data on disk with snapshots and an append-only file. no keeps data in memory only.", []string{"yes", "no"}, "yes"),
		settings.Choice("maxmemory-policy", "Eviction policy", "What happens when memory is full. noeviction rejects writes; the others remove keys to make room.", evictionPolicies, "noeviction"),
		settings.Text("maxmemory", "Max memory", "Memory the dataset may use, such as 1433mb or 4gb.", memoryPattern, "0").TunedFromServer(),
		threads,
		settings.Integer("maxclients", "Max clients", "Most client connections accepted at once.", 1, maxUnsigned32, "10000").TunedFromServer(),
		settings.Integer(pubsubLimitKey, "Pub/Sub output buffer", "Hard output buffer limit for Pub/Sub clients. The soft limit is a quarter of it.", 1, maxInt32, "32").In("MB"),
		settings.Integer("databases", "Number of databases", "Logical databases selectable with SELECT. Each one costs memory when the server starts.", 1, maxDatabases, "16").Restarting(),
		settings.Integer("lfu-log-factor", "LFU log factor", "Counter logarithm factor for the LFU eviction policies.", 0, maxInt32, "10"),
		settings.Integer("lfu-decay-time", "LFU decay time", "Minutes before an LFU counter is halved.", 0, maxInt32, "1").In("min"),
		settings.Integer("timeout", "Idle timeout", "Seconds before an idle client is disconnected. 0 never disconnects.", 0, maxInt32, "0").In("s"),
		settings.Text("notify-keyspace-events", "Keyspace notifications", "Events published to keyspace channels, such as Ex for expired keys. Empty turns them off.", keyspacePattern, ""),
		settings.Choice("acl-pubsub-default", "Default channel access", "Pub/Sub channels new users may use when their ACL names none.", []string{"resetchannels", "allchannels"}, "resetchannels"),
		settings.Integer("active-expire-effort", "Active expire effort", "Effort spent removing expired keys in the background.", 1, 10, "1"),
		settings.Integer("slowlog-log-slower-than", "Slow log threshold", "Commands slower than this many microseconds are logged. -1 turns the slow log off.", -1, maxInt64, "10000").In("µs"),
		settings.Integer("slowlog-max-len", "Slow log length", "Number of slow commands kept.", 0, maxInt64, "128"),
	)}
}

func (e Engine) maxIoThreads() float64 {
	if e.flavor.Name == Valkey.Name {
		return 256
	}

	return 128
}

func tunedFor(resources dtos.Member) map[string]string {
	return map[string]string{
		"maxmemory":  strconv.FormatInt(max(resources.MemoryMb, 1024)*70/100, 10) + "mb",
		"io-threads": strconv.FormatInt(min(max(max(resources.Cpus, 1)/2, 1), 16), 10),
		"maxclients": strconv.Itoa(openFilesLimit - reservedFiles),
	}
}
