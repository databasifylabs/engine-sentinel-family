package sentinel

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/databasifylabs/engine-sdk/manifest"
	"github.com/databasifylabs/engine-sdk/requests"
)

const (
	pubsubLimitKey       = "pubsub-output-buffer-limit"
	failoverMinimumNodes = 3
	snapshotSchedule     = "3600 1"
	engineUID            = 64999
)

func majorityOf(size int) int {
	return size/2 + 1
}

func requiredReplicasFor(size int) int {
	if size >= failoverMinimumNodes {
		return 1
	}

	return 0
}

func quoted(value string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`) + `"`
}

func directiveOf(key string, value string) string {
	if key == pubsubLimitKey {
		limit, _ := strconv.Atoi(value)

		return fmt.Sprintf("client-output-buffer-limit pubsub %dmb %dmb 60", limit, limit/4)
	}
	if value == "" {
		return key + ` ""`
	}

	return key + " " + value
}

func serverConfOf(e Engine, request requests.NodeConfigure, primary string) string {
	password := quoted(*request.Secrets.Admin)
	lines := []string{
		"port " + strconv.Itoa(dataPort),
		"bind * -::*",
		"protected-mode yes",
		"tcp-backlog 511",
		"tcp-keepalive 300",
		"daemonize no",
		"loglevel notice",
		`logfile ""`,
		"dir " + e.root + "/data",
		"tls-port " + strconv.Itoa(tlsPort),
		"tls-cert-file " + e.root + "/tls/server.pem",
		"tls-key-file " + e.root + "/tls/server.pem",
		"tls-ca-cert-file " + e.root + "/tls/ca.crt",
		"tls-auth-clients no",
		`tls-protocols "TLSv1.2 TLSv1.3"`,
		"requirepass " + password,
		"masterauth " + password,
		"maxmemory-clients 10%",
		"lazyfree-lazy-expire yes",
		"lazyfree-lazy-server-del yes",
		"lazyfree-lazy-user-del yes",
		"lazyfree-lazy-user-flush yes",
		"activedefrag yes",
		"latency-monitor-threshold 100",
	}
	lines = append(lines, persistenceLines(persistent(request))...)
	for _, key := range slices.Sorted(maps.Keys(request.Settings)) {
		if key != persistenceKey {
			lines = append(lines, directiveOf(key, request.Settings[key]))
		}
	}
	if request.Clustered {
		lines = append(lines,
			"replica-announce-ip "+request.Ip,
			"min-replicas-to-write "+strconv.Itoa(requiredReplicasFor(len(request.Members))),
			"min-replicas-max-lag 10",
		)
		if request.ReadOnly {
			lines = append(lines, "replica-priority 0")
		}
		if primary != "" && primary != request.Ip {
			lines = append(lines, "replicaof "+primary+" "+strconv.Itoa(dataPort))
		}
	}

	return strings.Join(lines, "\n") + "\n"
}

func persistenceLines(persistent bool) []string {
	if !persistent {
		return []string{`save ""`, "appendonly no"}
	}

	return []string{
		"save " + snapshotSchedule,
		"stop-writes-on-bgsave-error yes",
		"dbfilename dump.rdb",
		"appendonly yes",
		`appendfilename "appendonly.aof"`,
		`appenddirname "appendonlydir"`,
		"appendfsync everysec",
		"aof-use-rdb-preamble yes",
	}
}

func sentinelConfOf(e Engine, request requests.NodeConfigure, primary string) string {
	password := quoted(*request.Secrets.Admin)
	if primary == "" {
		primary = request.Ip
	}
	lines := []string{
		"port " + strconv.Itoa(sentinelPort),
		"bind " + request.Ip + " 127.0.0.1",
		"daemonize no",
		"dir " + e.root + "/sentinel",
		"requirepass " + password,
		"sentinel announce-ip " + request.Ip,
		"sentinel announce-port " + strconv.Itoa(sentinelPort),
		"sentinel sentinel-pass " + password,
		fmt.Sprintf("sentinel monitor %s %s %d %d", masterName, primary, dataPort, majorityOf(len(request.Members))),
		"sentinel auth-pass " + masterName + " " + password,
		"sentinel down-after-milliseconds " + masterName + " 5000",
		"sentinel failover-timeout " + masterName + " 60000",
		"sentinel parallel-syncs " + masterName + " 1",
	}

	return strings.Join(lines, "\n") + "\n"
}

func manifestOf(e Engine, request requests.NodeConfigure) ([]byte, error) {
	restart := 3 * time.Second
	if request.Clustered {
		restart = 15 * time.Second
	}
	server := e.process(e.serverUnit(), restart, 60*time.Second, e.root+"/conf/"+e.flavor.Name+".conf")
	server.Enabled = true
	processes := []manifest.Process{server}
	if request.Clustered {
		watcher := e.process(e.sentinelUnit(), 3*time.Second, 10*time.Second, e.root+"/sentinel/sentinel.conf", "--sentinel")
		watcher.After = []string{e.serverUnit()}
		processes = append(processes, watcher)
	}

	return json.MarshalIndent(manifest.Manifest{Processes: processes, Schedules: []manifest.Schedule{}}, "", "  ")
}

func (e Engine) process(name string, restart time.Duration, stop time.Duration, args ...string) manifest.Process {
	return manifest.Process{
		Name:        name,
		User:        e.flavor.Name,
		Argv:        append([]string{"sh", "-c", `umask 0027 && exec "$@"`, "private-umask", e.flavor.Server}, args...),
		Restart:     manifest.RestartAlways,
		RestartSec:  manifest.Duration(restart),
		StopSignal:  "TERM",
		StopTimeout: manifest.Duration(stop),
	}
}
