package sentinel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/databasifylabs/engine-sdk/command"
	"github.com/databasifylabs/engine-sdk/contract"
	"github.com/databasifylabs/engine-sdk/enums"
	"github.com/databasifylabs/engine-sdk/nodefs"
	"github.com/databasifylabs/engine-sdk/requests"
	"github.com/databasifylabs/engine-sdk/responses"
)

const (
	nodeState      = "state/node.json"
	appliedState   = "state/applied.json"
	originalsState = "state/originals.json"
	startRefused   = "conf/start-refused"
	promotionHold  = "replica-priority 0"
	readyTimeout   = 6 * time.Hour
	tlsTimeout     = 5 * time.Minute
	pollInterval   = 2 * time.Second
)

var (
	replicaOfLine = regexp.MustCompile(`(?m)^replicaof (\S+) \d+$`)
	holdLine      = regexp.MustCompile(`(?m)^replica-priority 0\n`)
	monitorLine   = regexp.MustCompile(`(?m)^sentinel monitor ` + masterName + ` (\S+) `)
)

func (e nodes) Configure(ctx context.Context, request requests.NodeConfigure) (responses.NodeConfigure, error) {
	if request.Secrets.Admin == nil {
		return responses.NodeConfigure{}, command.Invalid(e.flavor.Name + " needs the admin secret")
	}
	if err := e.catalog().Check(request.Settings); err != nil {
		return responses.NodeConfigure{}, err
	}
	for _, dir := range []string{"conf", "data", "sentinel", "state"} {
		if err := e.directory(dir); err != nil {
			return responses.NodeConfigure{}, err
		}
	}
	effective, err := e.effective(request)
	if err != nil {
		return responses.NodeConfigure{}, err
	}
	changed, err := e.write("conf/password", *request.Secrets.Admin, 0o600)
	if err != nil {
		return responses.NodeConfigure{}, err
	}
	primary := e.primaryFor(ctx, request)
	files := map[string]string{"conf/" + e.flavor.Name + ".conf": serverConfOf(e.Engine, effective, primary)}
	if request.Clustered && !e.exists("sentinel/sentinel.conf") {
		files["sentinel/sentinel.conf"] = sentinelConfOf(e.Engine, request, primary)
	}
	encoded, err := manifestOf(e.Engine, request)
	if err != nil {
		return responses.NodeConfigure{}, err
	}
	files["conf/processes.json"] = string(encoded)
	state, err := json.Marshal(request)
	if err != nil {
		return responses.NodeConfigure{}, err
	}
	files[nodeState] = string(state)
	for path, content := range files {
		written, err := e.write(path, content, 0o600)
		if err != nil {
			return responses.NodeConfigure{}, err
		}
		changed = changed || written
	}
	if err := e.refuseEmptyPrimary(ctx, request, primary); err != nil {
		return responses.NodeConfigure{}, err
	}

	return responses.NodeConfigure{Changed: changed}, e.supervisor.Do("daemon-reload")
}

func (e nodes) primaryFor(ctx context.Context, request requests.NodeConfigure) string {
	if !request.Clustered {
		return ""
	}
	if reported := e.peerPrimary(ctx, request); reported != "" {
		return reported
	}
	if request.Primary != nil {
		return *request.Primary
	}

	return ""
}

func (e Engine) peerPrimary(ctx context.Context, request requests.NodeConfigure) string {
	for _, member := range request.Members {
		if member.Ip == request.Ip {
			continue
		}
		reply, err := e.server.Do(ctx, member.Ip, sentinelPort, "SENTINEL", "get-master-addr-by-name", masterName)
		if err == nil && len(reply.Items) > 0 && net.ParseIP(reply.Items[0].Text) != nil {
			return reply.Items[0].Text
		}
	}
	if found := monitorLine.FindStringSubmatch(e.read("sentinel/sentinel.conf")); found != nil {
		return found[1]
	}

	return ""
}

func (e nodes) refuseEmptyPrimary(ctx context.Context, request requests.NodeConfigure, primary string) error {
	_ = os.Remove(e.path(startRefused))
	if !request.Clustered || request.ReadOnly {
		return nil
	}
	replicates := primary != "" && primary != request.Ip
	if replicates || !e.restartLosesData(persistent(request)) || !e.peerHoldsKeys(ctx, request) {
		return nil
	}
	_, err := e.write(startRefused, "refusing to start as an empty primary while a peer still holds the data: move the primary to that peer first", 0o600)

	return err
}

func (e Engine) peerHoldsKeys(ctx context.Context, request requests.NodeConfigure) bool {
	for _, member := range request.Members {
		if member.Ip == request.Ip {
			continue
		}
		reply, err := e.server.Do(ctx, member.Ip, dataPort, "DBSIZE")
		if err == nil && reply.Integer > 0 {
			return true
		}
	}

	return false
}

func (e Engine) restartLosesData(persistent bool) bool {
	return !persistent || !e.hasData()
}

func (e Engine) hasData() bool {
	if info, err := os.Stat(e.path("data/dump.rdb")); err == nil && info.Size() > 0 {
		return true
	}
	entries, err := os.ReadDir(e.path("data/appendonlydir"))

	return err == nil && len(entries) > 0
}

func (e nodes) Start(_ context.Context) error {
	request, err := e.saved()
	if err != nil {
		return err
	}
	if refused := e.read(startRefused); refused != "" {
		return command.Failure{Code: enums.ErrorCodeBusy, Message: refused}
	}
	if request.Clustered && !request.ReadOnly && e.replicaOf() != "" && e.restartLosesData(persistent(request)) {
		if err := e.hold(); err != nil {
			return err
		}
	}

	return e.supervisor.Do("start", e.serverUnit())
}

func (e nodes) Ready(ctx context.Context, _ requests.NodeReady, report contract.Report) error {
	request, err := e.saved()
	if err != nil {
		return err
	}
	report("waiting for " + e.flavor.Name)
	baseline := e.restarts()
	if err := e.until(ctx, readyTimeout, func() (bool, error) { return e.serving(ctx, request, baseline, report) }); err != nil {
		return err
	}
	report("waiting for TLS")
	if err := e.until(ctx, tlsTimeout, func() (bool, error) { return e.listening(tlsPort), nil }); err != nil {
		return err
	}
	if request.ReadOnly || !strings.Contains(e.conf(), promotionHold+"\n") {
		return nil
	}

	return e.release(ctx)
}

func (e Engine) restarts() int {
	restarts, _ := strconv.Atoi(e.supervisor.Property(e.serverUnit(), "NRestarts"))

	return restarts
}

func (e nodes) serving(ctx context.Context, request requests.NodeConfigure, baseline int, report contract.Report) (bool, error) {
	if e.restarts()-baseline > 3 {
		return false, fmt.Errorf("%s keeps stopping while it starts", e.flavor.Name)
	}
	if !e.supervisor.Active(e.serverUnit()) {
		return false, nil
	}
	replication, err := e.info(ctx, "replication")
	if err != nil {
		return false, nil
	}
	synced, err := e.synced(ctx, request, replication, report)
	if err != nil || !synced {
		return false, err
	}

	return e.aofWritten(ctx), nil
}

func (e nodes) synced(ctx context.Context, request requests.NodeConfigure, replication string, report contract.Report) (bool, error) {
	role, _ := infoField(replication, "role")
	if e.replicaOf() == "" {
		return role == "master", nil
	}
	link, _ := infoField(replication, "master_link_status")
	if role != "slave" || link != "up" {
		return false, nil
	}
	if !request.Clustered {
		return true, nil
	}
	primary := e.sentinelPrimary(ctx)
	current, _ := infoField(replication, "master_host")
	if primary == "" || primary == request.Ip || primary == current {
		return true, nil
	}
	report("following the primary")

	return false, e.follow(ctx, primary)
}

func (e Engine) sentinelPrimary(ctx context.Context) string {
	reply, err := e.server.Do(ctx, localhost, sentinelPort, "SENTINEL", "get-master-addr-by-name", masterName)
	if err != nil || len(reply.Items) == 0 {
		return ""
	}

	return reply.Items[0].Text
}

func (e Engine) follow(ctx context.Context, primary string) error {
	if _, err := e.server.Do(ctx, localhost, dataPort, "REPLICAOF", primary, strconv.Itoa(dataPort)); err != nil {
		return err
	}

	return e.setReplicaOf(primary)
}

func (e Engine) aofWritten(ctx context.Context) bool {
	persistence, err := e.info(ctx, "persistence")
	if err != nil {
		return false
	}
	running, _ := infoField(persistence, "aof_rewrite_in_progress")
	scheduled, _ := infoField(persistence, "aof_rewrite_scheduled")

	return running == "0" && scheduled == "0"
}

func (e Engine) listening(port int64) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(localhost, strconv.FormatInt(port, 10)), time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()

	return true
}

func (e Engine) release(ctx context.Context) error {
	if _, err := e.server.Do(ctx, localhost, dataPort, "CONFIG", "SET", "replica-priority", "100"); err != nil {
		return err
	}
	_, err := e.write(e.confPath(), holdLine.ReplaceAllString(e.conf(), ""), 0o600)

	return err
}

func (e Engine) hold() error {
	if strings.Contains(e.conf(), promotionHold+"\n") {
		return nil
	}
	_, err := e.write(e.confPath(), e.conf()+promotionHold+"\n", 0o600)

	return err
}

func (e nodes) Wake(_ context.Context, _ requests.NodeWake) error {
	_ = e.supervisor.Do("stop", e.sentinelUnit())
	_ = e.supervisor.Kill("KILL", e.serverUnit())

	return e.supervisor.Do("stop", e.serverUnit())
}

func (e nodes) Bootstrap(context.Context, requests.NodeBootstrap) error {
	return nil
}

func (e nodes) SyncConfig(context.Context) (responses.ConfigSync, error) {
	return responses.ConfigSync{}, nil
}

func (e nodes) Metrics(ctx context.Context) (responses.NodeMetrics, error) {
	stats, err := e.info(ctx, "all")
	if err != nil {
		return responses.NodeMetrics{}, err
	}
	number := func(key string) int64 {
		value, _ := infoField(stats, key)
		parsed, _ := strconv.ParseInt(value, 10, 64)

		return parsed
	}
	keys, hits, misses, evicted := keyCount(stats), number("keyspace_hits"), number("keyspace_misses"), number("evicted_keys")

	return responses.NodeMetrics{
		Connections: number("connected_clients"),
		Queries:     number("instantaneous_ops_per_sec"),
		DataMb:      number("used_memory") / (1 << 20),
		Keys:        &keys, Hits: &hits, Misses: &misses, Evicted: &evicted,
	}, nil
}

func keyCount(info string) int64 {
	var total int64
	for line := range strings.Lines(info) {
		name, fields, found := strings.Cut(strings.TrimSpace(line), ":")
		if !found || !strings.HasPrefix(name, "db") {
			continue
		}
		for field := range strings.SplitSeq(fields, ",") {
			if count, isKeys := strings.CutPrefix(field, "keys="); isKeys {
				parsed, _ := strconv.ParseInt(count, 10, 64)
				total += parsed
			}
		}
	}

	return total
}

func (e nodes) ApplyConfig(ctx context.Context) error {
	saved, err := e.saved()
	if err != nil {
		return err
	}
	request, err := e.effective(saved)
	if err != nil {
		return err
	}
	request.Settings = withoutKeys(request.Settings, e.restartOnly())
	desired := map[string]string{"appendonly": "no", "save": ""}
	if persistent(request) {
		desired = map[string]string{"appendonly": "yes", "save": snapshotSchedule}
	}
	for key, value := range request.Settings {
		name, setting, _ := strings.Cut(directiveOf(key, value), " ")
		desired[name] = strings.Trim(setting, `"`)
	}
	originals := map[string]string{}
	_ = json.Unmarshal([]byte(e.read(originalsState)), &originals)
	for key := range request.Settings {
		name := configKey(key)
		if _, known := originals[name]; known {
			continue
		}
		value, err := e.configGet(ctx, name)
		if err != nil {
			return err
		}
		originals[name] = value
	}
	var applied []string
	_ = json.Unmarshal([]byte(e.read(appliedState)), &applied)
	for _, key := range applied {
		if _, kept := request.Settings[key]; !kept {
			desired[configKey(key)] = originals[configKey(key)]
		}
	}
	encodedOriginals, _ := json.Marshal(originals)
	if _, err := e.write(originalsState, string(encodedOriginals), 0o600); err != nil {
		return err
	}
	for _, key := range slices.Sorted(maps.Keys(desired)) {
		if err := e.configSet(ctx, key, desired[key]); err != nil {
			return err
		}
	}
	names := slices.Sorted(maps.Keys(request.Settings))
	encoded, _ := json.Marshal(names)
	_, err = e.write(appliedState, string(encoded), 0o600)

	return err
}

func withoutKeys(values map[string]string, keys []string) map[string]string {
	kept := maps.Clone(values)
	for _, key := range keys {
		delete(kept, key)
	}

	return kept
}

func configKey(key string) string {
	name, _, _ := strings.Cut(directiveOf(key, "0"), " ")

	return name
}

func (e Engine) configGet(ctx context.Context, key string) (string, error) {
	reply, err := e.server.Do(ctx, localhost, dataPort, "CONFIG", "GET", key)
	if err != nil {
		return "", err
	}
	if len(reply.Items) < 2 {
		return "", fmt.Errorf("%s has no setting named %s", e.flavor.Name, key)
	}

	return reply.Items[1].Text, nil
}

func (e Engine) configSet(ctx context.Context, key string, value string) error {
	reply, err := e.server.Do(ctx, localhost, dataPort, "CONFIG", "SET", key, value)
	var refused ReplyError
	if errors.As(err, &refused) {
		return fmt.Errorf("could not set %s: %s", key, refused.Message)
	}
	if err != nil {
		return err
	}
	if reply.Text != "OK" {
		return fmt.Errorf("could not set %s: %s", key, reply.Text)
	}

	return nil
}

func (e nodes) InstallTLS(ctx context.Context, request requests.TlsInstall) error {
	return nodefs.Root(e.root).InstallTLS(ctx, request, true, e.ReloadTLS)
}

func (e nodes) Restart(context.Context) error {
	if err := e.restoreRestartOnly(); err != nil {
		return err
	}

	return e.supervisor.Do("restart", e.serverUnit())
}

func (e Engine) restoreRestartOnly() error {
	saved, err := e.saved()
	if err != nil {
		return nil
	}
	request, err := e.effective(saved)
	if err != nil {
		return err
	}
	conf := e.conf()
	for _, key := range e.restartOnly() {
		conf = regexp.MustCompile(`(?m)^`+regexp.QuoteMeta(configKey(key))+` .*\n`).ReplaceAllString(conf, "")
		if value, set := request.Settings[key]; set {
			conf += directiveOf(key, value) + "\n"
		}
	}
	_, err = e.write(e.confPath(), conf, 0o600)

	return err
}

func (e nodes) ReloadTLS(ctx context.Context) error {
	if !e.supervisor.Active(e.serverUnit()) {
		return nil
	}
	pem, ca := e.root+"/tls/server.pem", e.root+"/tls/ca.crt"
	_, err := e.server.Do(ctx, localhost, dataPort, "CONFIG", "SET", "tls-cert-file", pem, "tls-key-file", pem, "tls-ca-cert-file", ca)

	return err
}

func (e Engine) saved() (requests.NodeConfigure, error) {
	var request requests.NodeConfigure
	data, err := os.ReadFile(e.path(nodeState))
	if err != nil {
		return request, command.Failure{Code: enums.ErrorCodeBusy, Message: "run node configure first"}
	}

	return request, json.Unmarshal(data, &request)
}

func (e Engine) until(ctx context.Context, timeout time.Duration, check func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		done, err := check()
		if err != nil || done {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not become ready within %s", e.flavor.Name, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func (e Engine) confPath() string {
	return "conf/" + e.flavor.Name + ".conf"
}

func (e Engine) conf() string {
	return e.read(e.confPath())
}

func (e Engine) replicaOf() string {
	if found := replicaOfLine.FindStringSubmatch(e.conf()); found != nil {
		return found[1]
	}

	return ""
}

func (e Engine) setReplicaOf(primary string) error {
	conf := replicaOfLine.ReplaceAllString(e.conf(), "")
	conf = strings.TrimRight(conf, "\n") + "\n"
	if primary != "" {
		conf += "replicaof " + primary + " " + strconv.Itoa(dataPort) + "\n"
	}
	_, err := e.write(e.confPath(), conf, 0o600)

	return err
}

func (e Engine) path(relative string) string {
	return filepath.Join(e.root, relative)
}

func (e Engine) exists(relative string) bool {
	_, err := os.Stat(e.path(relative))

	return err == nil
}

func (e Engine) read(relative string) string {
	data, err := os.ReadFile(e.path(relative))
	if err != nil {
		return ""
	}

	return string(data)
}

func (e Engine) directory(relative string) error {
	path := e.path(relative)
	if err := os.MkdirAll(path, 0o750); err != nil {
		return err
	}

	return own(path)
}

func (e Engine) write(relative string, content string, mode os.FileMode) (bool, error) {
	path := e.path(relative)
	if current, err := os.ReadFile(path); err == nil && bytes.Equal(current, []byte(content)) {
		return false, nil
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, []byte(content), mode); err != nil {
		return false, err
	}
	if err := own(temporary); err != nil {
		return false, err
	}

	return true, os.Rename(temporary, path)
}

func own(path string) error {
	err := os.Lchown(path, engineUID, engineUID)
	if errors.Is(err, os.ErrPermission) && os.Geteuid() != 0 {
		return nil
	}

	return err
}
