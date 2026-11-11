package sentinel

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/databasifylabs/engine-sdk/command"
	"github.com/databasifylabs/engine-sdk/contract"
	"github.com/databasifylabs/engine-sdk/requests"
	"github.com/databasifylabs/engine-sdk/responses"
)

const (
	sentinelConf      = "sentinel/sentinel.conf"
	sentinelTimeout   = time.Minute
	failoverTimeout   = 2 * time.Minute
	settleTimeout     = 5 * time.Minute
	rejoinTimeout     = 10 * time.Minute
	primaryTimeout    = 2 * time.Minute
	resetTimeout      = 2 * time.Minute
	forceServeTimout  = 10 * time.Minute
	preferredPriority = "1"
	defaultPriority   = "100"
)

var (
	knownSentinel   = regexp.MustCompile(`(?m)^sentinel known-sentinel ` + masterName + ` (\S+) `)
	epochLines      = regexp.MustCompile(`(?m)^sentinel (config-epoch|leader-epoch) ` + masterName + ` \d+\n|^sentinel current-epoch \d+\n|^sentinel known-replica .*\n`)
	rdbOffset       = regexp.MustCompile(`AUX FIELD repl-offset = '(\d+)'`)
	minReplicasLine = regexp.MustCompile(`(?m)^min-replicas-to-write \d+$`)
	clientFields    = regexp.MustCompile(`(?:^| )(id|addr|laddr)=(\S+)`)
)

type cluster struct{ Engine }

func (e Engine) Cluster() contract.Cluster { return cluster{e} }

func (e cluster) Enable(ctx context.Context, _ requests.ClusterEnable, report contract.Report) error {
	request, err := e.saved()
	if err != nil {
		return err
	}
	report("starting sentinel")
	required := strconv.Itoa(requiredReplicasFor(len(request.Members)))
	if _, err := e.server.Do(ctx, localhost, dataPort, "CONFIG", "SET", "replica-announce-ip", request.Ip, "min-replicas-to-write", required, "min-replicas-max-lag", "10"); err != nil {
		return err
	}
	if err := e.supervisor.EnableNow(e.sentinelUnit()); err != nil {
		return err
	}

	return e.until(ctx, sentinelTimeout, func() (bool, error) { return e.sentinelAnswers(ctx), nil })
}

func (e Engine) sentinelAnswers(ctx context.Context) bool {
	reply, err := e.server.Do(ctx, localhost, sentinelPort, "PING")

	return err == nil && reply.Text == "PONG"
}

func (e cluster) Disable(ctx context.Context) (responses.ClusterDisable, error) {
	_ = e.supervisor.DisableNow(e.sentinelUnit())
	entries, _ := os.ReadDir(e.path("sentinel"))
	for _, entry := range entries {
		_ = os.RemoveAll(e.path("sentinel/" + entry.Name()))
	}
	if _, err := e.server.Do(ctx, localhost, dataPort, "REPLICAOF", "NO", "ONE"); err != nil {
		return responses.ClusterDisable{}, err
	}
	if _, err := e.server.Do(ctx, localhost, dataPort, "CONFIG", "SET", "min-replicas-to-write", "0"); err != nil {
		return responses.ClusterDisable{}, err
	}

	return responses.ClusterDisable{}, e.setReplicaOf("")
}

func (e cluster) Failover(ctx context.Context, request requests.ClusterFailover) error {
	previous := e.sentinelPrimary(ctx)
	if previous == request.Target {
		return nil
	}
	if err := e.preferTarget(ctx, request.Target, preferredPriority); err != nil {
		return err
	}
	defer func() { _ = e.preferTarget(context.WithoutCancel(ctx), request.Target, defaultPriority) }()
	if _, err := e.server.Do(ctx, localhost, sentinelPort, "SENTINEL", "FAILOVER", masterName); err != nil {
		return err
	}
	moved := func() (bool, error) {
		current := e.sentinelPrimary(ctx)

		return current != "" && current != previous, nil
	}
	if err := e.until(ctx, failoverTimeout, moved); err != nil {
		return err
	}
	saved, err := e.saved()
	if err != nil {
		return err
	}
	if previous == saved.Ip {
		if err := e.until(ctx, failoverTimeout, func() (bool, error) { return e.role(ctx) == "slave", nil }); err != nil {
			return err
		}
	}
	_ = e.until(ctx, settleTimeout, func() (bool, error) {
		return (e.role(ctx) == "master" || e.replicaSynced(ctx)) && e.aofWritten(ctx), nil
	})

	return e.setReplicaOf(e.replicaTargetFor(ctx))
}

func (e Engine) replicaTargetFor(ctx context.Context) string {
	replication, err := e.info(ctx, "replication")
	if err != nil {
		return e.replicaOf()
	}
	host, _ := infoField(replication, "master_host")

	return host
}

func (e cluster) Quorum(ctx context.Context) (responses.ClusterQuorum, error) {
	if !e.exists("conf/password") {
		return responses.ClusterQuorum{}, nil
	}
	master, err := e.server.Do(ctx, localhost, sentinelPort, "SENTINEL", "MASTER", masterName)
	if err != nil {
		return responses.ClusterQuorum{}, nil
	}
	others, _ := strconv.ParseInt(pairsOf(master)["num-other-sentinels"], 10, 64)
	quorum := responses.ClusterQuorum{Total: others + 1}
	primary := pairsOf(master)["ip"]
	if !e.servesAsPrimary(ctx, primary) {
		return quorum, nil
	}
	sentinels, err := e.server.Do(ctx, localhost, sentinelPort, "SENTINEL", "SENTINELS", masterName)
	if err != nil {
		return quorum, nil
	}
	reachable := int64(1)
	for _, peer := range sentinels.Items {
		flags := pairsOf(peer)["flags"]
		if !strings.Contains(flags, "s_down") && !strings.Contains(flags, "o_down") && !strings.Contains(flags, "disconnected") {
			reachable++
		}
	}
	quorum.Online = reachable

	return quorum, nil
}

func pairsOf(reply Reply) map[string]string {
	pairs := map[string]string{}
	for index := 0; index+1 < len(reply.Items); index += 2 {
		pairs[reply.Items[index].Text] = reply.Items[index+1].Text
	}

	return pairs
}

func (e Engine) servesAsPrimary(ctx context.Context, host string) bool {
	if host == "" {
		return false
	}
	reply, err := e.server.Do(ctx, host, dataPort, "ROLE")

	return err == nil && len(reply.Items) > 0 && reply.Items[0].Text == "master"
}

func (e cluster) ForceQuorum(ctx context.Context, _ requests.ClusterForceQuorum) (responses.ClusterForceQuorum, error) {
	request, err := e.saved()
	if err != nil {
		return responses.ClusterForceQuorum{}, err
	}
	_ = e.supervisor.Do("reset-failed", e.serverUnit(), e.sentinelUnit())
	_ = e.supervisor.Do("start", e.sentinelUnit())
	if primary, epoch := e.newestPrimary(ctx); primary != "" && epoch >= 0 {
		if primary == request.Ip {
			return responses.ClusterForceQuorum{Leader: primary}, e.requireReplicas(ctx, 0)
		}

		return responses.ClusterForceQuorum{Leader: primary}, e.followIfStopped(primary, request.Ip)
	}
	epoch := e.highestEpoch(ctx) + 1
	if err := e.supervisor.Do("stop", e.sentinelUnit()); err != nil {
		return responses.ClusterForceQuorum{}, err
	}
	rewritten := monitorLine.ReplaceAllString(epochLines.ReplaceAllString(e.read(sentinelConf), ""), "sentinel monitor "+masterName+" "+request.Ip+" ")
	rewritten += fmt.Sprintf("sentinel config-epoch %s %d\nsentinel leader-epoch %s %d\nsentinel current-epoch %d\n", masterName, epoch, masterName, epoch, epoch)
	if _, err := e.write(sentinelConf, rewritten, 0o600); err != nil {
		return responses.ClusterForceQuorum{}, err
	}
	if err := e.serveAsPrimary(ctx); err != nil {
		return responses.ClusterForceQuorum{}, err
	}
	if err := e.supervisor.Do("start", e.sentinelUnit()); err != nil {
		return responses.ClusterForceQuorum{}, err
	}

	return responses.ClusterForceQuorum{Leader: request.Ip}, e.until(ctx, sentinelTimeout, func() (bool, error) { return e.sentinelAnswers(ctx), nil })
}

func (e Engine) followIfStopped(primary string, self string) error {
	if e.supervisor.Active(e.serverUnit()) {
		return nil
	}
	if err := e.setReplicaOf(primary); err != nil {
		return err
	}

	return e.supervisor.Do("start", e.serverUnit())
}

func (e Engine) serveAsPrimary(ctx context.Context) error {
	if err := e.setReplicaOf(""); err != nil {
		return err
	}
	if _, err := e.write(e.confPath(), holdLine.ReplaceAllString(e.conf(), ""), 0o600); err != nil {
		return err
	}
	_ = os.Remove(e.path(startRefused))
	if e.supervisor.Active(e.serverUnit()) {
		_, _ = e.server.Do(ctx, localhost, dataPort, "REPLICAOF", "NO", "ONE")
		_, _ = e.server.Do(ctx, localhost, dataPort, "CONFIG", "SET", "replica-priority", "100")
	} else if err := e.supervisor.Do("start", e.serverUnit()); err != nil {
		return err
	}

	if err := e.until(ctx, forceServeTimout, func() (bool, error) { return e.role(ctx) == "master", nil }); err != nil {
		return err
	}

	return e.requireReplicas(ctx, 0)
}

func (e Engine) requireReplicas(ctx context.Context, count int) error {
	required := strconv.Itoa(count)
	if _, err := e.server.Do(ctx, localhost, dataPort, "CONFIG", "SET", "min-replicas-to-write", required); err != nil {
		return err
	}
	conf := minReplicasLine.ReplaceAllString(e.conf(), "min-replicas-to-write "+required)
	_, err := e.write(e.confPath(), conf, 0o600)

	return err
}

func (e Engine) sentinelHosts() []string {
	hosts := []string{localhost}
	for _, found := range knownSentinel.FindAllStringSubmatch(e.read(sentinelConf), -1) {
		if !slices.Contains(hosts, found[1]) {
			hosts = append(hosts, found[1])
		}
	}

	return hosts
}

func (e Engine) newestPrimary(ctx context.Context) (string, int64) {
	best, bestEpoch := "", int64(-1)
	for _, host := range e.sentinelHosts() {
		master, err := e.server.Do(ctx, host, sentinelPort, "SENTINEL", "MASTER", masterName)
		if err != nil {
			continue
		}
		view := pairsOf(master)
		epoch, err := strconv.ParseInt(view["config-epoch"], 10, 64)
		if err == nil && view["ip"] != "" && epoch > bestEpoch {
			best, bestEpoch = view["ip"], epoch
		}
	}
	if !e.servesAsPrimary(ctx, best) {
		return "", -1
	}

	return best, bestEpoch
}

func (e Engine) highestEpoch(ctx context.Context) int64 {
	highest := int64(0)
	for _, found := range regexp.MustCompile(`(?m)^sentinel (?:current-epoch|config-epoch `+masterName+`) (\d+)$`).FindAllStringSubmatch(e.read(sentinelConf), -1) {
		epoch, _ := strconv.ParseInt(found[1], 10, 64)
		highest = max(highest, epoch)
	}
	for _, host := range e.sentinelHosts() {
		master, err := e.server.Do(ctx, host, sentinelPort, "SENTINEL", "MASTER", masterName)
		if err != nil {
			continue
		}
		epoch, _ := strconv.ParseInt(pairsOf(master)["config-epoch"], 10, 64)
		highest = max(highest, epoch)
	}

	return highest
}

func (e cluster) Position(ctx context.Context) (responses.ClusterPosition, error) {
	position := e.livePosition(ctx)
	if position <= 0 {
		position = max(position, e.storedPosition(ctx))
	}
	if position < 0 && e.hasData() {
		position = 0
	}

	return responses.ClusterPosition{Position: &position}, nil
}

func (e Engine) livePosition(ctx context.Context) int64 {
	if !e.supervisor.Active(e.serverUnit()) {
		return -1
	}
	replication, err := e.info(ctx, "replication")
	if err != nil {
		return -1
	}
	offset, _ := infoField(replication, "master_repl_offset")
	parsed, err := strconv.ParseInt(offset, 10, 64)
	if err != nil {
		return -1
	}

	return parsed
}

func (e Engine) storedPosition(ctx context.Context) int64 {
	output, _ := exec.CommandContext(ctx, e.flavor.CheckRdb, e.path("data/dump.rdb")).CombinedOutput()

	return rdbOffsetIn(string(output))
}

func rdbOffsetIn(output string) int64 {
	found := rdbOffset.FindStringSubmatch(output)
	if found == nil {
		return -1
	}
	parsed, _ := strconv.ParseInt(found[1], 10, 64)

	return parsed
}

func (e cluster) Rejoin(ctx context.Context, request requests.ClusterRejoin, report contract.Report) error {
	saved, err := e.saved()
	if err != nil {
		return err
	}
	_ = e.supervisor.Do("reset-failed", e.serverUnit(), e.sentinelUnit())
	_ = e.supervisor.Do("start", e.sentinelUnit())
	var primary string
	found := func() (bool, error) {
		primary, _ = e.newestPrimary(ctx)

		return primary != "", nil
	}
	report("finding the primary")
	if err := e.until(ctx, primaryTimeout, found); err != nil {
		return err
	}
	replicate := ""
	if primary != saved.Ip {
		replicate = primary
	}
	if err := e.setReplicaOf(replicate); err != nil {
		return err
	}
	if !request.ReadOnly && e.restartLosesData(persistent(saved)) {
		if err := e.hold(); err != nil {
			return err
		}
	}
	report("replicating from the primary")
	if err := e.replicateOrStart(ctx, replicate); err != nil {
		return err
	}
	if replicate == "" {
		return nil
	}
	if err := e.until(ctx, rejoinTimeout, func() (bool, error) { return e.replicaSynced(ctx), nil }); err != nil {
		return err
	}
	if request.ReadOnly || !strings.Contains(e.conf(), promotionHold+"\n") {
		return nil
	}

	return e.release(ctx)
}

func (e Engine) replicateOrStart(ctx context.Context, primary string) error {
	if !e.supervisor.Active(e.serverUnit()) {
		return e.supervisor.Do("start", e.serverUnit())
	}
	if primary == "" {
		_, err := e.server.Do(ctx, localhost, dataPort, "REPLICAOF", "NO", "ONE")

		return err
	}
	_, err := e.server.Do(ctx, localhost, dataPort, "REPLICAOF", primary, strconv.Itoa(dataPort))

	return err
}

func (e Engine) replicaSynced(ctx context.Context) bool {
	replication, err := e.info(ctx, "replication")
	link, _ := infoField(replication, "master_link_status")
	syncing, _ := infoField(replication, "master_sync_in_progress")

	return err == nil && link == "up" && syncing == "0"
}

func (e cluster) UpdateMembership(ctx context.Context, request requests.ClusterMembership) error {
	size := len(request.Members)
	_, _ = e.server.Do(ctx, localhost, sentinelPort, "SENTINEL", "SET", masterName, "quorum", strconv.Itoa(majorityOf(size)))

	return e.requireReplicas(ctx, requiredReplicasFor(size))
}

func (cluster) Admit(context.Context, requests.MemberAdmit) error { return nil }

func (cluster) Promote(context.Context, requests.MemberPromote) error { return nil }

func (cluster) Forget(context.Context, requests.MemberForget) error { return contract.ErrUnsupported }

func (e nodes) Reset(ctx context.Context, request requests.NodeReset) error {
	keep := append([]string{localhost}, request.Hosts...)
	dropRemoved := func() {
		e.dropClients(ctx, dataPort, keep)
		e.dropClients(ctx, sentinelPort, keep)
		_, _ = e.server.Do(ctx, localhost, sentinelPort, "SENTINEL", "RESET", masterName)
	}
	dropRemoved()

	return e.until(ctx, resetTimeout, func() (bool, error) {
		sentinels, err := e.server.Do(ctx, localhost, sentinelPort, "SENTINEL", "SENTINELS", masterName)
		if err != nil {
			return false, nil
		}
		seen := int64(len(sentinels.Items))
		if seen > request.Peers {
			dropRemoved()

			return false, nil
		}

		return seen >= request.Peers, nil
	})
}

func (e Engine) dropClients(ctx context.Context, port int, keep []string) {
	clients, err := e.server.Do(ctx, localhost, port, "CLIENT", "LIST")
	if err != nil {
		return
	}
	for _, id := range removedClients(clients.Text, port, keep) {
		_, _ = e.server.Do(ctx, localhost, port, "CLIENT", "KILL", "ID", id)
	}
}

func removedClients(list string, port int, keep []string) []string {
	var removed []string
	for line := range strings.Lines(list) {
		fields := map[string]string{}
		for _, found := range clientFields.FindAllStringSubmatch(line, -1) {
			fields[found[1]] = found[2]
		}
		host := fields["addr"][:max(0, strings.LastIndex(fields["addr"], ":"))]
		localPort := fields["laddr"][strings.LastIndex(fields["laddr"], ":")+1:]
		if fields["id"] != "" && localPort == strconv.Itoa(port) && !slices.Contains(keep, host) {
			removed = append(removed, fields["id"])
		}
	}

	return removed
}

func (e Engine) role(ctx context.Context) string {
	replication, err := e.info(ctx, "replication")
	if err != nil {
		return ""
	}
	role, _ := infoField(replication, "role")

	return role
}

func (e Engine) preferTarget(ctx context.Context, target string, priority string) error {
	current, err := e.server.Do(ctx, target, dataPort, "CONFIG", "GET", "replica-priority")
	if err != nil {
		return err
	}
	if len(current.Items) < 2 || current.Items[1].Text == "0" {
		return command.Invalid(target + " is a read-only replica and cannot become the primary")
	}
	if _, err := e.server.Do(ctx, target, dataPort, "CONFIG", "SET", "replica-priority", priority); err != nil {
		return err
	}
	_, err = e.server.Do(ctx, target, dataPort, "CONFIG", "REWRITE")

	return err
}

func (e cluster) Watch(context.Context) error { return contract.ErrUnsupported }
