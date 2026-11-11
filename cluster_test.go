package sentinel

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/databasifylabs/engine-sdk/contract"
	"github.com/databasifylabs/engine-sdk/requests"
)

func masterView(ip string, epoch string, others string) Reply {
	return Reply{Items: []Reply{{Text: "ip"}, {Text: ip}, {Text: "config-epoch"}, {Text: epoch}, {Text: "num-other-sentinels"}, {Text: others}}}
}

func TestForceQuorumKeepsALivePrimary(t *testing.T) {
	server := fakeServer{replies: map[string]Reply{
		"127.0.0.1:26379 SENTINEL MASTER main": masterView("10.0.0.2", "7", "2"),
		"10.0.0.2:6379 ROLE":                   {Items: []Reply{{Text: "master"}}},
	}}
	engine, _ := testEngine(t, server, true)
	configure(t, engine, clusteredRequest("10.0.0.1", "10.0.0.2"))
	before := engine.read(sentinelConf)

	reply, err := engine.Cluster().ForceQuorum(context.Background(), requests.ClusterForceQuorum{OthersDown: true})

	if err != nil || reply.Leader != "10.0.0.2" || engine.read(sentinelConf) != before {
		t.Fatalf("reply = %+v, err = %v", reply, err)
	}
}

func TestForceQuorumBumpsTheEpoch(t *testing.T) {
	server := fakeServer{replies: map[string]Reply{
		"127.0.0.1:26379 SENTINEL MASTER main": masterView("10.0.0.2", "7", "2"),
		"10.0.0.2:6379 ROLE":                   {Items: []Reply{{Text: "slave"}}},
		"INFO replication":                     {Text: "role:master\r\n"},
		"127.0.0.1:26379 PING":                 {Text: "PONG"},
	}}
	engine, _ := testEngine(t, server, true)
	configure(t, engine, clusteredRequest("10.0.0.1", "10.0.0.2"))

	reply, err := engine.Cluster().ForceQuorum(context.Background(), requests.ClusterForceQuorum{OthersDown: true})

	conf := engine.read(sentinelConf)
	if err != nil || reply.Leader != "10.0.0.1" || !strings.Contains(conf, "sentinel monitor main 10.0.0.1 6379") || !strings.Contains(conf, "sentinel config-epoch main 8\n") || !strings.Contains(conf, "sentinel current-epoch 8\n") {
		t.Fatalf("reply = %+v, err = %v, conf =\n%s", reply, err, conf)
	}
	if engine.replicaOf() != "" {
		t.Fatalf("still replicates from %s", engine.replicaOf())
	}
	if !strings.Contains(engine.conf(), "\nmin-replicas-to-write 0\n") {
		t.Fatalf("a lone survivor still requires replicas:\n%s", engine.conf())
	}
}

func TestPositionFallsBackToTheRdbOffset(t *testing.T) {
	output := "[offset 0] Checking RDB file dump.rdb\n[offset 27] AUX FIELD repl-offset = '12345'\n"

	if got := rdbOffsetIn(output); got != 12345 {
		t.Fatalf("offset = %d", got)
	}
	if got := rdbOffsetIn("no offset here"); got != -1 {
		t.Fatalf("offset = %d", got)
	}
}

func TestMembershipQuorumIsMajority(t *testing.T) {
	cases := map[int][2]int{1: {1, 0}, 2: {2, 0}, 3: {2, 1}, 5: {3, 1}}
	for size, want := range cases {
		if majorityOf(size) != want[0] || requiredReplicasFor(size) != want[1] {
			t.Errorf("size %d: quorum %d, replicas %d", size, majorityOf(size), requiredReplicasFor(size))
		}
	}
}

func TestResetDropsClientsFromRemovedHosts(t *testing.T) {
	list := "id=3 addr=10.0.0.9:51000 laddr=10.0.0.1:6379 name=\nid=4 addr=10.0.0.2:51001 laddr=10.0.0.1:6379 name=\nid=5 addr=10.0.0.9:51002 laddr=10.0.0.1:26379 name=\n"

	removed := removedClients(list, 6379, []string{"127.0.0.1", "10.0.0.2"})

	if !slices.Equal(removed, []string{"3"}) {
		t.Fatalf("removed = %v", removed)
	}
}

func TestQuorumCountsReachableSentinels(t *testing.T) {
	server := fakeServer{replies: map[string]Reply{
		"127.0.0.1:26379 SENTINEL MASTER main":    masterView("10.0.0.1", "3", "2"),
		"10.0.0.1:6379 ROLE":                      {Items: []Reply{{Text: "master"}}},
		"127.0.0.1:26379 SENTINEL SENTINELS main": {Items: []Reply{{Items: []Reply{{Text: "flags"}, {Text: "sentinel"}}}, {Items: []Reply{{Text: "flags"}, {Text: "s_down,sentinel"}}}}},
	}}
	engine, _ := testEngine(t, server, true)
	configure(t, engine, clusteredRequest("10.0.0.1", "10.0.0.1"))

	quorum, err := engine.Cluster().Quorum(context.Background())

	if err != nil || quorum.Total != 3 || quorum.Online != 2 {
		t.Fatalf("quorum = %+v, err = %v", quorum, err)
	}
}

func TestFailoverWaitsUntilTheOldPrimaryIsDemoted(t *testing.T) {
	calls := 0
	server := &sequenceServer{replies: map[string][]Reply{
		"127.0.0.1:26379 SENTINEL get-master-addr-by-name main": {{Items: []Reply{{Text: "10.0.0.1"}}}, {Items: []Reply{{Text: "10.0.0.2"}}}},
		"127.0.0.1:6379 INFO replication":                       {{Text: "role:master\r\n"}, {Text: "role:slave\r\nmaster_host:10.0.0.2\r\nmaster_link_status:up\r\nmaster_sync_in_progress:0\r\n"}},
		"127.0.0.1:6379 INFO persistence":                       {{Text: "aof_rewrite_in_progress:0\r\naof_rewrite_scheduled:0\r\n"}},
		"10.0.0.2:6379 CONFIG GET replica-priority":             {{Items: []Reply{{Text: "replica-priority"}, {Text: "100"}}}},
	}, calls: &calls}
	engine, _ := testEngine(t, fakeServer{}, true)
	configure(t, engine, clusteredRequest("10.0.0.1", "10.0.0.1"))
	engine.server = server

	if err := engine.Cluster().Failover(context.Background(), requests.ClusterFailover{Target: "10.0.0.2"}); err != nil {
		t.Fatal(err)
	}

	if engine.replicaOf() != "10.0.0.2" {
		t.Fatalf("replicaof = %q after %d calls", engine.replicaOf(), calls)
	}
}

type sequenceServer struct {
	replies map[string][]Reply
	calls   *int
}

func (s *sequenceServer) Do(_ context.Context, host string, port int, args ...string) (Reply, error) {
	*s.calls++
	key := host + ":" + strconv.Itoa(port) + " " + strings.Join(args, " ")
	queue := s.replies[key]
	if len(queue) == 0 {
		return Reply{}, nil
	}
	reply := queue[0]
	if len(queue) > 1 {
		s.replies[key] = queue[1:]
	}

	return reply, nil
}

func TestFailoverPrefersTheTargetThenRestoresIt(t *testing.T) {
	sent := []string{}
	server := fakeServer{sent: &sent, replies: map[string]Reply{
		"127.0.0.1:26379 SENTINEL get-master-addr-by-name main": {Items: []Reply{{Text: "10.0.0.2"}}},
		"10.0.0.3:6379 CONFIG GET replica-priority":             {Items: []Reply{{Text: "replica-priority"}, {Text: "100"}}},
		"10.0.0.3:6379 CONFIG REWRITE":                          {Text: "OK"},
	}}
	engine, _ := testEngine(t, fakeServer{}, true)
	configure(t, engine, clusteredRequest("10.0.0.2", "10.0.0.1"))
	engine.server = server

	_ = engine.Cluster().Failover(context.Background(), requests.ClusterFailover{Target: "10.0.0.3"})

	if len(sent) < 5 || sent[2] != "CONFIG SET replica-priority 1" || !slices.Equal(sent[len(sent)-2:], []string{"CONFIG SET replica-priority 100", "CONFIG REWRITE"}) {
		t.Fatalf("sent = %q", sent)
	}
}

func TestFailoverRefusesAReadOnlyTarget(t *testing.T) {
	server := fakeServer{replies: map[string]Reply{
		"127.0.0.1:26379 SENTINEL get-master-addr-by-name main": {Items: []Reply{{Text: "10.0.0.1"}}},
		"10.0.0.3:6379 CONFIG GET replica-priority":             {Items: []Reply{{Text: "replica-priority"}, {Text: "0"}}},
	}}
	engine, _ := testEngine(t, server, true)

	err := engine.Cluster().Failover(context.Background(), requests.ClusterFailover{Target: "10.0.0.3"})

	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("err = %v", err)
	}
}

func TestForgetLeavesRemovedPeersToNodeReset(t *testing.T) {
	engine, _ := testEngine(t, fakeServer{}, true)

	err := engine.Cluster().Forget(context.Background(), requests.MemberForget{Ip: "10.0.0.3"})

	if !errors.Is(err, contract.ErrUnsupported) {
		t.Fatalf("err = %v, want unsupported so the app resets the sentinels", err)
	}
}
