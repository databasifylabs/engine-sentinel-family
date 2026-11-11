package sentinel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/databasifylabs/engine-sdk/command"
	"github.com/databasifylabs/engine-sdk/enums"
)

type fakeServer struct {
	replies map[string]Reply
	err     error
	sent    *[]string
}

func (f fakeServer) Do(_ context.Context, host string, port int, args ...string) (Reply, error) {
	command := strings.Join(args, " ")
	if f.sent != nil {
		*f.sent = append(*f.sent, command)
	}
	if f.err != nil {
		return Reply{}, f.err
	}
	if reply, found := f.replies[fmt.Sprintf("%s:%d %s", host, port, command)]; found {
		return reply, nil
	}
	if reply, found := f.replies[command]; found {
		return reply, nil
	}

	return Reply{}, command2err(command)
}

func command2err(command string) error {
	if strings.HasPrefix(command, "CONFIG SET") || strings.HasPrefix(command, "REPLICAOF") {
		return nil
	}

	return errors.New("unexpected command " + command)
}

func stringReader(text string) *strings.Reader { return strings.NewReader(text) }

func statusWith(t *testing.T, replication string) enums.NodeRole {
	t.Helper()
	server := fakeServer{replies: map[string]Reply{
		"INFO replication": {Text: replication},
		"INFO server":      {Text: "# Server\r\nvalkey_version:9.1.3\r\n"},
	}}
	status, err := Engine{flavor: Valkey, server: server}.Nodes().Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Version == nil || *status.Version != "9.1.3" {
		t.Fatalf("version = %v", status.Version)
	}

	return status.Role
}

func TestStatusMapsReplication(t *testing.T) {
	cases := map[string]enums.NodeRole{
		"role:master\r\n": enums.NodeRolePrimary,
		"role:slave\r\nmaster_link_status:up\r\nmaster_sync_in_progress:0\r\n":   enums.NodeRoleReplica,
		"role:slave\r\nmaster_link_status:down\r\nmaster_sync_in_progress:0\r\n": enums.NodeRoleUnknown,
		"role:slave\r\nmaster_link_status:up\r\nmaster_sync_in_progress:1\r\n":   enums.NodeRoleUnknown,
	}
	for replication, want := range cases {
		if got := statusWith(t, replication); got != want {
			t.Errorf("%q: role = %q, want %q", replication, got, want)
		}
	}
}

func TestStatusOfflineWhenUnreachable(t *testing.T) {
	engine := Engine{flavor: Valkey, server: fakeServer{err: command.Unavailable("refused")}}

	status, err := engine.Nodes().Status(context.Background())

	if err != nil || status.Role != enums.NodeRoleOffline || status.Version != nil {
		t.Fatalf("status = %+v, err = %v", status, err)
	}
}
