package sentinel

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/databasifylabs/engine-sdk/dtos"
	"github.com/databasifylabs/engine-sdk/requests"
)

func TestShakeConfigForContinuousSync(t *testing.T) {
	request := requests.MigrationStart{Continuous: true, Source: dtos.MigrationSource{Host: "10.0.0.5", Port: 6379, Username: "default", Password: `s"ecret`, Tls: true}}

	got := shakeConfigOf(request, "/srv/valkey/migration", "local\n")

	golden, err := os.ReadFile(filepath.Join("testdata", "shake-continuous.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if got != string(golden) {
		t.Fatalf("config differs from the golden file:\n%s", got)
	}
}

func TestProgressReportsTheCopyDoneOnceConsistent(t *testing.T) {
	engine, _ := testEngine(t, fakeServer{}, true)
	engine.supervisor = progressSupervisor{fakeSupervisor: engine.supervisor.(fakeSupervisor)}
	if err := os.MkdirAll(engine.path("logs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(engine.path("logs/valkey-migration.log"), []byte("start\n\x1b[32mread_count=[10], syncing aof, diff=[0]\x1b[0m\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	progress, err := engine.Migration().Progress(context.Background())

	if err != nil || progress.Copy != "succeeded" || progress.LagSeconds == nil || *progress.LagSeconds != 0 || progress.LagUnknown {
		t.Fatalf("progress = %+v, err = %v", progress, err)
	}
}

func TestProgressKeepsRunningWhileBehind(t *testing.T) {
	engine, _ := testEngine(t, fakeServer{}, true)
	engine.supervisor = progressSupervisor{fakeSupervisor: engine.supervisor.(fakeSupervisor)}
	if err := os.MkdirAll(engine.path("logs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(engine.path("logs/valkey-migration.log"), []byte("start\nread_count=[10], syncing rdb\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	progress, err := engine.Migration().Progress(context.Background())

	if err != nil || progress.Copy != "running" || !progress.LagUnknown {
		t.Fatalf("progress = %+v, err = %v", progress, err)
	}
}

type progressSupervisor struct{ fakeSupervisor }

func (progressSupervisor) Property(_ string, property string) string {
	return map[string]string{"LoadState": "loaded", "ActiveState": "active", "SubState": "running", "ExecMainStatus": "0"}[property]
}

func TestCheckWarnsAboutModules(t *testing.T) {
	warnings := moduleWarnings("# Modules\r\nmodule:name=vectorset,ver=1,api=1\r\nmodule:name=search,ver=20811,api=1\r\n", "# Modules\r\nmodule:name=vectorset,ver=1,api=1\r\n")

	if len(warnings) != 1 || warnings[0] != "the source loads the search module, whose data types this server cannot store" {
		t.Fatalf("warnings = %v", warnings)
	}
}

func TestCheckReadsTheVersionAndKeysOfAReachableSource(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					reply, err := readReply(reader)
					if err != nil {
						return
					}
					switch strings.ToUpper(reply.Items[0].Text) {
					case "AUTH":
						_, _ = conn.Write([]byte("+OK\r\n"))
					case "INFO":
						info := "# Server\r\nredis_version:7.2.4\r\n# Memory\r\nused_memory:2097152\r\n"
						if len(reply.Items) == 1 {
							info += "# Keyspace\r\ndb0:keys=12,expires=0,avg_ttl=0\r\ndb3:keys=3,expires=0,avg_ttl=0\r\n"
						}
						_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(info), info)
					}
				}
			}()
		}
	}()
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	portNumber, _ := strconv.ParseInt(port, 10, 64)
	engine, _ := testEngine(t, fakeServer{replies: map[string]Reply{}}, false)

	report, err := engine.Migration().Check(context.Background(), requests.MigrationCheck{Source: dtos.MigrationSource{Host: host, Port: portNumber, Username: "default", Password: "pw"}})

	if err != nil || report.Version == nil || *report.Version != "7.2.4" || report.Keys == nil || *report.Keys != 15 {
		t.Fatalf("report = %+v, err = %v", report, err)
	}
}
