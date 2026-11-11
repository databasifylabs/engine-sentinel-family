package sentinel

import (
	"context"
	"net"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/databasifylabs/engine-sdk/contract"
	"github.com/databasifylabs/engine-sdk/ctl"
	"github.com/databasifylabs/engine-sdk/dtos"
	"github.com/databasifylabs/engine-sdk/requests"
	"github.com/databasifylabs/engine-sdk/responses"
)

const (
	migrationDir   = "migration"
	shakeBinary    = "/usr/local/bin/redis-shake"
	consistentMark = "syncing aof, diff=[0]"
)

var ansiCodes = regexp.MustCompile(`\x1b\[[0-9;]*m`)

type migration struct{ Engine }

func (e Engine) Migration() contract.Migration { return migration{e} }

func (e Engine) migrationUnit() string { return e.flavor.Name + "-migration" }

func (e migration) Check(ctx context.Context, request requests.MigrationCheck) (responses.MigrationCheck, error) {
	report := responses.MigrationCheck{Databases: []string{}, Unreadable: []string{}, Unowned: []string{}, Occupied: []string{}, Warnings: []string{}}
	info, err := askSource(ctx, request.Source, "INFO")
	if err != nil {
		message := strings.SplitN(err.Error(), "\n", 2)[0]
		report.Error = &message

		return report, nil
	}
	report.Reachable, report.CanReplicate = true, true
	for _, field := range []string{"valkey_version", "redis_version"} {
		if version, found := infoField(info.Text, field); found {
			report.Version = &version

			break
		}
	}
	memory, _ := infoField(info.Text, "used_memory")
	if bytes, err := strconv.ParseInt(memory, 10, 64); err == nil {
		size := bytes / (1 << 20)
		report.SizeMb = &size
	}
	keys := keyCount(info.Text)
	report.Keys = &keys
	if modules, err := askSource(ctx, request.Source, "INFO", "modules"); err == nil {
		local, _ := e.info(ctx, "modules")
		report.Warnings = append(report.Warnings, moduleWarnings(modules.Text, local)...)
	}

	return report, nil
}

func askSource(ctx context.Context, source dtos.MigrationSource, args ...string) (Reply, error) {
	auth := []string{"AUTH", source.Password}
	if source.Username != "default" {
		auth = []string{"AUTH", source.Username, source.Password}
	}

	return ask(ctx, net.JoinHostPort(source.Host, strconv.FormatInt(source.Port, 10)), source.Tls, auth, args...)
}

func moduleWarnings(source string, local string) []string {
	warnings := []string{}
	available := moduleNames(local)
	for _, name := range moduleNames(source) {
		if !slices.Contains(available, name) {
			warnings = append(warnings, "the source loads the "+name+" module, whose data types this server cannot store")
		}
	}

	return warnings
}

func moduleNames(modules string) []string {
	names := []string{}
	for line := range strings.Lines(modules) {
		_, fields, found := strings.Cut(strings.TrimSpace(line), "name=")
		if found {
			name, _, _ := strings.Cut(fields, ",")
			names = append(names, name)
		}
	}

	return names
}

func (e migration) Start(_ context.Context, request requests.MigrationStart) error {
	_ = e.supervisor.Do("stop", e.migrationUnit())
	_ = e.supervisor.Do("reset-failed", e.migrationUnit())
	if err := os.RemoveAll(e.path(migrationDir)); err != nil {
		return err
	}
	if err := os.MkdirAll(e.path(migrationDir), 0o700); err != nil {
		return err
	}
	if _, err := e.write(migrationDir+"/shake.toml", shakeConfigOf(request, e.path(migrationDir), e.read("conf/password")), 0o600); err != nil {
		return err
	}

	return e.supervisor.Run(ctl.Transient{
		Unit:            e.migrationUnit(),
		Argv:            []string{shakeBinary, e.path(migrationDir + "/shake.toml")},
		Workdir:         e.path(migrationDir),
		RemainAfterExit: true,
	})
}

func shakeConfigOf(request requests.MigrationStart, dir string, password string) string {
	reader := []string{"[scan_reader]", "scan = true", "ksn = false", "count = 100"}
	if request.Continuous {
		reader = []string{"[sync_reader]", "sync_rdb = true", "sync_aof = true"}
	}
	source := request.Source
	lines := append(reader,
		"cluster = false",
		"address = "+quoted(net.JoinHostPort(source.Host, strconv.FormatInt(source.Port, 10))),
		"username = "+quoted(source.Username),
		"password = "+quoted(source.Password),
		"tls = "+strconv.FormatBool(source.Tls),
		"",
		"[redis_writer]",
		"cluster = false",
		"address = "+quoted(net.JoinHostPort(localhost, strconv.Itoa(dataPort))),
		"password = "+quoted(strings.TrimSpace(password)),
		"tls = false",
		"",
		"[advanced]",
		"dir = "+quoted(dir),
		"status_port = 0",
		`log_file = "shake.log"`,
		`log_level = "info"`,
		"log_interval = 5",
		`rdb_restore_command_behavior = "rewrite"`,
	)

	return strings.Join(lines, "\n") + "\n"
}

func (e migration) Progress(context.Context) (responses.MigrationProgress, error) {
	unit := e.migrationUnit()
	progress := responses.MigrationProgress{Copy: "lost", LagUnknown: true}
	if e.supervisor.Property(unit, "LoadState") == "not-found" {
		return progress, nil
	}
	progress.Copy = copyStateOf(e.supervisor.Property(unit, "ActiveState"), e.supervisor.Property(unit, "SubState"), e.supervisor.Property(unit, "ExecMainStatus"))
	progress.Log = ansiCodes.ReplaceAllString(lastLines(e.read(logFile(unit)), 3), "")
	if strings.Contains(progress.Log, consistentMark) {
		zero := int64(0)
		progress.LagSeconds, progress.LagUnknown = &zero, false
		if progress.Copy == "running" {
			progress.Copy = "succeeded"
		}
	}

	return progress, nil
}

func copyStateOf(active string, sub string, status string) string {
	if active == "active" && sub == "running" {
		return "running"
	}
	if status == "0" {
		return "succeeded"
	}

	return "failed"
}

func logFile(unit string) string {
	return "logs/" + unit + ".log"
}

func lastLines(text string, count int) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")

	return strings.Join(lines[max(0, len(lines)-count):], " ")
}

func (e migration) Stop(context.Context) error {
	_ = e.supervisor.Do("stop", e.migrationUnit())
	_ = e.supervisor.Do("reset-failed", e.migrationUnit())

	return os.RemoveAll(e.path(migrationDir))
}

func (migration) Copy(context.Context) error {
	return contract.ErrUnsupported
}
