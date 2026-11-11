package sentinel

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/databasifylabs/engine-sdk/command"
)

const (
	dialTimeout = 5 * time.Second
	clientName  = "databasify"
)

type Reply struct {
	Text    string
	Integer int64
	Items   []Reply
	Null    bool
}

type ReplyError struct{ Message string }

func (e ReplyError) Error() string { return e.Message }

type Server interface {
	Do(ctx context.Context, host string, port int, args ...string) (Reply, error)
}

type respServer struct {
	passwordFile string
}

func (s respServer) Do(ctx context.Context, host string, port int, args ...string) (Reply, error) {
	password, err := os.ReadFile(s.passwordFile)
	if err != nil {
		return Reply{}, command.Unavailable("no password file yet")
	}
	auth := []string{"AUTH", strings.TrimSpace(string(password))}
	if port == dataPort {
		auth = []string{"HELLO", "2", "AUTH", "default", strings.TrimSpace(string(password)), "SETNAME", clientName}
	}

	return ask(ctx, net.JoinHostPort(host, strconv.Itoa(port)), false, auth, args...)
}

func ask(ctx context.Context, address string, useTls bool, auth []string, args ...string) (Reply, error) {
	conn, err := dial(ctx, address, useTls)
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return Reply{}, command.Unavailable(err.Error())
		}

		return Reply{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	reader := bufio.NewReader(conn)
	if _, err := exchange(conn, reader, auth...); err != nil {
		return Reply{}, err
	}

	return exchange(conn, reader, args...)
}

func dial(ctx context.Context, address string, useTls bool) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: dialTimeout}
	if !useTls {
		return dialer.DialContext(ctx, "tcp", address)
	}

	return (&tls.Dialer{NetDialer: dialer, Config: &tls.Config{InsecureSkipVerify: true}}).DialContext(ctx, "tcp", address)
}

func exchange(writer io.Writer, reader *bufio.Reader, args ...string) (Reply, error) {
	if _, err := writer.Write(encodeCommand(args)); err != nil {
		return Reply{}, err
	}

	return readReply(reader)
}

func encodeCommand(args []string) []byte {
	var encoded strings.Builder
	fmt.Fprintf(&encoded, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&encoded, "$%d\r\n%s\r\n", len(arg), arg)
	}

	return []byte(encoded.String())
}

func readReply(reader *bufio.Reader) (Reply, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return Reply{}, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return Reply{}, errors.New("empty reply")
	}
	kind, body := line[0], line[1:]
	switch kind {
	case '+':
		return Reply{Text: body}, nil
	case '-':
		return Reply{}, ReplyError{Message: body}
	case ':':
		number, err := strconv.ParseInt(body, 10, 64)

		return Reply{Integer: number, Text: body}, err
	case '$':
		return readBulk(reader, body)
	case '*':
		return readArray(reader, body)
	}

	return Reply{}, fmt.Errorf("unexpected reply %q", line)
}

func readBulk(reader *bufio.Reader, body string) (Reply, error) {
	size, err := strconv.Atoi(body)
	if err != nil || size < 0 {
		return Reply{Null: true}, err
	}
	data := make([]byte, size+2)
	if _, err := io.ReadFull(reader, data); err != nil {
		return Reply{}, err
	}

	return Reply{Text: string(data[:size])}, nil
}

func readArray(reader *bufio.Reader, body string) (Reply, error) {
	count, err := strconv.Atoi(body)
	if err != nil || count < 0 {
		return Reply{Null: true}, err
	}
	items := make([]Reply, 0, count)
	for range count {
		item, err := readReply(reader)
		if err != nil {
			return Reply{}, err
		}
		items = append(items, item)
	}

	return Reply{Items: items}, nil
}

func infoField(info string, key string) (string, bool) {
	for line := range strings.Lines(info) {
		name, value, found := strings.Cut(strings.TrimSpace(line), ":")
		if found && name == key {
			return value, true
		}
	}

	return "", false
}
