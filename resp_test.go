package sentinel

import (
	"bufio"
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func fakeRespServer(t *testing.T, answers []string) (string, int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for _, answer := range answers {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			reader := bufio.NewReader(conn)
			for _, reply := range []string{"+OK\r\n", answer} {
				if _, err := readReply(reader); err != nil {
					break
				}
				_, _ = conn.Write([]byte(reply))
			}
			_ = conn.Close()
		}
	}()
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	number, _ := strconv.Atoi(port)

	return host, number
}

func TestRespRoundTrip(t *testing.T) {
	passwordFile := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(passwordFile, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	answers := []string{"+PONG\r\n", ":5\r\n", "$5\r\nhello\r\n", "*2\r\n$1\r\na\r\n:7\r\n", "$-1\r\n"}
	host, port := fakeRespServer(t, answers)
	server := respServer{passwordFile: passwordFile}

	replies := make([]Reply, 0, len(answers))
	for range answers {
		reply, err := server.Do(context.Background(), host, port, "PING")
		if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, reply)
	}

	if replies[0].Text != "PONG" || replies[1].Integer != 5 || replies[2].Text != "hello" || len(replies[3].Items) != 2 || replies[3].Items[1].Integer != 7 || !replies[4].Null {
		t.Fatalf("replies = %+v", replies)
	}
}

func TestRespErrorReplyIsAnError(t *testing.T) {
	reply, err := readReply(bufio.NewReader(stringReader("-ERR wrong\r\n")))

	if _, isReplyError := err.(ReplyError); !isReplyError || reply.Text != "" {
		t.Fatalf("reply = %+v, err = %v", reply, err)
	}
}
