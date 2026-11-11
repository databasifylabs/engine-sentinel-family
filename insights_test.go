package sentinel

import (
	"testing"
)

func slowEntry(micros int64, args ...string) Reply {
	words := make([]Reply, 0, len(args))
	for _, arg := range args {
		words = append(words, Reply{Text: arg})
	}

	return Reply{Items: []Reply{{Integer: 1}, {Integer: 1700000000}, {Integer: micros}, {Items: words}}}
}

func TestInsightsGroupAndMask(t *testing.T) {
	slowlog := Reply{Items: []Reply{slowEntry(3000, "set", "k1", "v"), slowEntry(5000, "SET", "k2", "v"), slowEntry(4000, "GET", "k2")}}

	statements := statementsOf(slowlog)

	if len(statements) != 2 || statements[0].Text != "SET ? ?" || statements[0].Calls != 2 || statements[0].TotalMs != 8 || statements[0].MeanMs != 4 || statements[1].Text != "GET ?" {
		t.Fatalf("statements = %+v", statements)
	}
}

func TestInsightsSkipTheWrappersOwnCommands(t *testing.T) {
	ours := slowEntry(9000, "CONFIG", "GET", "maxmemory")
	ours.Items = append(ours.Items, Reply{Text: "127.0.0.1:5000"}, Reply{Text: clientName})
	slowlog := Reply{Items: []Reply{ours, slowEntry(9000, "AUTH", "x"), slowEntry(1000, "GET", "k")}}

	statements := statementsOf(slowlog)

	if len(statements) != 1 || statements[0].Text != "GET ?" {
		t.Fatalf("statements = %+v", statements)
	}
}
