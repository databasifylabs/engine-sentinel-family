package sentinel

import (
	"cmp"
	"context"
	"slices"
	"strings"

	"github.com/databasifylabs/engine-sdk/dtos"
	"github.com/databasifylabs/engine-sdk/responses"
)

const (
	slowlogEntries = "128"
	topStatements  = 50
)

func (e nodes) Insights(ctx context.Context) (responses.QueryInsights, error) {
	slowlog, err := e.server.Do(ctx, localhost, dataPort, "SLOWLOG", "GET", slowlogEntries)
	if err != nil {
		return responses.QueryInsights{}, err
	}

	return responses.QueryInsights{Statements: statementsOf(slowlog)}, nil
}

func statementsOf(slowlog Reply) []dtos.Statement {
	totals := map[string]*dtos.Statement{}
	micros := map[string]int64{}
	for _, entry := range slowlog.Items {
		if len(entry.Items) < 4 || len(entry.Items[3].Items) == 0 || fromWrapper(entry) {
			continue
		}
		text := shapeOf(entry.Items[3].Items)
		if totals[text] == nil {
			totals[text] = &dtos.Statement{Text: text}
		}
		totals[text].Calls++
		micros[text] += entry.Items[2].Integer
	}
	statements := make([]dtos.Statement, 0, len(totals))
	for text, statement := range totals {
		statement.TotalMs = micros[text] / 1000
		statement.MeanMs = micros[text] / statement.Calls / 1000
		statements = append(statements, *statement)
	}
	slices.SortFunc(statements, func(a, b dtos.Statement) int {
		return cmp.Or(cmp.Compare(micros[b.Text], micros[a.Text]), cmp.Compare(a.Text, b.Text))
	})

	return statements[:min(len(statements), topStatements)]
}

func shapeOf(args []Reply) string {
	words := []string{strings.ToUpper(args[0].Text)}
	for range args[1:] {
		words = append(words, "?")
	}

	return strings.Join(words, " ")
}

func fromWrapper(entry Reply) bool {
	command := strings.ToUpper(entry.Items[3].Items[0].Text)

	return command == "AUTH" || command == "HELLO" || len(entry.Items) > 5 && entry.Items[5].Text == clientName
}
