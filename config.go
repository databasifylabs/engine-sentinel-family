package sentinel

import (
	"context"
	"strings"

	"github.com/databasifylabs/engine-sdk/contract"
	"github.com/databasifylabs/engine-sdk/dtos"
	"github.com/databasifylabs/engine-sdk/requests"
	"github.com/databasifylabs/engine-sdk/responses"
	"github.com/databasifylabs/engine-sdk/settings"
)

const tunedState = "state/tuned.json"

type config struct{ Engine }

func (e Engine) Config() contract.Config { return config{e} }

func (e Engine) effective(request requests.NodeConfigure) (requests.NodeConfigure, error) {
	tuned, err := e.catalog().Tuned(e.path(tunedState), request.Self())
	if err != nil {
		return request, err
	}
	request.Settings = settings.Effective(tuned, request.Settings)

	return request, nil
}

func (e Engine) restartOnly() []string {
	keys := []string{}
	for _, field := range e.catalog().Fields {
		if field.Restart {
			keys = append(keys, field.Key)
		}
	}

	return keys
}

func (e config) Fields(context.Context) (responses.ConfigFields, error) {
	request, err := e.saved()
	if err != nil {
		return responses.ConfigFields{Fields: e.catalog().Fields}, nil
	}
	tuned, err := e.catalog().Tuned(e.path(tunedState), request.Self())

	return responses.ConfigFields{Fields: e.catalog().FieldsWith(tuned)}, err
}

func (e config) Show(ctx context.Context) (responses.ConfigShow, error) {
	request, err := e.saved()
	if err != nil {
		return responses.ConfigShow{}, err
	}
	tuned, err := e.catalog().Tuned(e.path(tunedState), request.Self())
	if err != nil {
		return responses.ConfigShow{}, err
	}
	effective := settings.Effective(tuned, request.Settings)
	values := []dtos.ConfigValue{}
	for _, field := range e.catalog().Fields {
		value, err := e.configGet(ctx, configKey(field.Key))
		if err != nil {
			return responses.ConfigShow{}, err
		}
		wanted, set := effective[field.Key]
		values = append(values, dtos.ConfigValue{
			Key: field.Key, Value: value, Source: e.catalog().SourceOf(field.Key, tuned, request.Settings),
			PendingRestart: field.Restart && set && !strings.EqualFold(value, wanted),
		})
	}

	return responses.ConfigShow{Values: values}, nil
}

func (e config) Tune(ctx context.Context) (responses.ConfigTune, error) {
	request, err := e.saved()
	if err != nil {
		return responses.ConfigTune{}, err
	}
	previous, err := e.catalog().Tuned(e.path(tunedState), request.Self())
	if err != nil {
		return responses.ConfigTune{}, err
	}
	tuned, err := e.catalog().Retune(e.path(tunedState), request.Self())
	if err != nil {
		return responses.ConfigTune{}, err
	}
	if _, err := e.Nodes().Configure(ctx, request); err != nil {
		return responses.ConfigTune{}, err
	}
	if err := e.Nodes().ApplyConfig(ctx); err != nil {
		return responses.ConfigTune{}, err
	}

	return responses.ConfigTune{Tuned: tuned, Restart: e.catalog().RestartChanged(previous, tuned, request.Settings)}, nil
}
