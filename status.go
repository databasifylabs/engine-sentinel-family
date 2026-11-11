package sentinel

import (
	"context"
	"errors"

	"github.com/databasifylabs/engine-sdk/command"
	"github.com/databasifylabs/engine-sdk/enums"
	"github.com/databasifylabs/engine-sdk/responses"
)

func (e nodes) Status(ctx context.Context) (responses.NodeStatus, error) {
	replication, err := e.info(ctx, "replication")
	if isUnavailable(err) {
		return responses.NodeStatus{Role: enums.NodeRoleOffline}, nil
	}
	if err != nil {
		return responses.NodeStatus{}, err
	}
	server, err := e.info(ctx, "server")
	if err != nil {
		return responses.NodeStatus{}, err
	}
	status := responses.NodeStatus{Role: roleOf(replication)}
	if version, found := infoField(server, e.flavor.VersionField); found {
		status.Version = &version
	}

	return status, nil
}

func roleOf(replication string) enums.NodeRole {
	role, _ := infoField(replication, "role")
	if role == "master" {
		return enums.NodeRolePrimary
	}
	link, _ := infoField(replication, "master_link_status")
	syncing, _ := infoField(replication, "master_sync_in_progress")
	if role == "slave" && link == "up" && syncing == "0" {
		return enums.NodeRoleReplica
	}

	return enums.NodeRoleUnknown
}

func (e Engine) info(ctx context.Context, section string) (string, error) {
	reply, err := e.server.Do(ctx, localhost, dataPort, "INFO", section)

	return reply.Text, err
}

func isUnavailable(err error) bool {
	var failure command.Failure

	return errors.As(err, &failure) && failure.Code == enums.ErrorCodeUnavailable
}
