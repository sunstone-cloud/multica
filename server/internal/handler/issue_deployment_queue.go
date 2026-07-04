package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/multica-ai/multica/server/internal/scheduler"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func (h *Handler) maybeRecoverDeploymentQueueForBlocker(ctx context.Context, blocker db.Issue, source string) {
	if h.TxStarter == nil || h.IssueService == nil || h.TaskService == nil {
		return
	}
	if _, err := scheduler.RecoverDeploymentQueueForBlocker(ctx, h.TxStarter, h.Queries, h.TaskService, blocker); err != nil {
		slog.Warn("deployment queue event recovery failed",
			"issue_id", uuidToString(blocker.ID),
			"issue_number", blocker.Number,
			"status", blocker.Status,
			"source", source,
			"error", err)
	}
}

func isDeploymentCompletionMetadataKey(key string) bool {
	switch key {
	case "deployment_status",
		"testflight_status",
		"testflight_state",
		"app_store_connect_status":
		return true
	default:
		return false
	}
}

func isDeploymentTerminalMetadataValue(raw json.RawMessage) bool {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "distributed",
		"completed",
		"complete",
		"done",
		"success",
		"succeeded",
		"uploaded",
		"ready_for_testing",
		"failed",
		"failure",
		"cancelled",
		"canceled",
		"error",
		"rejected":
		return true
	default:
		return false
	}
}
