package service

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const (
	defaultHelperDeliveryWorkspaceID = "84b0bc45-d036-418d-bf56-88988bf61919"
	defaultHelperDeliveryProjectID   = "699f2b0f-15d2-4950-a862-6780189edd5d"
	defaultHelperDeliverySquadID     = "ea09b29a-443d-4065-8a49-7dd7a95fc44f"
	defaultHelperDeliveryAgentIDs    = "9c02ec40-f1c4-44fb-af17-55b90b56bff9"
)

type helperDeliveryIntakeConfig struct {
	WorkspaceID pgtype.UUID
	ProjectID   pgtype.UUID
	SquadID     pgtype.UUID
	HelperIDs   map[string]struct{}
}

func (s *IssueService) maybeApplyHelperDeliveryIntake(ctx context.Context, q *db.Queries, issue db.Issue, p IssueCreateParams) (db.Issue, error) {
	cfg, ok := helperDeliveryIntakeConfigFromEnv()
	if !ok || !helperDeliveryIntakeMatches(issue, p, cfg) {
		return issue, nil
	}
	if _, err := q.GetProjectInWorkspace(ctx, db.GetProjectInWorkspaceParams{ID: cfg.ProjectID, WorkspaceID: cfg.WorkspaceID}); err != nil {
		slog.Warn("helper delivery intake skipped: project is missing", "workspace_id", util.UUIDToString(cfg.WorkspaceID), "project_id", util.UUIDToString(cfg.ProjectID), "error", err)
		return issue, nil
	}
	if _, err := q.GetSquadInWorkspace(ctx, db.GetSquadInWorkspaceParams{ID: cfg.SquadID, WorkspaceID: cfg.WorkspaceID}); err != nil {
		slog.Warn("helper delivery intake skipped: squad is missing", "workspace_id", util.UUIDToString(cfg.WorkspaceID), "squad_id", util.UUIDToString(cfg.SquadID), "error", err)
		return issue, nil
	}

	updated, err := q.UpdateIssue(ctx, db.UpdateIssueParams{
		ID:            issue.ID,
		Title:         pgtype.Text{String: issue.Title, Valid: true},
		Description:   issue.Description,
		Status:        pgtype.Text{String: issue.Status, Valid: true},
		Priority:      pgtype.Text{String: issue.Priority, Valid: true},
		AssigneeType:  pgtype.Text{String: "squad", Valid: true},
		AssigneeID:    cfg.SquadID,
		Position:      pgtype.Float8{Float64: issue.Position, Valid: true},
		StartDate:     issue.StartDate,
		DueDate:       issue.DueDate,
		ParentIssueID: issue.ParentIssueID,
		ProjectID:     cfg.ProjectID,
	})
	if err != nil {
		return issue, fmt.Errorf("helper delivery intake update issue: %w", err)
	}

	metadata := map[string]string{
		"workflow":        "delivery",
		"pipeline_mode":   "onestep",
		"pipeline_status": "intake",
		"current_stage":   "intake",
		"intake_source":   "helper_delivery_create_hook",
		"stage_summary":   "Helper-created delivery issue auto-routed to OneStep intake.",
		"next_action":     "OneStep intake squad should classify and dispatch the delivery pipeline.",
	}
	for key, value := range metadata {
		updated, err = q.SetIssueMetadataKey(ctx, db.SetIssueMetadataKeyParams{
			Key:         key,
			Value:       []byte(strconv.Quote(value)),
			ID:          updated.ID,
			WorkspaceID: updated.WorkspaceID,
		})
		if err != nil {
			return issue, fmt.Errorf("helper delivery intake set metadata %s: %w", key, err)
		}
	}

	content := fmt.Sprintf("[@OneStep 开发 Squad](mention://squad/%s) Helper-created delivery issue detected; routed to OneStep intake automatically.", util.UUIDToString(cfg.SquadID))
	if _, err := q.CreateComment(ctx, db.CreateCommentParams{
		IssueID:     updated.ID,
		WorkspaceID: updated.WorkspaceID,
		AuthorType:  "agent",
		AuthorID:    p.CreatorID,
		Content:     content,
		Type:        "comment",
	}); err != nil {
		return issue, fmt.Errorf("helper delivery intake comment: %w", err)
	}

	return updated, nil
}

func helperDeliveryIntakeMatches(issue db.Issue, p IssueCreateParams, cfg helperDeliveryIntakeConfig) bool {
	if !sameUUID(issue.WorkspaceID, cfg.WorkspaceID) || !sameUUID(p.WorkspaceID, cfg.WorkspaceID) {
		return false
	}
	if p.CreatorType != "agent" {
		return false
	}
	if _, ok := cfg.HelperIDs[util.UUIDToString(p.CreatorID)]; !ok {
		return false
	}
	if p.ProjectID.Valid || p.AssigneeID.Valid || p.AssigneeType.Valid {
		return false
	}
	if p.ParentIssueID.Valid || issue.ParentIssueID.Valid {
		return false
	}
	if issue.Status == "backlog" || issue.Status == "done" || issue.Status == "cancelled" {
		return false
	}
	return helperDeliveryLooksLikeDelivery(issue.Title, issue.Description.String)
}

func helperDeliveryLooksLikeDelivery(title, description string) bool {
	text := strings.ToLower(title + "\n" + description)
	signals := []string{
		"修复", "实现", "开发", "改造", "优化", "上线", "发布", "部署", "适配", "接入", "补齐", "验收标准", "期望修复",
		"bug", "fix", "implement", "build", "deliver", "release", "ship", "deploy", "acceptance criteria",
	}
	for _, signal := range signals {
		if strings.Contains(text, strings.ToLower(signal)) {
			return true
		}
	}
	return false
}

func helperDeliveryIntakeConfigFromEnv() (helperDeliveryIntakeConfig, bool) {
	workspaceID, ok := parseHelperDeliveryUUID(envOrDefault("MULTICA_HELPER_DELIVERY_INTAKE_WORKSPACE_ID", defaultHelperDeliveryWorkspaceID))
	if !ok {
		return helperDeliveryIntakeConfig{}, false
	}
	projectID, ok := parseHelperDeliveryUUID(envOrDefault("MULTICA_HELPER_DELIVERY_INTAKE_PROJECT_ID", defaultHelperDeliveryProjectID))
	if !ok {
		return helperDeliveryIntakeConfig{}, false
	}
	squadID, ok := parseHelperDeliveryUUID(envOrDefault("MULTICA_HELPER_DELIVERY_INTAKE_SQUAD_ID", defaultHelperDeliverySquadID))
	if !ok {
		return helperDeliveryIntakeConfig{}, false
	}
	helperIDs := map[string]struct{}{}
	for _, raw := range strings.Split(envOrDefault("MULTICA_HELPER_DELIVERY_INTAKE_HELPER_AGENT_IDS", defaultHelperDeliveryAgentIDs), ",") {
		id, ok := parseHelperDeliveryUUID(strings.TrimSpace(raw))
		if !ok {
			continue
		}
		helperIDs[util.UUIDToString(id)] = struct{}{}
	}
	if len(helperIDs) == 0 {
		return helperDeliveryIntakeConfig{}, false
	}
	return helperDeliveryIntakeConfig{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		SquadID:     squadID,
		HelperIDs:   helperIDs,
	}, true
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func parseHelperDeliveryUUID(value string) (pgtype.UUID, bool) {
	id, err := util.ParseUUID(value)
	if err != nil {
		return pgtype.UUID{}, false
	}
	return id, true
}

func sameUUID(a, b pgtype.UUID) bool {
	return a.Valid && b.Valid && util.UUIDToString(a) == util.UUIDToString(b)
}
