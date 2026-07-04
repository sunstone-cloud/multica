package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const (
	// JobNameDeploymentQueueRecovery is stable because it keys sys_cron_executions.
	JobNameDeploymentQueueRecovery = "deployment_queue_recovery"

	deploymentQueueStatusReady         = "ready"
	deploymentQueueStatusNeedsDecision = "needs_decision"
)

type deploymentQueueRecoveryWakeService interface {
	EnqueueTaskForIssueWithHandoff(context.Context, db.Issue, string) (db.AgentTaskQueue, error)
	EnqueueTaskForSquadLeaderWithHandoff(context.Context, db.Issue, pgtype.UUID, pgtype.UUID, string) (db.AgentTaskQueue, error)
}

// DeploymentQueueRecoveryJob detects waiting deployment queue heads whose
// blocked_by_deployment target has already reached a terminal outcome. It only
// recovers one waiting head per workspace/environment and skips environments
// with an active deployment so TestFlight-style queues stay serial.
func DeploymentQueueRecoveryJob(
	pool *pgxpool.Pool,
	queries *db.Queries,
	taskSvc *service.TaskService,
) JobSpec {
	return JobSpec{
		Name:              JobNameDeploymentQueueRecovery,
		Cadence:           2 * time.Minute,
		ScheduleDelay:     0,
		CatchUpMode:       CatchUpLatestOnly,
		CatchUpWindow:     time.Hour,
		RunTimeout:        time.Minute,
		StaleTimeout:      3 * time.Minute,
		HeartbeatInterval: 30 * time.Second,
		AllowStaleReentry: true,
		MaxAttempts:       3,
		RetryBackoff: []time.Duration{
			30 * time.Second,
			time.Minute,
			5 * time.Minute,
		},
		Scopes:  StaticScopes(ScopeGlobal),
		Handler: deploymentQueueRecoveryHandler(pool, queries, taskSvc),
	}
}

func deploymentQueueRecoveryHandler(
	pool *pgxpool.Pool,
	queries *db.Queries,
	waker deploymentQueueRecoveryWakeService,
) Handler {
	return func(ctx context.Context, in HandlerInput) (HandlerResult, error) {
		recovered, err := RecoverDeploymentQueueStaleBlockers(ctx, pool, queries, waker)
		if err != nil {
			return HandlerResult{}, err
		}
		if in.Heartbeat != nil {
			_ = in.Heartbeat(ctx)
		}
		return HandlerResult{
			RowsAffected: int64(len(recovered)),
			Result: map[string]any{
				"recovered": len(recovered),
			},
		}, nil
	}
}

type DeploymentQueueRecoveryResult struct {
	IssueID       pgtype.UUID
	IssueNumber   int32
	Environment   string
	BlockerRef    string
	BlockerID     pgtype.UUID
	BlockerNumber int32
	BlockerStatus string
	QueueStatus   string
	Reason        string
}

type deploymentQueueCandidate struct {
	IssueID            pgtype.UUID
	WorkspaceID        pgtype.UUID
	IssueNumber        int32
	Environment        string
	BlockerRef         string
	BlockerID          pgtype.UUID
	BlockerNumber      pgtype.Int4
	BlockerStatus      pgtype.Text
	BlockerMetadata    []byte
	BlockerHasDoneMark bool
	BlockerHasFailMark bool
}

// RecoverDeploymentQueueStaleBlockers performs one recovery pass. It commits
// metadata/comment changes before enqueueing Athena/queue-owner work, so a task
// can never observe a rolled-back recovery.
func RecoverDeploymentQueueStaleBlockers(
	ctx context.Context,
	pool *pgxpool.Pool,
	queries *db.Queries,
	waker deploymentQueueRecoveryWakeService,
) ([]DeploymentQueueRecoveryResult, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("deployment queue recovery: begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	candidates, err := listRecoverableDeploymentQueueHeads(ctx, tx)
	if err != nil {
		return nil, err
	}

	txQueries := queries.WithTx(tx)
	results := make([]DeploymentQueueRecoveryResult, 0, len(candidates))
	issueIDs := make([]pgtype.UUID, 0, len(candidates))

	for _, c := range candidates {
		outcome := classifyDeploymentBlocker(c)
		if outcome == blockerActive {
			continue
		}

		queueStatus := deploymentQueueStatusReady
		reason := "blocker completed"
		if outcome == blockerFailed {
			queueStatus = deploymentQueueStatusNeedsDecision
			reason = "blocker reached a failure terminal state"
		}
		if outcome == blockerMissing {
			queueStatus = deploymentQueueStatusNeedsDecision
			reason = "blocker issue could not be found"
		}

		if _, err := tx.Exec(ctx, `
			UPDATE issue
			   SET metadata = (metadata - 'blocked_by_deployment')
			              || jsonb_build_object('deployment_queue_status', $2::text),
			       updated_at = now()
			 WHERE id = $1
		`, c.IssueID, queueStatus); err != nil {
			return nil, fmt.Errorf("deployment queue recovery: update issue %s: %w", util.UUIDToString(c.IssueID), err)
		}

		result := DeploymentQueueRecoveryResult{
			IssueID:       c.IssueID,
			IssueNumber:   c.IssueNumber,
			Environment:   c.Environment,
			BlockerRef:    c.BlockerRef,
			BlockerID:     c.BlockerID,
			BlockerStatus: blockerStatusLabel(c),
			QueueStatus:   queueStatus,
			Reason:        reason,
		}
		if c.BlockerNumber.Valid {
			result.BlockerNumber = c.BlockerNumber.Int32
		}

		if _, err := txQueries.CreateComment(ctx, db.CreateCommentParams{
			IssueID:     c.IssueID,
			WorkspaceID: c.WorkspaceID,
			AuthorType:  "system",
			AuthorID:    pgtype.UUID{Valid: true},
			Type:        "system",
			Content:     deploymentQueueRecoveryComment(result),
		}); err != nil {
			return nil, fmt.Errorf("deployment queue recovery: comment on issue %s: %w", util.UUIDToString(c.IssueID), err)
		}

		results = append(results, result)
		issueIDs = append(issueIDs, c.IssueID)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("deployment queue recovery: commit: %w", err)
	}

	if waker != nil {
		for i, issueID := range issueIDs {
			issue, err := queries.GetIssue(ctx, issueID)
			if err != nil {
				return results, fmt.Errorf("deployment queue recovery: reload issue %s: %w", util.UUIDToString(issueID), err)
			}
			if err := wakeDeploymentQueueOwner(ctx, queries, waker, issue, results[i]); err != nil {
				slog.Warn("deployment queue recovery: wake owner failed",
					"issue_id", util.UUIDToString(issueID),
					"error", err)
			}
		}
	}

	return results, nil
}

func listRecoverableDeploymentQueueHeads(ctx context.Context, tx pgx.Tx) ([]deploymentQueueCandidate, error) {
	rows, err := tx.Query(ctx, `
		WITH ranked AS (
			SELECT
				i.id,
				i.workspace_id,
				i.number,
				i.position,
				i.created_at,
				i.metadata->>'deployment_env' AS env,
				i.metadata->>'blocked_by_deployment' AS blocker_ref,
				row_number() OVER (
					PARTITION BY i.workspace_id, i.metadata->>'deployment_env'
					ORDER BY i.position ASC, i.created_at DESC
				) AS rn
			FROM issue i
			WHERE i.status NOT IN ('done', 'cancelled')
			  AND i.metadata->>'deployment_env' IS NOT NULL
			  AND i.metadata->>'blocked_by_deployment' IS NOT NULL
			  AND COALESCE(i.metadata->>'deployment_queue_status', '') IN (
				'waiting', 'blocked', 'blocked_by_deployment'
			  )
		),
		heads AS (
			SELECT * FROM ranked WHERE rn = 1
		)
		SELECT
			i.id,
			i.workspace_id,
			i.number,
			h.env,
			h.blocker_ref,
			b.id,
			b.number,
			b.status,
			b.metadata,
			EXISTS (
				SELECT 1 FROM comment c
				WHERE c.issue_id = b.id
				  AND c.content LIKE '%[STAGE:DEPLOY_DONE]%'
			) AS blocker_has_done_mark,
			EXISTS (
				SELECT 1 FROM comment c
				WHERE c.issue_id = b.id
				  AND c.content LIKE '%[STAGE:DEPLOY_FAILED]%'
			) AS blocker_has_fail_mark
		FROM heads h
		JOIN issue i ON i.id = h.id
		LEFT JOIN issue b
		  ON b.workspace_id = i.workspace_id
		 AND (
			b.id::text = h.blocker_ref
			OR b.number::text = h.blocker_ref
			OR b.number::text = substring(h.blocker_ref FROM '([0-9]+)$')
		 )
		WHERE NOT EXISTS (
			SELECT 1
			  FROM issue active
			 WHERE active.workspace_id = i.workspace_id
			   AND active.id <> i.id
			   AND active.status NOT IN ('done', 'cancelled')
			   AND active.metadata->>'deployment_env' = h.env
			   AND COALESCE(active.metadata->>'deployment_queue_status', '') IN (
				'active', 'deploying', 'dispatched', 'in_progress', 'running'
			   )
		)
		ORDER BY i.workspace_id, h.env
		FOR UPDATE OF i SKIP LOCKED
	`)
	if err != nil {
		return nil, fmt.Errorf("deployment queue recovery: list heads: %w", err)
	}
	defer rows.Close()

	var out []deploymentQueueCandidate
	for rows.Next() {
		var c deploymentQueueCandidate
		if err := rows.Scan(
			&c.IssueID,
			&c.WorkspaceID,
			&c.IssueNumber,
			&c.Environment,
			&c.BlockerRef,
			&c.BlockerID,
			&c.BlockerNumber,
			&c.BlockerStatus,
			&c.BlockerMetadata,
			&c.BlockerHasDoneMark,
			&c.BlockerHasFailMark,
		); err != nil {
			return nil, fmt.Errorf("deployment queue recovery: scan head: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("deployment queue recovery: scan heads: %w", err)
	}
	return out, nil
}

type blockerOutcome int

const (
	blockerActive blockerOutcome = iota
	blockerCompleted
	blockerFailed
	blockerMissing
)

func classifyDeploymentBlocker(c deploymentQueueCandidate) blockerOutcome {
	if !c.BlockerID.Valid {
		return blockerMissing
	}
	if c.BlockerHasFailMark {
		return blockerFailed
	}
	if c.BlockerHasDoneMark {
		return blockerCompleted
	}
	if c.BlockerStatus.Valid {
		switch c.BlockerStatus.String {
		case "done":
			return blockerCompleted
		case "cancelled":
			return blockerFailed
		}
	}

	md := map[string]any{}
	_ = json.Unmarshal(c.BlockerMetadata, &md)
	if metadataHasAnyStatus(md, []string{
		"deployment_status",
		"testflight_status",
		"testflight_state",
		"app_store_connect_status",
	}, "distributed", "completed", "complete", "done", "success", "succeeded", "uploaded", "ready_for_testing") {
		return blockerCompleted
	}
	if metadataHasAnyStatus(md, []string{
		"deployment_status",
		"testflight_status",
		"testflight_state",
		"app_store_connect_status",
	}, "failed", "failure", "cancelled", "canceled", "error", "rejected") {
		return blockerFailed
	}
	return blockerActive
}

func metadataHasAnyStatus(md map[string]any, keys []string, statuses ...string) bool {
	want := make(map[string]struct{}, len(statuses))
	for _, s := range statuses {
		want[s] = struct{}{}
	}
	for _, key := range keys {
		v, ok := md[key]
		if !ok {
			continue
		}
		if _, ok := want[strings.ToLower(strings.TrimSpace(fmt.Sprint(v)))]; ok {
			return true
		}
	}
	return false
}

func blockerStatusLabel(c deploymentQueueCandidate) string {
	if !c.BlockerID.Valid {
		return "missing"
	}
	if c.BlockerStatus.Valid && c.BlockerStatus.String != "" {
		return c.BlockerStatus.String
	}
	return "unknown"
}

func deploymentQueueRecoveryComment(r DeploymentQueueRecoveryResult) string {
	next := "Athena Lite should re-evaluate the queue head."
	if r.QueueStatus == deploymentQueueStatusNeedsDecision {
		next = "Athena Lite should decide whether to retry, skip, or escalate to the owner."
	}

	return fmt.Sprintf(
		"Deployment queue watchdog recovered a stale blocker.\n\nEnvironment: %s\nBlocked by: %s\nBlocker status: %s\nReason: %s\nQueue status: %s\nNext: %s",
		r.Environment,
		r.BlockerRef,
		r.BlockerStatus,
		r.Reason,
		r.QueueStatus,
		next,
	)
}

func wakeDeploymentQueueOwner(
	ctx context.Context,
	queries *db.Queries,
	waker deploymentQueueRecoveryWakeService,
	issue db.Issue,
	result DeploymentQueueRecoveryResult,
) error {
	handoff := fmt.Sprintf(
		"Deployment queue watchdog recovered stale blocker %s for %s; queue_status=%s. Re-evaluate the queue head before dispatching deployment.",
		result.BlockerRef,
		result.Environment,
		result.QueueStatus,
	)

	switch issue.AssigneeType.String {
	case "agent":
		_, err := waker.EnqueueTaskForIssueWithHandoff(ctx, issue, handoff)
		return err
	case "squad":
		squad, err := queries.GetSquad(ctx, issue.AssigneeID)
		if err != nil {
			return fmt.Errorf("load squad: %w", err)
		}
		_, err = waker.EnqueueTaskForSquadLeaderWithHandoff(ctx, issue, squad.LeaderID, squad.ID, handoff)
		return err
	default:
		return nil
	}
}
