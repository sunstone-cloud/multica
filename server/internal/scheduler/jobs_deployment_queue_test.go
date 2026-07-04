package scheduler

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestRecoverDeploymentQueueStaleBlockerRestoresOnlyQueueHead(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	queries := db.New(pool)
	wsID := seedDeploymentQueueWorkspace(t, pool)
	t.Cleanup(func() { cleanupDeploymentQueueWorkspace(t, pool, wsID) })

	seedDeploymentQueueIssue(t, pool, deploymentQueueIssue{
		WorkspaceID: wsID,
		Number:      933,
		Title:       "ZHI-933 completed TestFlight",
		Status:      "done",
		Position:    -10,
		Metadata: map[string]string{
			"deployment_env":           "ios-testflight",
			"deployment_queue_status":  "completed",
			"testflight_status":        "distributed",
			"blocked_by_deployment":    "",
			"deployment_release_track": "test",
		},
	})
	waitingHead := seedDeploymentQueueIssue(t, pool, deploymentQueueIssue{
		WorkspaceID: wsID,
		Number:      949,
		Title:       "ZHI-949 waiting on stale blocker",
		Status:      "todo",
		Position:    -9,
		Metadata: map[string]string{
			"deployment_env":          "ios-testflight",
			"deployment_queue_status": "waiting",
			"blocked_by_deployment":   "ZHI-933",
		},
	})
	waitingTail := seedDeploymentQueueIssue(t, pool, deploymentQueueIssue{
		WorkspaceID: wsID,
		Number:      964,
		Title:       "ZHI-964 still behind queue head",
		Status:      "todo",
		Position:    -8,
		Metadata: map[string]string{
			"deployment_env":          "ios-testflight",
			"deployment_queue_status": "waiting",
			"blocked_by_deployment":   "ZHI-949",
		},
	})

	got, err := RecoverDeploymentQueueStaleBlockers(ctx, pool, queries, nil)
	if err != nil {
		t.Fatalf("RecoverDeploymentQueueStaleBlockers: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one recovered queue head, got %d: %+v", len(got), got)
	}
	if got[0].IssueNumber != 949 || got[0].QueueStatus != deploymentQueueStatusReady {
		t.Fatalf("unexpected recovery result: %+v", got[0])
	}

	headMD := readIssueMetadata(t, pool, waitingHead)
	if _, ok := headMD["blocked_by_deployment"]; ok {
		t.Fatalf("head still has blocked_by_deployment: %v", headMD)
	}
	if headMD["deployment_queue_status"] != deploymentQueueStatusReady {
		t.Fatalf("head queue status = %v, want ready; metadata=%v", headMD["deployment_queue_status"], headMD)
	}

	tailMD := readIssueMetadata(t, pool, waitingTail)
	if tailMD["blocked_by_deployment"] != "ZHI-949" || tailMD["deployment_queue_status"] != "waiting" {
		t.Fatalf("tail waiting item should remain untouched, metadata=%v", tailMD)
	}

	comment := latestIssueComment(t, pool, waitingHead)
	if !strings.Contains(comment, "Deployment queue watchdog recovered a stale blocker") ||
		!strings.Contains(comment, "Blocked by: ZHI-933") {
		t.Fatalf("audit comment missing recovery details: %q", comment)
	}
}

func TestRecoverDeploymentQueueActiveBlockerDoesNotClear(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	queries := db.New(pool)
	wsID := seedDeploymentQueueWorkspace(t, pool)
	t.Cleanup(func() { cleanupDeploymentQueueWorkspace(t, pool, wsID) })

	seedDeploymentQueueIssue(t, pool, deploymentQueueIssue{
		WorkspaceID: wsID,
		Number:      933,
		Title:       "ZHI-933 active TestFlight",
		Status:      "in_progress",
		Position:    -10,
		Metadata: map[string]string{
			"deployment_env":          "ios-testflight",
			"deployment_queue_status": "ready",
			"deployment_status":       "uploading",
		},
	})
	waiting := seedDeploymentQueueIssue(t, pool, deploymentQueueIssue{
		WorkspaceID: wsID,
		Number:      949,
		Title:       "ZHI-949 waiting on active blocker",
		Status:      "todo",
		Position:    -9,
		Metadata: map[string]string{
			"deployment_env":          "ios-testflight",
			"deployment_queue_status": "waiting",
			"blocked_by_deployment":   "ZHI-933",
		},
	})

	got, err := RecoverDeploymentQueueStaleBlockers(ctx, pool, queries, nil)
	if err != nil {
		t.Fatalf("RecoverDeploymentQueueStaleBlockers: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("active blocker must not recover, got %+v", got)
	}
	md := readIssueMetadata(t, pool, waiting)
	if md["blocked_by_deployment"] != "ZHI-933" || md["deployment_queue_status"] != "waiting" {
		t.Fatalf("waiting issue changed despite active blocker: %v", md)
	}
	if n := countIssueComments(t, pool, waiting); n != 0 {
		t.Fatalf("active blocker should not create audit comment, got %d", n)
	}
}

func TestRecoverDeploymentQueueSkipsWhenSameEnvironmentActive(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	queries := db.New(pool)
	wsID := seedDeploymentQueueWorkspace(t, pool)
	t.Cleanup(func() { cleanupDeploymentQueueWorkspace(t, pool, wsID) })

	seedDeploymentQueueIssue(t, pool, deploymentQueueIssue{
		WorkspaceID: wsID,
		Number:      933,
		Title:       "ZHI-933 completed TestFlight",
		Status:      "done",
		Position:    -10,
		Metadata: map[string]string{
			"deployment_env":          "ios-testflight",
			"deployment_queue_status": "completed",
			"testflight_status":       "distributed",
		},
	})
	waiting := seedDeploymentQueueIssue(t, pool, deploymentQueueIssue{
		WorkspaceID: wsID,
		Number:      949,
		Title:       "ZHI-949 waiting on stale blocker",
		Status:      "todo",
		Position:    -9,
		Metadata: map[string]string{
			"deployment_env":          "ios-testflight",
			"deployment_queue_status": "waiting",
			"blocked_by_deployment":   "ZHI-933",
		},
	})
	seedDeploymentQueueIssue(t, pool, deploymentQueueIssue{
		WorkspaceID: wsID,
		Number:      950,
		Title:       "ZHI-950 currently running TestFlight",
		Status:      "in_progress",
		Position:    -7,
		Metadata: map[string]string{
			"deployment_env":          "ios-testflight",
			"deployment_queue_status": "dispatched",
		},
	})

	got, err := RecoverDeploymentQueueStaleBlockers(ctx, pool, queries, nil)
	if err != nil {
		t.Fatalf("RecoverDeploymentQueueStaleBlockers: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("same-env active deployment must keep waiting head parked, got %+v", got)
	}
	md := readIssueMetadata(t, pool, waiting)
	if md["blocked_by_deployment"] != "ZHI-933" || md["deployment_queue_status"] != "waiting" {
		t.Fatalf("waiting issue changed despite active deployment: %v", md)
	}
}

type deploymentQueueIssue struct {
	WorkspaceID string
	Number      int
	Title       string
	Status      string
	Position    float64
	Metadata    map[string]string
}

func seedDeploymentQueueWorkspace(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	slug := "deployment-queue-" + uniqueSuffix()
	var id string
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO workspace (name, slug)
		VALUES ($1, $1)
		RETURNING id
	`, slug).Scan(&id); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	return id
}

func cleanupDeploymentQueueWorkspace(t *testing.T, pool *pgxpool.Pool, wsID string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `DELETE FROM workspace WHERE id = $1`, wsID); err != nil {
		t.Logf("cleanup workspace: %v", err)
	}
}

func seedDeploymentQueueIssue(t *testing.T, pool *pgxpool.Pool, in deploymentQueueIssue) string {
	t.Helper()
	md := make(map[string]string, len(in.Metadata))
	for k, v := range in.Metadata {
		if v != "" {
			md[k] = v
		}
	}
	rawMD, err := json.Marshal(md)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	var id string
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO issue (
			workspace_id, title, description, status, priority,
			creator_type, creator_id, position, number, metadata
		)
		VALUES (
			$1, $2, '', $3, 'high',
			'agent', gen_random_uuid(), $4, $5, $6::jsonb
		)
		RETURNING id
	`, in.WorkspaceID, in.Title, in.Status, in.Position, in.Number, string(rawMD)).Scan(&id); err != nil {
		t.Fatalf("seed issue %s: %v", in.Title, err)
	}
	return id
}

func readIssueMetadata(t *testing.T, pool *pgxpool.Pool, issueID string) map[string]any {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(context.Background(), `
		SELECT metadata FROM issue WHERE id = $1
	`, issueID).Scan(&raw); err != nil {
		t.Fatalf("read issue metadata: %v", err)
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal metadata %s: %v", string(raw), err)
	}
	return out
}

func latestIssueComment(t *testing.T, pool *pgxpool.Pool, issueID string) string {
	t.Helper()
	var content string
	if err := pool.QueryRow(context.Background(), `
		SELECT content
		  FROM comment
		 WHERE issue_id = $1
		 ORDER BY created_at DESC
		 LIMIT 1
	`, issueID).Scan(&content); err != nil {
		t.Fatalf("read latest comment: %v", err)
	}
	return content
}

func countIssueComments(t *testing.T, pool *pgxpool.Pool, issueID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM comment WHERE issue_id = $1
	`, issueID).Scan(&n); err != nil {
		t.Fatalf("count comments: %v", err)
	}
	return n
}

func TestDeploymentQueueRecoveryCommentForFailedTerminal(t *testing.T) {
	body := deploymentQueueRecoveryComment(DeploymentQueueRecoveryResult{
		Environment:   "ios-testflight",
		BlockerRef:    "ZHI-933",
		BlockerStatus: "cancelled",
		QueueStatus:   deploymentQueueStatusNeedsDecision,
		Reason:        "blocker reached a failure terminal state",
	})
	if !strings.Contains(body, "retry, skip, or escalate") {
		t.Fatalf("failed terminal comment must route decision back to Athena/owner path: %q", body)
	}
}
