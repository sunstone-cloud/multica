package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCreateIssueHelperDeliveryAutoRoutesToOneStepIntake(t *testing.T) {
	ctx := context.Background()

	helperID := createHandlerTestAgent(t, "Helper Delivery Intake", []byte("[]"))
	helperTaskID := createHandlerTestTaskForAgent(t, helperID)

	var leaderID string
	if err := testPool.QueryRow(ctx, `
		SELECT id FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1
	`, testWorkspaceID).Scan(&leaderID); err != nil {
		t.Fatalf("load squad leader: %v", err)
	}

	var projectID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO project (workspace_id, title, status, priority)
		VALUES ($1, $2, 'active', 'medium')
		RETURNING id
	`, testWorkspaceID, "OneStep Intake Test").Scan(&projectID); err != nil {
		t.Fatalf("create project: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM project WHERE id = $1`, projectID)
	})

	var squadID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO squad (workspace_id, name, description, leader_id, creator_id)
		VALUES ($1, $2, '', $3, $4)
		RETURNING id
	`, testWorkspaceID, "OneStep Delivery Intake Squad", leaderID, testUserID).Scan(&squadID); err != nil {
		t.Fatalf("create squad: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM squad WHERE id = $1`, squadID)
	})

	t.Setenv("MULTICA_HELPER_DELIVERY_INTAKE_WORKSPACE_ID", testWorkspaceID)
	t.Setenv("MULTICA_HELPER_DELIVERY_INTAKE_PROJECT_ID", projectID)
	t.Setenv("MULTICA_HELPER_DELIVERY_INTAKE_SQUAD_ID", squadID)
	t.Setenv("MULTICA_HELPER_DELIVERY_INTAKE_HELPER_AGENT_IDS", helperID)

	title := fmt.Sprintf("修复 timeline 图片高清预览缩放与历史多图缩略图兼容 %d", time.Now().UnixNano())
	w := httptest.NewRecorder()
	req := newRequest("POST", "/api/issues?workspace_id="+testWorkspaceID, map[string]any{
		"title":       title,
		"description": "期望修复：图片预览缩放异常。验收标准：Helper 创建后无需人工补 metadata 或 mention，自动进入 OneStep intake。",
	})
	req.Header.Set("X-Agent-ID", helperID)
	req.Header.Set("X-Task-ID", helperTaskID)
	testHandler.CreateIssue(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("CreateIssue: expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var created IssueResponse
	if err := json.NewDecoder(w.Body).Decode(&created); err != nil {
		t.Fatalf("decode issue: %v", err)
	}
	t.Cleanup(func() {
		cleanupReq := newRequest("DELETE", "/api/issues/"+created.ID, nil)
		cleanupReq = withURLParam(cleanupReq, "id", created.ID)
		testHandler.DeleteIssue(httptest.NewRecorder(), cleanupReq)
	})

	var gotProjectID, assigneeType, assigneeID, workflow, pipelineStatus string
	if err := testPool.QueryRow(ctx, `
		SELECT project_id::text, assignee_type, assignee_id::text, metadata->>'workflow', metadata->>'pipeline_status'
		FROM issue
		WHERE id = $1
	`, created.ID).Scan(&gotProjectID, &assigneeType, &assigneeID, &workflow, &pipelineStatus); err != nil {
		t.Fatalf("load created issue: %v", err)
	}
	if gotProjectID != projectID || assigneeType != "squad" || assigneeID != squadID {
		t.Fatalf("issue routing mismatch: project=%s assignee=%s/%s, want project=%s assignee=squad/%s", gotProjectID, assigneeType, assigneeID, projectID, squadID)
	}
	if workflow != "delivery" || pipelineStatus != "intake" {
		t.Fatalf("metadata mismatch: workflow=%q pipeline_status=%q", workflow, pipelineStatus)
	}

	var commentContent string
	if err := testPool.QueryRow(ctx, `
		SELECT content
		FROM comment
		WHERE issue_id = $1 AND author_type = 'agent' AND author_id = $2
		ORDER BY created_at DESC
		LIMIT 1
	`, created.ID, helperID).Scan(&commentContent); err != nil {
		t.Fatalf("load intake comment: %v", err)
	}
	if !strings.Contains(commentContent, "mention://squad/"+squadID) {
		t.Fatalf("intake comment missing squad mention: %q", commentContent)
	}

	var taskCount int
	if err := testPool.QueryRow(ctx, `
		SELECT count(*)
		FROM agent_task_queue
		WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued' AND is_leader_task = true
	`, created.ID, leaderID).Scan(&taskCount); err != nil {
		t.Fatalf("count leader tasks: %v", err)
	}
	if taskCount != 1 {
		t.Fatalf("expected one queued squad-leader task, got %d", taskCount)
	}
}
