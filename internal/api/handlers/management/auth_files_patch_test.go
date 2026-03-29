package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestPatchAuthFileFieldsUpdatesOAuthChannelLabel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	_, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "oauth-auth-1",
		FileName: "oauth-auth-1.json",
		Provider: "claude",
		Metadata: map[string]any{
			"email": "old@example.com",
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}

	h := &Handler{
		cfg:         &config.Config{},
		authManager: manager,
	}

	body, err := json.Marshal(map[string]any{
		"name":  "oauth-auth-1.json",
		"label": "Team Alpha",
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/auth-files/fields", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PatchAuthFileFields(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d, body=%s", http.StatusOK, rec.Code, rec.Body.String())
	}

	updated, ok := manager.GetByID("oauth-auth-1")
	if !ok || updated == nil {
		t.Fatal("expected updated auth")
	}
	if updated.Label != "Team Alpha" {
		t.Fatalf("label = %q, want %q", updated.Label, "Team Alpha")
	}
	if got, _ := updated.Metadata["label"].(string); got != "Team Alpha" {
		t.Fatalf("metadata label = %q, want %q", got, "Team Alpha")
	}
}

func TestPatchAuthFileFieldsRejectsDuplicateOAuthChannelLabel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	_, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "oauth-auth-2",
		FileName: "oauth-auth-2.json",
		Provider: "gemini",
		Metadata: map[string]any{
			"email": "oauth@example.com",
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}

	h := &Handler{
		cfg: &config.Config{
			ClaudeKey: []config.ClaudeKey{
				{Name: "Shared Channel"},
			},
		},
		authManager: manager,
	}

	body, err := json.Marshal(map[string]any{
		"name":  "oauth-auth-2.json",
		"label": "shared channel",
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/auth-files/fields", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PatchAuthFileFields(c)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d, body=%s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}

	updated, ok := manager.GetByID("oauth-auth-2")
	if !ok || updated == nil {
		t.Fatal("expected auth to remain registered")
	}
	if updated.Label != "" {
		t.Fatalf("label = %q, want empty", updated.Label)
	}
	if _, exists := updated.Metadata["label"]; exists {
		t.Fatalf("unexpected metadata label after rejected update: %v", updated.Metadata["label"])
	}
}
