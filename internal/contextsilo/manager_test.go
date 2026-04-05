package contextsilo

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	zjson "github.com/sourcegraph/zoekt/internal/json"
)

func TestBuildContextIDDeterministic(t *testing.T) {
	one := BuildContextID("Acme", "Checkout", 12, "2026-03-30T00:00:00Z", "ABC123")
	two := BuildContextID("acme", "checkout", 12, "2026-03-30T00:00:00Z", "abc123")
	if one != two {
		t.Fatalf("expected deterministic context id, got %q vs %q", one, two)
	}
}

func TestBuildContextIDChangesWhenHeadSHAChanges(t *testing.T) {
	first := BuildContextID("acme", "checkout", 12, "2026-03-30T00:00:00Z", "aaa")
	second := BuildContextID("acme", "checkout", 12, "2026-03-30T00:00:00Z", "bbb")
	if first == second {
		t.Fatalf("expected context ids to differ for different head sha values")
	}
}

func TestResolveReadyContextFromFilesystemStatusAndManifest(t *testing.T) {
	tmp := t.TempDir()
	manager, err := NewManager(tmp, "", 24*time.Hour)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	contextID := "ctx_abc"
	contextDir := filepath.Join(tmp, "contexts", contextID)
	manifestPath := filepath.Join(contextDir, "manifest.json")
	statusPath := filepath.Join(contextDir, "status.json")

	manifest := contextManifest{
		ContextID: contextID,
		Repos: []manifestRepo{
			{RepoName: "github.com/acme/checkout", SHA: "sha-a"},
			{RepoName: "github.com/acme/inventory", SHA: "sha-b"},
		},
		GeneratedAt: nowISO(),
	}
	if err := writeJSONAtomic(manifestPath, manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	status := contextStatus{
		ContextID:      contextID,
		Status:         statusReady,
		ManifestPath:   manifestPath,
		IndexDir:       filepath.Join(contextDir, "index"),
		CreatedAt:      nowISO(),
		UpdatedAt:      nowISO(),
		LastAccessedAt: nowISO(),
	}
	if err := writeJSONAtomic(statusPath, status); err != nil {
		t.Fatalf("write status: %v", err)
	}

	scope, err := manager.Resolve(contextID)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(scope.RepoNames) != 2 {
		t.Fatalf("expected 2 repo names, got %d", len(scope.RepoNames))
	}
}

func TestResolveReturnsDeterministicContextErrors(t *testing.T) {
	tmp := t.TempDir()
	manager, err := NewManager(tmp, "", 24*time.Hour)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	_, err = manager.Resolve("ctx_missing")
	if !errors.Is(err, zjson.ErrUnknownContextID) {
		t.Fatalf("expected ErrUnknownContextID, got %v", err)
	}

	contextID := "ctx_building"
	contextDir := filepath.Join(tmp, "contexts", contextID)
	manifestPath := filepath.Join(contextDir, "manifest.json")
	if err := writeJSONAtomic(manifestPath, contextManifest{ContextID: contextID, GeneratedAt: nowISO()}); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	status := contextStatus{
		ContextID:      contextID,
		Status:         statusBuilding,
		ManifestPath:   manifestPath,
		IndexDir:       filepath.Join(contextDir, "index"),
		CreatedAt:      nowISO(),
		UpdatedAt:      nowISO(),
		LastAccessedAt: nowISO(),
	}
	if err := writeJSONAtomic(filepath.Join(contextDir, "status.json"), status); err != nil {
		t.Fatalf("write status: %v", err)
	}

	_, err = manager.Resolve(contextID)
	if !errors.Is(err, zjson.ErrContextNotReady) {
		t.Fatalf("expected ErrContextNotReady, got %v", err)
	}
}

func TestLoadNameFiltersTreatsPatternsAsCaseInsensitive(t *testing.T) {
	tmp := t.TempDir()
	configPath := filepath.Join(tmp, "context_filters.json")
	config := `[{"GithubOrg":"acme","Name":".*_TEST.*"}]`
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	manager, err := NewManager(tmp, configPath, 24*time.Hour)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	filters, err := manager.loadNameFilters("acme")
	if err != nil {
		t.Fatalf("loadNameFilters: %v", err)
	}
	if len(filters) != 1 {
		t.Fatalf("expected one filter, got %d", len(filters))
	}
	if !matchesFilters(filters, "pantry_pal_ui_test") {
		t.Fatalf("expected lowercase repo name to match case-insensitive filter")
	}
	if !matchesFilters(filters, "PANTRY_PAL_API_TEST") {
		t.Fatalf("expected uppercase repo name to match case-insensitive filter")
	}
}

func TestResolveRepoSHAAtOrBeforeReturnsSHAWhenPresent(t *testing.T) {
	client := &githubClient{
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Host != "api.github.com" || req.URL.Path != "/repos/acme/checkout/commits" {
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`[{"sha":"abc123"}]`)),
					Request:    req,
				}, nil
			}),
		},
	}

	sha, err := client.resolveRepoSHAAtOrBefore(context.Background(), "acme", "checkout", "2026-03-30T00:00:00Z")
	if err != nil {
		t.Fatalf("resolveRepoSHAAtOrBefore: %v", err)
	}
	if sha != "abc123" {
		t.Fatalf("expected sha abc123, got %q", sha)
	}
}

func TestResolveRepoSHAAtOrBeforeReturnsNoCommitAtAnchorError(t *testing.T) {
	client := &githubClient{
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Host != "api.github.com" || req.URL.Path != "/repos/acme/newrepo/commits" {
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`[]`)),
					Request:    req,
				}, nil
			}),
		},
	}

	_, err := client.resolveRepoSHAAtOrBefore(context.Background(), "acme", "newrepo", "2026-03-30T00:00:00Z")
	if !errors.Is(err, ErrNoCommitAtOrBeforeAnchor) {
		t.Fatalf("expected ErrNoCommitAtOrBeforeAnchor, got %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
