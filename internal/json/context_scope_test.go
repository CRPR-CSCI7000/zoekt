package json

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sourcegraph/zoekt/query"
)

func TestApplyScopeToQueryRejectsEmptyScope(t *testing.T) {
	_, err := ApplyScopeToQuery(&query.Const{Value: true}, ContextScope{})
	if !errors.Is(err, ErrEmptyContext) {
		t.Fatalf("expected ErrEmptyContext, got %v", err)
	}
}

func TestCatalogScopeResolverResolveReadyContext(t *testing.T) {
	tmpDir := t.TempDir()
	catalogPath := filepath.Join(tmpDir, "context_catalog.json")
	payload := map[string]any{
		"contexts": map[string]any{
			"ctx_ready": map[string]any{
				"status":     "READY",
				"repo_names": []string{"github.com/acme/repo"},
				"repo_ids":   []int{12, 12, 0},
			},
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := os.WriteFile(catalogPath, raw, 0o644); err != nil {
		t.Fatalf("write catalog: %v", err)
	}

	resolver := NewCatalogScopeResolver(catalogPath)
	scope, err := resolver.Resolve("ctx_ready")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(scope.RepoNames) != 1 || scope.RepoNames[0] != "github.com/acme/repo" {
		t.Fatalf("unexpected repo names: %#v", scope.RepoNames)
	}
	if len(scope.RepoIDs) != 1 || scope.RepoIDs[0] != 12 {
		t.Fatalf("unexpected repo ids: %#v", scope.RepoIDs)
	}
}

func TestCatalogScopeResolverRejectsNotReadyContext(t *testing.T) {
	tmpDir := t.TempDir()
	catalogPath := filepath.Join(tmpDir, "context_catalog.json")
	payload := map[string]any{
		"contexts": map[string]any{
			"ctx_building": map[string]any{
				"status":     "BUILDING",
				"repo_names": []string{"github.com/acme/repo"},
			},
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := os.WriteFile(catalogPath, raw, 0o644); err != nil {
		t.Fatalf("write catalog: %v", err)
	}

	resolver := NewCatalogScopeResolver(catalogPath)
	_, err = resolver.Resolve("ctx_building")
	if !errors.Is(err, ErrContextNotReady) {
		t.Fatalf("expected ErrContextNotReady, got %v", err)
	}
}

func TestCatalogScopeResolverRejectsMissingContextID(t *testing.T) {
	tmpDir := t.TempDir()
	catalogPath := filepath.Join(tmpDir, "context_catalog.json")
	if err := os.WriteFile(catalogPath, []byte(`{"contexts":{}}`), 0o644); err != nil {
		t.Fatalf("write catalog: %v", err)
	}

	resolver := NewCatalogScopeResolver(catalogPath)
	_, err := resolver.Resolve("")
	if !errors.Is(err, ErrMissingContextID) {
		t.Fatalf("expected ErrMissingContextID, got %v", err)
	}
}
