package json

import (
	"errors"
	"testing"

	"github.com/sourcegraph/zoekt/query"
)

func TestApplyScopeToQueryRejectsEmptyScope(t *testing.T) {
	_, err := ApplyScopeToQuery(&query.Const{Value: true}, ContextScope{})
	if !errors.Is(err, ErrEmptyContext) {
		t.Fatalf("expected ErrEmptyContext, got %v", err)
	}
}

func TestNormalizeScopeDeduplicatesAndSorts(t *testing.T) {
	scope := normalizeScope(ContextScope{
		RepoIDs:   []uint32{12, 0, 7, 12},
		RepoNames: []string{" github.com/acme/b ", "github.com/acme/a", "github.com/acme/b"},
	})
	if len(scope.RepoIDs) != 2 || scope.RepoIDs[0] != 7 || scope.RepoIDs[1] != 12 {
		t.Fatalf("unexpected repo ids: %#v", scope.RepoIDs)
	}
	if len(scope.RepoNames) != 2 || scope.RepoNames[0] != "github.com/acme/a" || scope.RepoNames[1] != "github.com/acme/b" {
		t.Fatalf("unexpected repo names: %#v", scope.RepoNames)
	}
}
