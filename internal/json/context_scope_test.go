package json

import (
	"errors"
	"strings"
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

func TestApplyScopeToQueryPrefersRepoNamesOverRepoIDsWhenBothProvided(t *testing.T) {
	q, err := ApplyScopeToQuery(
		&query.Const{Value: true},
		ContextScope{
			RepoIDs:   []uint32{7, 12},
			RepoNames: []string{"github.com/acme/a", "github.com/acme/b"},
		},
	)
	if err != nil {
		t.Fatalf("ApplyScopeToQuery: %v", err)
	}

	rendered := q.String()
	if strings.Contains(rendered, "(repoids ") {
		t.Fatalf("expected scoped query to avoid repoid filter when repo names exist, got %s", rendered)
	}
	if !strings.Contains(rendered, "repo:(?i)^github\\.com/acme/a$") {
		t.Fatalf("expected scoped query to include repo-name regex for acme/a, got %s", rendered)
	}
	if !strings.Contains(rendered, "repo:(?i)^github\\.com/acme/b$") {
		t.Fatalf("expected scoped query to include repo-name regex for acme/b, got %s", rendered)
	}
}
