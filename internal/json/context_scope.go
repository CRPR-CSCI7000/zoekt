package json

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/grafana/regexp"
	"github.com/sourcegraph/zoekt/query"
)

var (
	ErrMissingContextID = errors.New("missing required context_id")
	ErrUnknownContextID = errors.New("invalid context_id")
	ErrContextNotReady  = errors.New("context_id is not ready")
	ErrEmptyContext     = errors.New("context scope is empty")
)

type ContextScope struct {
	RepoIDs   []uint32
	RepoNames []string
}

type ContextScopeResolver interface {
	Resolve(contextID string) (ContextScope, error)
}

func normalizeScope(scope ContextScope) ContextScope {
	repoIDSet := map[uint32]struct{}{}
	repoIDs := make([]uint32, 0, len(scope.RepoIDs))
	for _, id := range scope.RepoIDs {
		if id == 0 {
			continue
		}
		if _, ok := repoIDSet[id]; ok {
			continue
		}
		repoIDSet[id] = struct{}{}
		repoIDs = append(repoIDs, id)
	}
	sort.Slice(repoIDs, func(i, j int) bool { return repoIDs[i] < repoIDs[j] })

	repoNameSet := map[string]struct{}{}
	repoNames := make([]string, 0, len(scope.RepoNames))
	for _, name := range scope.RepoNames {
		normalized := strings.TrimSpace(name)
		if normalized == "" {
			continue
		}
		if _, ok := repoNameSet[normalized]; ok {
			continue
		}
		repoNameSet[normalized] = struct{}{}
		repoNames = append(repoNames, normalized)
	}
	sort.Strings(repoNames)

	return ContextScope{
		RepoIDs:   repoIDs,
		RepoNames: repoNames,
	}
}

func ApplyScopeToQuery(base query.Q, scope ContextScope) (query.Q, error) {
	normalized := normalizeScope(scope)
	if len(normalized.RepoIDs) == 0 && len(normalized.RepoNames) == 0 {
		return nil, ErrEmptyContext
	}

	scoped := base
	if len(normalized.RepoNames) > 0 {
		nameQueries := make([]query.Q, 0, len(normalized.RepoNames))
		for _, repoName := range normalized.RepoNames {
			re, err := regexp.Compile("(?i)^" + regexp.QuoteMeta(repoName) + "$")
			if err != nil {
				return nil, fmt.Errorf("invalid scoped repo name %q: %w", repoName, err)
			}
			nameQueries = append(nameQueries, &query.Repo{Regexp: re})
		}
		if len(nameQueries) == 1 {
			scoped = query.NewAnd(scoped, nameQueries[0])
		} else {
			scoped = query.NewAnd(scoped, &query.Or{Children: nameQueries})
		}
		return scoped, nil
	}

	if len(normalized.RepoIDs) > 0 {
		scoped = query.NewAnd(scoped, query.NewRepoIDs(normalized.RepoIDs...))
	}
	return scoped, nil
}
