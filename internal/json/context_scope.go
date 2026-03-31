package json

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

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

type CatalogScopeResolver struct {
	path string

	mu         sync.Mutex
	lastLoaded time.Time
	lastErr    error
	cache      map[string]catalogContext
}

type catalogPayload struct {
	Contexts map[string]catalogContext `json:"contexts"`
}

type catalogContext struct {
	Status    string   `json:"status"`
	RepoIDs   []uint32 `json:"repo_ids"`
	RepoNames []string `json:"repo_names"`
}

func NewCatalogScopeResolver(path string) *CatalogScopeResolver {
	return &CatalogScopeResolver{
		path: strings.TrimSpace(path),
	}
}

func (r *CatalogScopeResolver) Resolve(contextID string) (ContextScope, error) {
	if strings.TrimSpace(contextID) == "" {
		return ContextScope{}, ErrMissingContextID
	}

	contexts, err := r.load()
	if err != nil {
		return ContextScope{}, err
	}

	entry, ok := contexts[contextID]
	if !ok {
		return ContextScope{}, fmt.Errorf("%w: %s", ErrUnknownContextID, contextID)
	}
	if !strings.EqualFold(strings.TrimSpace(entry.Status), "READY") {
		return ContextScope{}, fmt.Errorf("%w: %s", ErrContextNotReady, contextID)
	}

	scope := normalizeScope(ContextScope{
		RepoIDs:   append([]uint32(nil), entry.RepoIDs...),
		RepoNames: append([]string(nil), entry.RepoNames...),
	})
	if len(scope.RepoIDs) == 0 && len(scope.RepoNames) == 0 {
		return ContextScope{}, fmt.Errorf("%w: %s", ErrEmptyContext, contextID)
	}
	return scope, nil
}

func (r *CatalogScopeResolver) load() (map[string]catalogContext, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if strings.TrimSpace(r.path) == "" {
		return nil, fmt.Errorf("context catalog path is empty")
	}

	fi, err := os.Stat(r.path)
	if err != nil {
		r.lastErr = err
		return nil, fmt.Errorf("failed to stat context catalog %s: %w", r.path, err)
	}

	if r.cache != nil && !fi.ModTime().After(r.lastLoaded) {
		return r.cache, nil
	}

	raw, err := os.ReadFile(r.path)
	if err != nil {
		r.lastErr = err
		return nil, fmt.Errorf("failed to read context catalog %s: %w", r.path, err)
	}

	var payload catalogPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		r.lastErr = err
		return nil, fmt.Errorf("failed to parse context catalog %s: %w", r.path, err)
	}
	if payload.Contexts == nil {
		payload.Contexts = map[string]catalogContext{}
	}

	r.cache = payload.Contexts
	r.lastLoaded = fi.ModTime()
	r.lastErr = nil
	return r.cache, nil
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
	if len(normalized.RepoIDs) > 0 {
		scoped = query.NewAnd(scoped, query.NewRepoIDs(normalized.RepoIDs...))
	}
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
	}
	return scoped, nil
}

func DefaultCatalogPath(root string) string {
	base := strings.TrimSpace(root)
	if base == "" {
		return ""
	}
	return filepath.Join(base, "catalog", "context_catalog.json")
}
