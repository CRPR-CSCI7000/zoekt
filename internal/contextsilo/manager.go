package contextsilo

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/sourcegraph/zoekt"
	zjson "github.com/sourcegraph/zoekt/internal/json"
	"github.com/sourcegraph/zoekt/query"
	"github.com/sourcegraph/zoekt/search"
)

const (
	statusBuilding   = "BUILDING"
	statusReady      = "READY"
	statusFailed     = "FAILED"
	contextIDVersion = "v2"
)

type ensureRequest struct {
	Owner    string `json:"owner"`
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	Wait     bool   `json:"wait"`
}

type ensureResponse struct {
	ContextID       string `json:"context_id"`
	Owner           string `json:"owner"`
	Repo            string `json:"repo"`
	PRNumber        int    `json:"pr_number"`
	AnchorCreatedAt string `json:"anchor_created_at"`
	HeadSHA         string `json:"head_sha,omitempty"`
	Status          string `json:"status"`
	ManifestPath    string `json:"manifest_path"`
	IndexDir        string `json:"index_dir"`
	Error           string `json:"error,omitempty"`
}

type contextStatus struct {
	ContextID       string `json:"context_id"`
	Owner           string `json:"owner"`
	Repo            string `json:"repo"`
	PRNumber        int    `json:"pr_number"`
	AnchorCreatedAt string `json:"anchor_created_at"`
	HeadSHA         string `json:"head_sha,omitempty"`
	Status          string `json:"status"`
	Error           string `json:"error,omitempty"`
	ManifestPath    string `json:"manifest_path"`
	IndexDir        string `json:"index_dir"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
	LastAccessedAt  string `json:"last_accessed_at"`
}

type manifestIdentity struct {
	Owner           string `json:"owner"`
	Repo            string `json:"repo"`
	PRNumber        int    `json:"pr_number"`
	AnchorCreatedAt string `json:"anchor_created_at"`
	HeadSHA         string `json:"head_sha,omitempty"`
}

type manifestRepo struct {
	RepoOwner string `json:"repo_owner"`
	Repo      string `json:"repo"`
	RepoName  string `json:"repo_name"`
	SHA       string `json:"sha"`
}

type contextManifest struct {
	ContextID   string           `json:"context_id"`
	Identity    manifestIdentity `json:"identity"`
	Repos       []manifestRepo   `json:"repos"`
	GeneratedAt string           `json:"generated_at"`
}

type nameFilterConfig struct {
	GithubOrg string `json:"GithubOrg"`
	Name      string `json:"Name"`
}

type compiledFilter struct {
	org     string
	pattern *regexp.Regexp
}

type githubClient struct {
	httpClient *http.Client
	token      string
}

type Manager struct {
	indexRoot    string
	configPath   string
	contextsRoot string
	idleTTL      time.Duration
	github       *githubClient

	mu        sync.Mutex
	locks     map[string]*sync.Mutex
	searchers map[string]zoekt.Streamer
}

func NewManager(indexRoot string, configPath string, idleTTL time.Duration) (*Manager, error) {
	root := strings.TrimSpace(indexRoot)
	if root == "" {
		return nil, fmt.Errorf("index root is required")
	}
	if idleTTL <= 0 {
		idleTTL = 24 * time.Hour
	}
	contextsRoot := filepath.Join(root, "contexts")
	if err := os.MkdirAll(contextsRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create contexts root: %w", err)
	}

	client := &githubClient{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		token:      strings.TrimSpace(os.Getenv("GITHUB_TOKEN")),
	}

	return &Manager{
		indexRoot:    root,
		configPath:   strings.TrimSpace(configPath),
		contextsRoot: contextsRoot,
		idleTTL:      idleTTL,
		github:       client,
		locks:        map[string]*sync.Mutex{},
		searchers:    map[string]zoekt.Streamer{},
	}, nil
}

func BuildContextID(owner string, repo string, prNumber int, anchorCreatedAt string, headSHA string) string {
	identity := fmt.Sprintf(
		"%s|%s/%s/%d@%s#%s",
		contextIDVersion,
		strings.ToLower(strings.TrimSpace(owner)),
		strings.ToLower(strings.TrimSpace(repo)),
		prNumber,
		strings.TrimSpace(anchorCreatedAt),
		strings.ToLower(strings.TrimSpace(headSHA)),
	)
	sum := sha1.Sum([]byte(identity))
	return "ctx_" + fmt.Sprintf("%x", sum[:])[:20]
}

func (m *Manager) ServeEnsure(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "Only POST is supported")
		return
	}

	var req ensureRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	if !req.Wait {
		req.Wait = false
	} else {
		req.Wait = true
	}

	resp, err := m.Ensure(r.Context(), req)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if errors.Is(err, ErrInvalidRequest) {
			statusCode = http.StatusBadRequest
		}
		writeJSONError(w, statusCode, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func (m *Manager) ServeStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "Only GET is supported")
		return
	}
	contextID := strings.TrimSpace(r.URL.Query().Get("context_id"))
	if contextID == "" {
		writeJSONError(w, http.StatusBadRequest, "context_id is required")
		return
	}
	statusPath := m.statusPath(contextID)
	status, err := readStatusFile(statusPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeJSONError(w, http.StatusNotFound, "context not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, status)
}

var ErrInvalidRequest = errors.New("invalid ensure request")
var ErrNoCommitAtOrBeforeAnchor = errors.New("no commit at or before anchor")

func (m *Manager) Ensure(ctx context.Context, req ensureRequest) (*ensureResponse, error) {
	owner := strings.TrimSpace(req.Owner)
	repo := strings.TrimSpace(req.Repo)
	if owner == "" || repo == "" {
		return nil, fmt.Errorf("%w: owner and repo are required", ErrInvalidRequest)
	}
	if req.PRNumber <= 0 {
		return nil, fmt.Errorf("%w: pr_number must be > 0", ErrInvalidRequest)
	}

	_ = m.gcSweep()

	pr, err := m.github.getPullRequest(ctx, owner, repo, req.PRNumber)
	if err != nil {
		return nil, err
	}
	anchorRaw := strings.TrimSpace(asString(pr["created_at"]))
	if anchorRaw == "" {
		return nil, fmt.Errorf("pull request payload missing created_at")
	}
	anchor, err := normalizeTimestamp(anchorRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid pull request created_at: %w", err)
	}
	headSHA, sourceCloneOwner, sourceCloneRepo, err := extractPRHead(pr, owner, repo)
	if err != nil {
		return nil, err
	}

	contextID := BuildContextID(owner, repo, req.PRNumber, anchor, headSHA)
	lock := m.contextLock(contextID)
	lock.Lock()
	defer lock.Unlock()

	statusPath := m.statusPath(contextID)
	status, err := readStatusFile(statusPath)
	if err == nil && strings.EqualFold(status.Status, statusReady) {
		if err := m.touch(status); err != nil {
			return nil, err
		}
		return m.toEnsureResponse(status), nil
	}

	status, err = m.buildContext(
		ctx,
		owner,
		repo,
		req.PRNumber,
		anchor,
		headSHA,
		sourceCloneOwner,
		sourceCloneRepo,
		contextID,
	)
	if err != nil {
		return nil, err
	}
	return m.toEnsureResponse(status), nil
}

func (m *Manager) Resolve(contextID string) (zjson.ContextScope, error) {
	contextID = strings.TrimSpace(contextID)
	if contextID == "" {
		return zjson.ContextScope{}, zjson.ErrMissingContextID
	}

	status, err := readStatusFile(m.statusPath(contextID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return zjson.ContextScope{}, fmt.Errorf("%w: %s", zjson.ErrUnknownContextID, contextID)
		}
		return zjson.ContextScope{}, err
	}
	if !strings.EqualFold(status.Status, statusReady) {
		return zjson.ContextScope{}, fmt.Errorf("%w: %s", zjson.ErrContextNotReady, contextID)
	}

	manifest, err := readManifest(status.ManifestPath)
	if err != nil {
		return zjson.ContextScope{}, err
	}
	repoNames := make([]string, 0, len(manifest.Repos))
	repoIDs := make([]uint32, 0, len(manifest.Repos))
	repoIDSeen := map[uint32]struct{}{}
	for _, repo := range manifest.Repos {
		name := strings.TrimSpace(repo.RepoName)
		if name == "" {
			continue
		}
		repoNames = append(repoNames, name)
		repoID := uint32(stableRepoID(name))
		if _, exists := repoIDSeen[repoID]; !exists {
			repoIDSeen[repoID] = struct{}{}
			repoIDs = append(repoIDs, repoID)
		}
	}
	if len(repoNames) == 0 {
		return zjson.ContextScope{}, fmt.Errorf("%w: %s", zjson.ErrEmptyContext, contextID)
	}
	sort.Strings(repoNames)
	sort.Slice(repoIDs, func(i, j int) bool { return repoIDs[i] < repoIDs[j] })

	_ = m.touch(status)
	return zjson.ContextScope{
		RepoIDs:   repoIDs,
		RepoNames: repoNames,
	}, nil
}

func (m *Manager) Streamer(contextID string) (zoekt.Streamer, error) {
	contextID = strings.TrimSpace(contextID)
	if contextID == "" {
		return nil, zjson.ErrMissingContextID
	}

	status, err := readStatusFile(m.statusPath(contextID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", zjson.ErrUnknownContextID, contextID)
		}
		return nil, err
	}
	if !strings.EqualFold(status.Status, statusReady) {
		return nil, fmt.Errorf("%w: %s", zjson.ErrContextNotReady, contextID)
	}

	_ = m.touch(status)

	m.mu.Lock()
	defer m.mu.Unlock()
	if streamer, ok := m.searchers[contextID]; ok {
		return streamer, nil
	}
	streamer, err := search.NewDirectorySearcherFast(status.IndexDir)
	if err != nil {
		return nil, err
	}
	m.searchers[contextID] = streamer
	return streamer, nil
}

func (m *Manager) buildContext(
	ctx context.Context,
	owner string,
	repo string,
	prNumber int,
	anchor string,
	headSHA string,
	sourceCloneOwner string,
	sourceCloneRepo string,
	contextID string,
) (*contextStatus, error) {
	contextDir := m.contextDir(contextID)
	manifestPath := filepath.Join(contextDir, "manifest.json")
	indexDir := filepath.Join(contextDir, "index")
	reposDir := filepath.Join(contextDir, "repos")
	statusPath := m.statusPath(contextID)
	lockPath := filepath.Join(contextDir, "lock")
	if err := os.MkdirAll(contextDir, 0o755); err != nil {
		return nil, err
	}

	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	defer lockFile.Close()
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		return nil, err
	}
	defer func() { _ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN) }()

	now := nowISO()
	status := &contextStatus{
		ContextID:       contextID,
		Owner:           owner,
		Repo:            repo,
		PRNumber:        prNumber,
		AnchorCreatedAt: anchor,
		HeadSHA:         headSHA,
		Status:          statusBuilding,
		ManifestPath:    manifestPath,
		IndexDir:        indexDir,
		CreatedAt:       now,
		UpdatedAt:       now,
		LastAccessedAt:  now,
	}
	if err := writeJSONAtomic(statusPath, status); err != nil {
		return nil, err
	}

	manifest, err := m.buildManifest(
		ctx,
		owner,
		repo,
		prNumber,
		anchor,
		headSHA,
		sourceCloneOwner,
		sourceCloneRepo,
		contextID,
	)
	if err != nil {
		status.Status = statusFailed
		status.Error = err.Error()
		status.UpdatedAt = nowISO()
		status.LastAccessedAt = status.UpdatedAt
		_ = writeJSONAtomic(statusPath, status)
		return nil, err
	}
	if err := writeJSONAtomic(manifestPath, manifest); err != nil {
		status.Status = statusFailed
		status.Error = err.Error()
		status.UpdatedAt = nowISO()
		status.LastAccessedAt = status.UpdatedAt
		_ = writeJSONAtomic(statusPath, status)
		return nil, err
	}

	if err := os.RemoveAll(indexDir); err != nil {
		return nil, err
	}
	if err := os.RemoveAll(reposDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(indexDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(reposDir, 0o755); err != nil {
		return nil, err
	}

	for _, entry := range manifest.Repos {
		if err := m.indexRepoAtAnchor(ctx, reposDir, indexDir, entry); err != nil {
			status.Status = statusFailed
			status.Error = err.Error()
			status.UpdatedAt = nowISO()
			status.LastAccessedAt = status.UpdatedAt
			_ = writeJSONAtomic(statusPath, status)
			return nil, err
		}
	}

	verifiedSearcher, err := verifyManifestReady(ctx, indexDir, manifest.Repos)
	if err != nil {
		status.Status = statusFailed
		status.Error = err.Error()
		status.UpdatedAt = nowISO()
		status.LastAccessedAt = status.UpdatedAt
		_ = writeJSONAtomic(statusPath, status)
		return nil, err
	}

	status.Status = statusReady
	status.Error = ""
	status.UpdatedAt = nowISO()
	status.LastAccessedAt = status.UpdatedAt
	if err := writeJSONAtomic(statusPath, status); err != nil {
		verifiedSearcher.Close()
		return nil, err
	}

	m.mu.Lock()
	if stale, ok := m.searchers[contextID]; ok {
		stale.Close()
	}
	m.searchers[contextID] = verifiedSearcher
	m.mu.Unlock()

	return status, nil
}

func verifyManifestReady(ctx context.Context, indexDir string, manifestRepos []manifestRepo) (zoekt.Streamer, error) {
	streamer, err := search.NewDirectorySearcherFast(indexDir)
	if err != nil {
		return nil, err
	}

	for {
		list, err := streamer.List(ctx, &query.Const{Value: true}, nil)
		if err != nil {
			streamer.Close()
			return nil, err
		}

		// Searcher startup is asynchronous. Wait until shard load settles before
		// asserting manifest completeness, otherwise we can fail with false
		// "missing repository" errors while shards are still loading.
		if list.Crashes > 0 {
			select {
			case <-ctx.Done():
				streamer.Close()
				return nil, ctx.Err()
			case <-time.After(250 * time.Millisecond):
				continue
			}
		}

		inventory := map[string]string{}
		for _, repoEntry := range list.Repos {
			name := strings.ToLower(strings.TrimSpace(repoEntry.Repository.Name))
			if name == "" {
				continue
			}
			inventory[name] = extractHeadSHA(repoEntry.Repository.Branches)
		}

		for _, repo := range manifestRepos {
			name := strings.ToLower(strings.TrimSpace(repo.RepoName))
			expected := strings.ToLower(strings.TrimSpace(repo.SHA))
			got, ok := inventory[name]
			if !ok {
				streamer.Close()
				return nil, fmt.Errorf("context index missing repository %s", repo.RepoName)
			}
			got = strings.ToLower(strings.TrimSpace(got))
			if expected != "" && got != "" && expected != got {
				streamer.Close()
				return nil, fmt.Errorf("indexed SHA mismatch for %s: expected=%s got=%s", repo.RepoName, expected, got)
			}
		}
		return streamer, nil
	}
}

func extractHeadSHA(branches []zoekt.RepositoryBranch) string {
	first := ""
	for _, branch := range branches {
		version := strings.TrimSpace(branch.Version)
		if version != "" && first == "" {
			first = version
		}
		if strings.EqualFold(strings.TrimSpace(branch.Name), "HEAD") && version != "" {
			return version
		}
		if strings.EqualFold(strings.TrimSpace(branch.Name), "crpr-context") && version != "" {
			return version
		}
	}
	return first
}

func (m *Manager) indexRepoAtAnchor(ctx context.Context, reposDir string, indexDir string, entry manifestRepo) error {
	repoName := strings.TrimSpace(entry.RepoName)
	owner := strings.TrimSpace(entry.RepoOwner)
	repo := strings.TrimSpace(entry.Repo)
	sha := strings.TrimSpace(entry.SHA)
	if repoName == "" || owner == "" || repo == "" || sha == "" {
		return fmt.Errorf("invalid manifest entry for indexing")
	}

	cloneURL := fmt.Sprintf("https://github.com/%s/%s.git", owner, repo)
	repoID := stableRepoID(repoName)
	if err := runCmd(ctx, "zoekt-git-clone", "-dest", reposDir, "-name", repoName, "-repoid", strconv.Itoa(repoID), cloneURL); err != nil {
		return fmt.Errorf("clone %s: %w", repoName, err)
	}

	repoPath := filepath.Join(reposDir, filepath.FromSlash(repoName)+".git")
	webURL := fmt.Sprintf("https://github.com/%s/%s", owner, repo)
	if err := runCmd(ctx, "git", "-C", repoPath, "config", "zoekt.web-url", webURL); err != nil {
		return fmt.Errorf("set zoekt.web-url for %s: %w", repoName, err)
	}
	if err := runCmd(ctx, "git", "-C", repoPath, "config", "zoekt.web-url-type", "github"); err != nil {
		return fmt.Errorf("set zoekt.web-url-type for %s: %w", repoName, err)
	}

	if err := runCmd(ctx, "git", "-C", repoPath, "fetch", "--quiet", "origin", sha); err != nil {
		return fmt.Errorf("fetch anchored sha %s for %s: %w", sha, repoName, err)
	}
	if err := runCmd(ctx, "git", "-C", repoPath, "update-ref", "refs/heads/crpr-context", sha); err != nil {
		return fmt.Errorf("update anchored ref for %s: %w", repoName, err)
	}
	if err := runCmd(ctx, "zoekt-git-index", "-index", indexDir, "-branches", "crpr-context", repoPath); err != nil {
		return fmt.Errorf("index %s: %w", repoName, err)
	}
	return nil
}

func stableRepoID(repoName string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(strings.TrimSpace(repoName))))
	value := int(h.Sum32() & 0x7fffffff)
	if value == 0 {
		value = 1
	}
	return value
}

func runCmd(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = bytes.NewBuffer(nil)
	cmd.Stdout = io.Discard
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return fmt.Errorf("%s %s: %s", name, strings.Join(args, " "), message)
	}
	return nil
}

func (m *Manager) buildManifest(
	ctx context.Context,
	owner string,
	repo string,
	prNumber int,
	anchor string,
	headSHA string,
	sourceCloneOwner string,
	sourceCloneRepo string,
	contextID string,
) (*contextManifest, error) {
	repos, err := m.github.listOrgRepos(ctx, owner)
	if err != nil {
		return nil, err
	}

	filters, _ := m.loadNameFilters(owner)
	selected := make([]string, 0, len(repos))
	selectedSet := map[string]struct{}{}
	for _, name := range repos {
		if !matchesFilters(filters, name) {
			continue
		}
		selected = append(selected, name)
		selectedSet[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}

	sourceRepoName := strings.TrimSpace(repo)
	if sourceRepoName != "" {
		key := strings.ToLower(sourceRepoName)
		if _, ok := selectedSet[key]; !ok {
			selected = append(selected, sourceRepoName)
			selectedSet[key] = struct{}{}
		}
	}
	sort.Strings(selected)

	manifestRepos := make([]manifestRepo, 0, len(selected))
	for _, repoName := range selected {
		cloneOwner := owner
		cloneRepo := repoName
		sha := ""
		canonicalName := canonicalRepoName(owner, repoName)

		if strings.EqualFold(repoName, repo) {
			sha = strings.TrimSpace(headSHA)
			if sha == "" {
				return nil, fmt.Errorf("source repo head sha is required")
			}
			if strings.TrimSpace(sourceCloneOwner) != "" {
				cloneOwner = sourceCloneOwner
			}
			if strings.TrimSpace(sourceCloneRepo) != "" {
				cloneRepo = sourceCloneRepo
			}
			canonicalName = canonicalRepoName(owner, repo)
		} else {
			resolvedSHA, err := m.github.resolveRepoSHAAtOrBefore(ctx, owner, repoName, anchor)
			if err != nil {
				if errors.Is(err, ErrNoCommitAtOrBeforeAnchor) {
					continue
				}
				return nil, fmt.Errorf("resolve sha for %s/%s at %s: %w", owner, repoName, anchor, err)
			}
			sha = resolvedSHA
		}

		manifestRepos = append(manifestRepos, manifestRepo{
			RepoOwner: cloneOwner,
			Repo:      cloneRepo,
			RepoName:  canonicalName,
			SHA:       sha,
		})
	}

	return &contextManifest{
		ContextID: contextID,
		Identity: manifestIdentity{
			Owner:           owner,
			Repo:            repo,
			PRNumber:        prNumber,
			AnchorCreatedAt: anchor,
			HeadSHA:         strings.TrimSpace(headSHA),
		},
		Repos:       manifestRepos,
		GeneratedAt: nowISO(),
	}, nil
}

func canonicalRepoName(owner string, repo string) string {
	return fmt.Sprintf("github.com/%s/%s", strings.ToLower(strings.TrimSpace(owner)), strings.ToLower(strings.TrimSpace(repo)))
}

func (m *Manager) loadNameFilters(owner string) ([]compiledFilter, error) {
	if strings.TrimSpace(m.configPath) == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(m.configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var payload []nameFilterConfig
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, nil
	}

	ownerLower := strings.ToLower(strings.TrimSpace(owner))
	filters := make([]compiledFilter, 0, len(payload))
	for _, item := range payload {
		cfgOwner := strings.ToLower(strings.TrimSpace(item.GithubOrg))
		if cfgOwner != "" && cfgOwner != ownerLower {
			continue
		}
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		ciRe, err := regexp.Compile("(?i)" + name)
		if err != nil {
			continue
		}
		filters = append(filters, compiledFilter{org: cfgOwner, pattern: ciRe})
	}
	return filters, nil
}

func matchesFilters(filters []compiledFilter, repoName string) bool {
	if len(filters) == 0 {
		return true
	}
	for _, f := range filters {
		if f.pattern.MatchString(repoName) {
			return true
		}
	}
	return false
}

func (m *Manager) contextLock(contextID string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lock, ok := m.locks[contextID]; ok {
		return lock
	}
	lock := &sync.Mutex{}
	m.locks[contextID] = lock
	return lock
}

func (m *Manager) touch(status *contextStatus) error {
	status.LastAccessedAt = nowISO()
	status.UpdatedAt = status.LastAccessedAt
	return writeJSONAtomic(m.statusPath(status.ContextID), status)
}

func (m *Manager) gcSweep() error {
	entries, err := os.ReadDir(m.contextsRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	cutoff := time.Now().UTC().Add(-m.idleTTL)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		contextID := entry.Name()
		status, err := readStatusFile(m.statusPath(contextID))
		if err != nil {
			continue
		}
		lastAccess, err := parseTimestamp(status.LastAccessedAt)
		if err != nil {
			continue
		}
		if !lastAccess.Before(cutoff) {
			continue
		}

		m.mu.Lock()
		if streamer, ok := m.searchers[contextID]; ok {
			streamer.Close()
			delete(m.searchers, contextID)
		}
		m.mu.Unlock()

		_ = os.RemoveAll(m.contextDir(contextID))
	}
	return nil
}

func (m *Manager) toEnsureResponse(status *contextStatus) *ensureResponse {
	return &ensureResponse{
		ContextID:       status.ContextID,
		Owner:           status.Owner,
		Repo:            status.Repo,
		PRNumber:        status.PRNumber,
		AnchorCreatedAt: status.AnchorCreatedAt,
		HeadSHA:         status.HeadSHA,
		Status:          status.Status,
		ManifestPath:    status.ManifestPath,
		IndexDir:        status.IndexDir,
		Error:           status.Error,
	}
}

func (m *Manager) contextDir(contextID string) string {
	return filepath.Join(m.contextsRoot, contextID)
}

func (m *Manager) statusPath(contextID string) string {
	return filepath.Join(m.contextDir(contextID), "status.json")
}

func readStatusFile(path string) (*contextStatus, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var status contextStatus
	if err := json.Unmarshal(raw, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func readManifest(path string) (*contextManifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var manifest contextManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func writeJSONAtomic(path string, payload any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]string{"error": strings.TrimSpace(message)})
}

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func parseTimestamp(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC(), nil
	}
	return time.Parse(time.RFC3339, value)
}

func normalizeTimestamp(value string) (string, error) {
	parsed, err := parseTimestamp(value)
	if err != nil {
		return "", err
	}
	return parsed.UTC().Format(time.RFC3339), nil
}

func asString(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprintf("%v", value)
}

func extractPRHead(pr map[string]any, defaultOwner string, defaultRepo string) (string, string, string, error) {
	headValue, ok := pr["head"]
	if !ok {
		return "", "", "", fmt.Errorf("pull request payload missing head")
	}
	headPayload, ok := headValue.(map[string]any)
	if !ok {
		return "", "", "", fmt.Errorf("pull request payload has invalid head")
	}

	headSHA := strings.TrimSpace(asString(headPayload["sha"]))
	if headSHA == "" {
		return "", "", "", fmt.Errorf("pull request payload missing head.sha")
	}

	owner := strings.TrimSpace(defaultOwner)
	repo := strings.TrimSpace(defaultRepo)
	if repoPayload, ok := headPayload["repo"].(map[string]any); ok {
		if repoName := strings.TrimSpace(asString(repoPayload["name"])); repoName != "" {
			repo = repoName
		}
		if ownerPayload, ok := repoPayload["owner"].(map[string]any); ok {
			if login := strings.TrimSpace(asString(ownerPayload["login"])); login != "" {
				owner = login
			}
		}
	}

	if owner == "" || repo == "" {
		return "", "", "", fmt.Errorf("pull request payload missing head repo owner/name")
	}
	return headSHA, owner, repo, nil
}

func (c *githubClient) doJSON(ctx context.Context, method string, url string, payload any, out any) error {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewBuffer(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "zoekt-context-silo")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("github api %s %s failed (%d): %s", method, url, resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *githubClient) getPullRequest(ctx context.Context, owner string, repo string, prNumber int) (map[string]any, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/pulls/%d", owner, repo, prNumber)
	var payload map[string]any
	if err := c.doJSON(ctx, http.MethodGet, url, nil, &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (c *githubClient) listOrgRepos(ctx context.Context, owner string) ([]string, error) {
	collected := map[string]struct{}{}

	fetchPages := func(base string) error {
		for page := 1; page <= 100; page++ {
			url := fmt.Sprintf(base+"?type=all&sort=full_name&direction=asc&per_page=100&page=%d", page)
			var repos []map[string]any
			if err := c.doJSON(ctx, http.MethodGet, url, nil, &repos); err != nil {
				if strings.Contains(err.Error(), "(404)") {
					return os.ErrNotExist
				}
				return err
			}
			if len(repos) == 0 {
				break
			}
			for _, item := range repos {
				name := strings.TrimSpace(asString(item["name"]))
				if name != "" {
					collected[name] = struct{}{}
				}
			}
			if len(repos) < 100 {
				break
			}
		}
		return nil
	}

	orgURL := fmt.Sprintf("https://api.github.com/orgs/%s/repos", owner)
	if err := fetchPages(orgURL); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		userURL := fmt.Sprintf("https://api.github.com/users/%s/repos", owner)
		if err := fetchPages(userURL); err != nil {
			return nil, err
		}
	}

	result := make([]string, 0, len(collected))
	for name := range collected {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

func (c *githubClient) resolveRepoSHAAtOrBefore(ctx context.Context, owner string, repo string, anchor string) (string, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/commits?until=%s&per_page=1&page=1", owner, repo, anchor)
	var commits []map[string]any
	if err := c.doJSON(ctx, http.MethodGet, url, nil, &commits); err != nil {
		return "", err
	}
	if len(commits) > 0 {
		sha := strings.TrimSpace(asString(commits[0]["sha"]))
		if sha != "" {
			return sha, nil
		}
	}
	return "", fmt.Errorf("%w for %s/%s at %s", ErrNoCommitAtOrBeforeAnchor, owner, repo, anchor)
}
