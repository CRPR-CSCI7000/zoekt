package json

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sourcegraph/zoekt"
	"github.com/sourcegraph/zoekt/query"
)

// defaultTimeout is the maximum amount of time a search request should
// take. This is the same default used by Sourcegraph.
const defaultTimeout = 20 * time.Second

type ServerOptions struct {
	RequireContext          bool
	ContextResolver         ContextScopeResolver
	ContextSearcherResolver ContextSearcherResolver
}

type ContextSearcherResolver interface {
	Streamer(contextID string) (zoekt.Streamer, error)
}

func JSONServer(searcher zoekt.Searcher, opts ...ServerOptions) http.Handler {
	var options ServerOptions
	if len(opts) > 0 {
		options = opts[0]
	}
	s := jsonSearcher{
		Searcher: searcher,
		options:  options,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/search", s.jsonSearch)
	mux.HandleFunc("/list", s.jsonList)
	mux.HandleFunc("/list-all", s.jsonListAll)
	return mux
}

type jsonSearcher struct {
	Searcher zoekt.Searcher
	options  ServerOptions
}

type jsonSearchArgs struct {
	Q         string
	RepoIDs   *[]uint32
	Opts      *zoekt.SearchOptions
	ContextID string `json:"context_id"`
}

type jsonSearchReply struct {
	Result *zoekt.SearchResult
}

type jsonListArgs struct {
	Q         string
	Opts      *zoekt.ListOptions
	ContextID string `json:"context_id"`
}

type jsonListReply struct {
	List *zoekt.RepoList
}

type jsonListAllArgs struct {
	ContextID string `json:"context_id"`
}

func (s *jsonSearcher) jsonSearch(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	w.Header().Add("Content-Type", "application/json")

	if req.Method != "POST" {
		jsonError(w, http.StatusMethodNotAllowed, "Only POST is supported")
		return
	}

	searchArgs := jsonSearchArgs{}
	err := json.NewDecoder(req.Body).Decode(&searchArgs)
	if err != nil {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}
	if searchArgs.Q == "" {
		jsonError(w, http.StatusBadRequest, "missing query")
		return
	}
	if searchArgs.Opts == nil {
		searchArgs.Opts = &zoekt.SearchOptions{}
	}

	q, err := query.Parse(searchArgs.Q)
	if err != nil {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}

	if searchArgs.RepoIDs != nil {
		q = query.NewAnd(q, query.NewRepoIDs(*searchArgs.RepoIDs...))
	}

	if s.options.RequireContext {
		scopedQuery, statusCode, err := s.applyRequiredContext(q, searchArgs.ContextID)
		if err != nil {
			jsonError(w, statusCode, err.Error())
			return
		}
		q = scopedQuery
	}
	activeSearcher, statusCode, err := s.searcherForContext(searchArgs.ContextID)
	if err != nil {
		jsonError(w, statusCode, err.Error())
		return
	}

	// Set a timeout if the user hasn't specified one.
	if searchArgs.Opts.MaxWallTime == 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
	}

	if err := CalculateDefaultSearchLimits(ctx, q, activeSearcher, searchArgs.Opts); err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}

	searchResult, err := activeSearcher.Search(ctx, q, searchArgs.Opts)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}

	err = json.NewEncoder(w).Encode(jsonSearchReply{searchResult})
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
}

func jsonError(w http.ResponseWriter, statusCode int, err string) {
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(struct{ Error string }{Error: err})
}

// Calculates and sets heuristic defaults on opts for various upper bounds on
// the number of matches when searching, if none are already specified. The
// defaults are derived from opts.MaxDocDisplayCount, so if none is set, there
// is no calculation to do.
func CalculateDefaultSearchLimits(ctx context.Context,
	q query.Q,
	searcher zoekt.Searcher,
	opts *zoekt.SearchOptions,
) error {
	if opts.MaxDocDisplayCount == 0 || opts.ShardMaxMatchCount != 0 {
		return nil
	}

	maxResultDocs := opts.MaxDocDisplayCount
	// This is a special mode of Search that _only_ calculates ShardFilesConsidered and bails ASAP.
	if result, err := searcher.Search(ctx, q, &zoekt.SearchOptions{EstimateDocCount: true}); err != nil {
		return err
	} else if numdocs := result.ShardFilesConsidered; numdocs > 10000 {
		// If the search touches many shards and many files, we
		// have to limit the number of matches.  This setting
		// is based on the number of documents eligible after
		// considering reponames, so large repos (both
		// android, chromium are about 500k files) aren't
		// covered fairly.

		// 10k docs, 50 maxResultDocs -> max match = (250 + 250 / 10)
		opts.ShardMaxMatchCount = maxResultDocs*5 + (5*maxResultDocs)/(numdocs/1000)
	} else {
		// Virtually no limits for a small corpus.
		n := numdocs + maxResultDocs*100
		opts.ShardMaxMatchCount = n
		opts.TotalMaxMatchCount = n
	}

	return nil
}

func (s *jsonSearcher) jsonList(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	if req.Method != "POST" {
		jsonError(w, http.StatusMethodNotAllowed, "Only POST is supported")
		return
	}

	listArgs := jsonListArgs{}
	err := json.NewDecoder(req.Body).Decode(&listArgs)
	if err != nil {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}

	query, err := query.Parse(listArgs.Q)
	if err != nil {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}

	if s.options.RequireContext {
		scopedQuery, statusCode, err := s.applyRequiredContext(query, listArgs.ContextID)
		if err != nil {
			jsonError(w, statusCode, err.Error())
			return
		}
		query = scopedQuery
	}
	activeSearcher, statusCode, err := s.searcherForContext(listArgs.ContextID)
	if err != nil {
		jsonError(w, statusCode, err.Error())
		return
	}

	listResult, err := activeSearcher.List(req.Context(), query, listArgs.Opts)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}

	err = json.NewEncoder(w).Encode(jsonListReply{listResult})
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
}

func (s *jsonSearcher) jsonListAll(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	if req.Method != "POST" {
		jsonError(w, http.StatusMethodNotAllowed, "Only POST is supported")
		return
	}

	listAllArgs := jsonListAllArgs{}
	if err := json.NewDecoder(req.Body).Decode(&listAllArgs); err != nil && err != io.EOF {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}

	activeSearcher := zoekt.Searcher(s.Searcher)
	if contextID := strings.TrimSpace(listAllArgs.ContextID); contextID != "" {
		if s.options.ContextSearcherResolver == nil {
			jsonError(w, http.StatusInternalServerError, "required context mode is enabled without searcher resolver")
			return
		}
		streamer, err := s.options.ContextSearcherResolver.Streamer(contextID)
		if err != nil {
			switch {
			case errors.Is(err, ErrMissingContextID),
				errors.Is(err, ErrUnknownContextID),
				errors.Is(err, ErrContextNotReady),
				errors.Is(err, ErrEmptyContext):
				jsonError(w, http.StatusBadRequest, err.Error())
			default:
				jsonError(w, http.StatusInternalServerError, err.Error())
			}
			return
		}
		activeSearcher = streamer
	}

	listResult, err := activeSearcher.List(req.Context(), &query.Const{Value: true}, nil)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := json.NewEncoder(w).Encode(jsonListReply{List: listResult}); err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
}

func (s *jsonSearcher) searcherForContext(contextIDRaw string) (zoekt.Searcher, int, error) {
	if !s.options.RequireContext {
		return s.Searcher, http.StatusOK, nil
	}
	contextID := strings.TrimSpace(contextIDRaw)
	if contextID == "" {
		return nil, http.StatusBadRequest, ErrMissingContextID
	}
	if s.options.ContextSearcherResolver == nil {
		return nil, http.StatusInternalServerError, errors.New("required context mode is enabled without searcher resolver")
	}
	streamer, err := s.options.ContextSearcherResolver.Streamer(contextID)
	if err != nil {
		switch {
		case errors.Is(err, ErrMissingContextID),
			errors.Is(err, ErrUnknownContextID),
			errors.Is(err, ErrContextNotReady),
			errors.Is(err, ErrEmptyContext):
			return nil, http.StatusBadRequest, err
		default:
			return nil, http.StatusInternalServerError, err
		}
	}
	return streamer, http.StatusOK, nil
}

func (s *jsonSearcher) applyRequiredContext(base query.Q, contextIDRaw string) (query.Q, int, error) {
	contextID := strings.TrimSpace(contextIDRaw)
	if contextID == "" {
		return nil, http.StatusBadRequest, ErrMissingContextID
	}
	if s.options.ContextResolver == nil {
		return nil, http.StatusInternalServerError, errors.New("required context mode is enabled without resolver")
	}

	scope, err := s.options.ContextResolver.Resolve(contextID)
	if err != nil {
		switch {
		case errors.Is(err, ErrMissingContextID):
			return nil, http.StatusBadRequest, err
		case errors.Is(err, ErrUnknownContextID), errors.Is(err, ErrContextNotReady), errors.Is(err, ErrEmptyContext):
			return nil, http.StatusBadRequest, err
		default:
			return nil, http.StatusInternalServerError, err
		}
	}

	scopedQuery, err := ApplyScopeToQuery(base, scope)
	if err != nil {
		if errors.Is(err, ErrEmptyContext) {
			return nil, http.StatusBadRequest, err
		}
		return nil, http.StatusInternalServerError, err
	}
	return scopedQuery, http.StatusOK, nil
}
