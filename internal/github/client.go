package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/robinjoseph08/backlog/internal/dependencies"
	"github.com/robinjoseph08/backlog/internal/scheduler"
)

type Repository struct {
	Slug          string
	DefaultBranch string
}

type CompletionOutcome struct {
	PullRequest    string
	PRFound        bool
	Merged         bool
	IssueClosed    bool
	AutoMergeArmed bool
}

type IssueState struct {
	Open   bool
	Labels []string
}

type OwnedRunIssue struct {
	Number        int
	URL           string
	State         string
	ClosureReason string
	Labels        []string
}

type OwnedRunPullRequest struct {
	Number         int
	URL            string
	Branch         string
	Commit         string
	State          string
	Merged         bool
	AutoMergeArmed bool
	Comments       []string
}

type Client struct {
	Executable string
	Dir        string
}

// CandidateDiscoveryOperation identifies a Candidate discovery operation.
type CandidateDiscoveryOperation string

const (
	CandidateDiscoveryList    CandidateDiscoveryOperation = "list candidates"
	CandidateDiscoveryInspect CandidateDiscoveryOperation = "inspect candidate"
)

// CandidateDiscoveryError identifies the failed operation and optional issue
// while preserving the underlying GitHub error.
type CandidateDiscoveryError struct {
	Operation CandidateDiscoveryOperation
	Issue     int
	Err       error
	// Cause is a concise terminal cause suitable for presentation grouping.
	// Err retains the complete command and wrapping context for Diagnostics.
	Cause string
}

func (e *CandidateDiscoveryError) Error() string {
	if e.Issue > 0 {
		return fmt.Sprintf("%s #%d: %v", e.Operation, e.Issue, e.Err)
	}
	return fmt.Sprintf("%s: %v", e.Operation, e.Err)
}

func (e *CandidateDiscoveryError) Unwrap() error { return e.Err }

func (c Client) Repository(ctx context.Context) (Repository, error) {
	var response struct {
		NameWithOwner    string `json:"nameWithOwner"`
		DefaultBranchRef struct {
			Name string `json:"name"`
		} `json:"defaultBranchRef"`
	}
	if err := c.jsonCommand(ctx, &response, "repo", "view", "--json", "nameWithOwner,defaultBranchRef"); err != nil {
		return Repository{}, fmt.Errorf("discover repository: %w", err)
	}
	if response.NameWithOwner == "" || response.DefaultBranchRef.Name == "" {
		return Repository{}, fmt.Errorf("discover repository: gh returned incomplete repository metadata")
	}
	return Repository{Slug: response.NameWithOwner, DefaultBranch: response.DefaultBranchRef.Name}, nil
}

func (c Client) Candidates(ctx context.Context, repo string) ([]scheduler.Candidate, error) {
	var listed []struct {
		Number    int       `json:"number"`
		Title     string    `json:"title"`
		CreatedAt time.Time `json:"createdAt"`
		URL       string    `json:"url"`
	}
	if err := c.jsonCommand(ctx, &listed,
		"issue", "list", "--repo", repo, "--state", "open", "--label", "ready-for-agent",
		"--limit", "1000", "--json", "number,title,createdAt,url",
	); err != nil {
		return nil, newCandidateDiscoveryError(CandidateDiscoveryList, 0, err)
	}

	candidates := make([]scheduler.Candidate, 0, len(listed))
	for _, item := range listed {
		candidate, err := c.candidate(ctx, repo, item.Number)
		if err != nil {
			return nil, newCandidateDiscoveryError(CandidateDiscoveryInspect, item.Number, err)
		}
		candidates = append(candidates, candidate)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
			return candidates[i].Number < candidates[j].Number
		}
		return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
	})
	return candidates, nil
}

func (c Client) candidate(ctx context.Context, repo string, number int) (scheduler.Candidate, error) {
	var issue struct {
		Number    int       `json:"number"`
		Title     string    `json:"title"`
		Body      string    `json:"body"`
		State     string    `json:"state"`
		URL       string    `json:"url"`
		CreatedAt time.Time `json:"createdAt"`
	}
	if err := c.jsonCommand(ctx, &issue, "issue", "view", fmt.Sprint(number), "--repo", repo,
		"--json", "number,title,body,state,url,createdAt"); err != nil {
		return scheduler.Candidate{}, err
	}
	if !strings.EqualFold(issue.State, "open") {
		return scheduler.Candidate{}, fmt.Errorf("candidate is no longer open")
	}
	if issue.Number != number || issue.Number <= 0 {
		return scheduler.Candidate{}, fmt.Errorf("candidate identity mismatch: requested #%d, received #%d", number, issue.Number)
	}
	if strings.TrimSpace(issue.Title) == "" || strings.TrimSpace(issue.URL) == "" || issue.CreatedAt.IsZero() {
		return scheduler.Candidate{}, errors.New("candidate omitted required title, URL, or creation time")
	}

	blockers, err := c.nativeBlockers(ctx, repo, number)
	if err != nil {
		return scheduler.Candidate{}, fmt.Errorf("native blockers: %w", err)
	}
	comments, err := c.comments(ctx, repo, number)
	if err != nil {
		return scheduler.Candidate{}, fmt.Errorf("comments: %w", err)
	}
	entries := []string{issue.Body}
	for _, comment := range comments {
		entries = append(entries, comment.Body)
	}
	repositoryParts := strings.SplitN(repo, "/", 2)
	if len(repositoryParts) != 2 {
		return scheduler.Candidate{}, fmt.Errorf("invalid repository name %q", repo)
	}
	for _, reference := range dependencies.ParseForRepository(entries, repositoryParts[0], repositoryParts[1]) {
		blocker, open, err := c.resolveReference(ctx, repo, reference)
		if err != nil {
			return scheduler.Candidate{}, fmt.Errorf("resolve text blocker %s: %w", formatReference(reference), err)
		}
		if open {
			blockers = appendUniqueBlocker(blockers, blocker)
		}
	}
	return scheduler.Candidate{
		Number:    issue.Number,
		Title:     issue.Title,
		URL:       issue.URL,
		CreatedAt: issue.CreatedAt,
		Blockers:  blockers,
	}, nil
}

type issueComment struct {
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

func (c Client) comments(ctx context.Context, repo string, issue int) ([]issueComment, error) {
	var pages [][]issueComment
	endpoint := fmt.Sprintf("repos/%s/issues/%d/comments?per_page=100", repo, issue)
	if err := c.jsonCommand(ctx, &pages, "api",
		"-H", "Accept: application/vnd.github+json",
		"-H", "X-GitHub-Api-Version: 2026-03-10",
		endpoint, "--paginate", "--slurp",
	); err != nil {
		return nil, err
	}
	var comments []issueComment
	for _, page := range pages {
		comments = append(comments, page...)
	}
	sort.SliceStable(comments, func(i, j int) bool { return comments[i].CreatedAt.Before(comments[j].CreatedAt) })
	return comments, nil
}

func (c Client) nativeBlockers(ctx context.Context, repo string, issue int) ([]scheduler.Blocker, error) {
	var pages [][]struct {
		Number int    `json:"number"`
		Title  string `json:"title"`
		URL    string `json:"html_url"`
		State  string `json:"state"`
	}
	endpoint := fmt.Sprintf("repos/%s/issues/%d/dependencies/blocked_by?per_page=100", repo, issue)
	if err := c.jsonCommand(ctx, &pages, "api",
		"-H", "Accept: application/vnd.github+json",
		"-H", "X-GitHub-Api-Version: 2026-03-10",
		endpoint, "--paginate", "--slurp",
	); err != nil {
		return nil, err
	}
	blockers := make([]scheduler.Blocker, 0)
	for _, page := range pages {
		for _, dependency := range page {
			if !strings.EqualFold(dependency.State, "open") {
				continue
			}
			owner, name := repositoryFromIssueURL(dependency.URL)
			blockers = appendUniqueBlocker(blockers, scheduler.Blocker{
				Owner: owner, Repo: name, Number: dependency.Number, Title: dependency.Title, URL: dependency.URL,
			})
		}
	}
	return blockers, nil
}

func (c Client) resolveReference(ctx context.Context, currentRepo string, reference dependencies.Reference) (scheduler.Blocker, bool, error) {
	repo := currentRepo
	if reference.Owner != "" {
		repo = reference.Owner + "/" + reference.Repo
	}
	var issue struct {
		State string `json:"state"`
		Title string `json:"title"`
		URL   string `json:"url"`
	}
	if err := c.jsonCommand(ctx, &issue, "issue", "view", fmt.Sprint(reference.Number), "--repo", repo,
		"--json", "state,title,url"); err != nil {
		return scheduler.Blocker{}, false, err
	}
	parts := strings.SplitN(repo, "/", 2)
	blocker := scheduler.Blocker{Number: reference.Number, Title: issue.Title, URL: issue.URL}
	if len(parts) == 2 {
		blocker.Owner, blocker.Repo = parts[0], parts[1]
	}
	return blocker, strings.EqualFold(issue.State, "open"), nil
}

func (c Client) IssueState(ctx context.Context, repo string, issue int) (IssueState, error) {
	var response struct {
		Number int    `json:"number"`
		URL    string `json:"url"`
		State  string `json:"state"`
		Labels []struct {
			Name string `json:"name"`
		} `json:"labels"`
	}
	if err := c.jsonCommand(ctx, &response, "issue", "view", fmt.Sprint(issue), "--repo", repo, "--json", "number,url,state,labels"); err != nil {
		return IssueState{}, fmt.Errorf("inspect issue state and labels: %w", err)
	}
	if response.Number != issue || !resourceURLMatches(response.URL, repo, "issues", issue) ||
		(!strings.EqualFold(response.State, "open") && !strings.EqualFold(response.State, "closed")) {
		return IssueState{}, errors.New("inspect issue state and labels: gh returned incomplete or mismatched issue identity/state")
	}
	labels := make([]string, 0, len(response.Labels))
	for _, label := range response.Labels {
		if label.Name == "" {
			return IssueState{}, errors.New("inspect issue state and labels: GitHub returned an unnamed label")
		}
		for _, existing := range labels {
			if strings.EqualFold(existing, label.Name) {
				return IssueState{}, fmt.Errorf("inspect issue state and labels: GitHub returned duplicate label %q", label.Name)
			}
		}
		for _, managed := range []string{"in-progress", "ready-for-agent", "needs-triage", "needs-info", "ready-for-human", "wontfix"} {
			if label.Name != managed && strings.EqualFold(label.Name, managed) {
				return IssueState{}, fmt.Errorf("inspect issue state and labels: GitHub returned non-canonical managed label %q", label.Name)
			}
		}
		labels = append(labels, label.Name)
	}
	return IssueState{Open: strings.EqualFold(response.State, "open"), Labels: labels}, nil
}

// OwnedRunResources reads the issue and every pull request for the exact owned
// branch. Incomplete or mismatched identities fail closed.
func (c Client) OwnedRunResources(ctx context.Context, repo string, issueNumber int, branch string) (OwnedRunIssue, []OwnedRunPullRequest, error) {
	var issue struct {
		Number int             `json:"number"`
		URL    string          `json:"url"`
		State  string          `json:"state"`
		Labels json.RawMessage `json:"labels"`
	}
	if err := c.jsonCommand(ctx, &issue, "issue", "view", fmt.Sprint(issueNumber), "--repo", repo,
		"--json", "number,url,state,labels"); err != nil {
		return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run issue: %w", err)
	}
	if issue.Number != issueNumber || !resourceURLMatches(issue.URL, repo, "issues", issueNumber) ||
		(!strings.EqualFold(issue.State, "open") && !strings.EqualFold(issue.State, "closed")) {
		return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run issue: gh returned incomplete or unknown issue identity/state")
	}
	var labels []struct {
		Name string `json:"name"`
	}
	if len(issue.Labels) == 0 || string(issue.Labels) == "null" || json.Unmarshal(issue.Labels, &labels) != nil {
		return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run issue: gh returned unknown labels")
	}
	resultIssue := OwnedRunIssue{Number: issue.Number, URL: issue.URL, State: strings.ToLower(issue.State)}
	for _, label := range labels {
		if label.Name == "" {
			return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run issue: gh returned a label without identity")
		}
		resultIssue.Labels = append(resultIssue.Labels, label.Name)
	}

	if branch == "" {
		return resultIssue, nil, nil
	}
	parts := strings.SplitN(repo, "/", 2)
	if len(parts) != 2 || parts[0] == "" {
		return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run pull requests: invalid repository %q", repo)
	}
	var pulls []struct {
		Number           int             `json:"number"`
		URL              string          `json:"url"`
		State            string          `json:"state"`
		MergedAt         json.RawMessage `json:"mergedAt"`
		AutoMergeRequest json.RawMessage `json:"autoMergeRequest"`
		IsDraft          *bool           `json:"isDraft"`
		HeadRefName      string          `json:"headRefName"`
		HeadRefOID       string          `json:"headRefOid"`
		HeadOwner        struct {
			Login string `json:"login"`
		} `json:"headRepositoryOwner"`
		HeadRepository struct {
			NameWithOwner string `json:"nameWithOwner"`
		} `json:"headRepository"`
	}
	var pullsJSON json.RawMessage
	if err := c.jsonCommand(ctx, &pullsJSON, "pr", "list", "--repo", repo, "--state", "all", "--head", branch, "--limit", "1000",
		"--json", "number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRefOid,headRepositoryOwner,headRepository"); err != nil {
		return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run pull requests: %w", err)
	}
	if len(pullsJSON) == 0 || string(pullsJSON) == "null" || json.Unmarshal(pullsJSON, &pulls) != nil {
		return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run pull requests: gh returned an unknown pull request list")
	}
	if len(pulls) == 1000 {
		return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run pull requests: result reached the inspection limit; completeness is unknown")
	}
	resultPulls := make([]OwnedRunPullRequest, 0, len(pulls))
	for _, pull := range pulls {
		if pull.Number <= 0 || !resourceURLMatches(pull.URL, repo, "pull", pull.Number) || pull.HeadRefName != branch || !validCommitOID(pull.HeadRefOID) ||
			!strings.EqualFold(pull.HeadOwner.Login, parts[0]) || !strings.EqualFold(pull.HeadRepository.NameWithOwner, repo) {
			return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run pull requests: gh returned incomplete or mismatched pull request identity")
		}
		state := strings.ToLower(pull.State)
		if state != "open" && state != "closed" && state != "merged" {
			return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run pull request #%d: unknown state %q", pull.Number, pull.State)
		}
		merged, err := inspectedMergedState(pull.MergedAt)
		if err != nil {
			return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run pull request #%d: %w", pull.Number, err)
		}
		autoMergeArmed, err := inspectedAutoMergeState(pull.AutoMergeRequest, pull.IsDraft)
		if err != nil {
			return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run pull request #%d: %w", pull.Number, err)
		}
		merged = merged || state == "merged"
		if merged {
			state = "merged"
		}
		result := OwnedRunPullRequest{
			Number: pull.Number, URL: pull.URL, Branch: pull.HeadRefName, Commit: pull.HeadRefOID,
			State: state, Merged: merged, AutoMergeArmed: autoMergeArmed,
		}
		if state == "open" {
			comments, err := c.ownedPullRequestComments(ctx, repo, pull.Number)
			if err != nil {
				return OwnedRunIssue{}, nil, fmt.Errorf("inspect owned Run pull request #%d comments: %w", pull.Number, err)
			}
			result.Comments = comments
		}
		resultPulls = append(resultPulls, result)
	}
	return resultIssue, resultPulls, nil
}

func (c Client) ownedPullRequestComments(ctx context.Context, repo string, number int) ([]string, error) {
	var pagesJSON json.RawMessage
	endpoint := fmt.Sprintf("repos/%s/issues/%d/comments?per_page=100", repo, number)
	if err := c.jsonCommand(ctx, &pagesJSON, "api",
		"-H", "Accept: application/vnd.github+json",
		"-H", "X-GitHub-Api-Version: 2026-03-10",
		endpoint, "--paginate", "--slurp",
	); err != nil {
		return nil, err
	}
	var pages [][]struct {
		Body *string `json:"body"`
	}
	if len(pagesJSON) == 0 || string(pagesJSON) == "null" || json.Unmarshal(pagesJSON, &pages) != nil {
		return nil, errors.New("gh returned an unknown comment list")
	}
	if len(pages) == 0 {
		return nil, errors.New("gh returned an unknown comment list")
	}
	var comments []string
	for _, page := range pages {
		if page == nil {
			return nil, errors.New("gh returned an unknown comment page")
		}
		for _, comment := range page {
			if comment.Body == nil {
				return nil, errors.New("gh returned a comment without a body")
			}
			comments = append(comments, *comment.Body)
		}
	}
	return comments, nil
}

// IssueClosureState is a verified GitHub issue closure snapshot. Reason is
// normalized when supported and otherwise retains a string value when one was
// available. Callers decide whether closure-reason support is required for the
// outcome they are planning.
type IssueClosureState struct {
	Open   bool
	Reason string
}

type issueClosureResponse struct {
	Number      int             `json:"number"`
	URL         string          `json:"url"`
	State       string          `json:"state"`
	StateReason json.RawMessage `json:"stateReason"`
}

// IssueClosure verifies exact issue identity and open/closed state without
// making closure-reason support a prerequisite for observing that state.
func (c Client) IssueClosure(ctx context.Context, repo string, issueNumber int) (IssueClosureState, error) {
	issue, err := c.inspectIssueClosure(ctx, repo, issueNumber)
	if err != nil {
		return IssueClosureState{}, err
	}
	if strings.EqualFold(issue.State, "open") {
		if _, err := inspectedClosureReason(issue.State, issue.StateReason); err != nil {
			return IssueClosureState{}, fmt.Errorf("inspect issue closure: %w", err)
		}
		return IssueClosureState{Open: true}, nil
	}
	var reason string
	if json.Unmarshal(issue.StateReason, &reason) == nil {
		switch strings.ToUpper(reason) {
		case "COMPLETED":
			reason = "completed"
		case "NOT_PLANNED":
			reason = "not-planned"
		}
	}
	return IssueClosureState{Reason: reason}, nil
}

// IssueClosureReason verifies a closed issue and returns one of GitHub's
// supported closure reasons. Open, unavailable, and unknown state fail closed.
func (c Client) IssueClosureReason(ctx context.Context, repo string, issueNumber int) (string, error) {
	issue, err := c.inspectIssueClosure(ctx, repo, issueNumber)
	if err != nil {
		return "", err
	}
	reason, err := inspectedClosureReason(issue.State, issue.StateReason)
	if err != nil {
		return "", fmt.Errorf("inspect issue closure: %w", err)
	}
	return reason, nil
}

func (c Client) inspectIssueClosure(ctx context.Context, repo string, issueNumber int) (issueClosureResponse, error) {
	var issue issueClosureResponse
	if err := c.jsonCommand(ctx, &issue, "issue", "view", fmt.Sprint(issueNumber), "--repo", repo,
		"--json", "number,url,state,stateReason"); err != nil {
		return issue, fmt.Errorf("inspect issue closure: %w", err)
	}
	if issue.Number != issueNumber || !resourceURLMatches(issue.URL, repo, "issues", issueNumber) ||
		(!strings.EqualFold(issue.State, "open") && !strings.EqualFold(issue.State, "closed")) {
		return issue, errors.New("inspect issue closure: gh returned incomplete or mismatched issue identity/state")
	}
	return issue, nil
}

func inspectedClosureReason(issueState string, raw json.RawMessage) (string, error) {
	if strings.EqualFold(issueState, "open") {
		if len(raw) == 0 || string(raw) == "null" {
			return "", nil
		}
		return "", errors.New("open issue has an unsupported closure reason")
	}
	if len(raw) == 0 || string(raw) == "null" {
		return "", errors.New("closed issue has no supported closure reason")
	}
	var reason string
	if err := json.Unmarshal(raw, &reason); err != nil {
		return "", errors.New("closed issue has an unknown closure reason")
	}
	switch strings.ToUpper(reason) {
	case "COMPLETED":
		return "completed", nil
	case "NOT_PLANNED":
		return "not-planned", nil
	default:
		return "", fmt.Errorf("closed issue has unsupported closure reason %q", reason)
	}
}

func validCommitOID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdefABCDEF", character) {
			return false
		}
	}
	return strings.Trim(value, "0") != ""
}

func resourceURLMatches(rawURL, repo, resource string, number int) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || !asciiEqualFold(parsed.Host, "github.com") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	expectedPath := fmt.Sprintf("/%s/%s/%d", repo, resource, number)
	return asciiEqualFold(strings.TrimSuffix(parsed.Path, "/"), expectedPath)
}

func asciiEqualFold(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range len(left) {
		leftByte, rightByte := left[index], right[index]
		if leftByte >= 'A' && leftByte <= 'Z' {
			leftByte += 'a' - 'A'
		}
		if rightByte >= 'A' && rightByte <= 'Z' {
			rightByte += 'a' - 'A'
		}
		if leftByte != rightByte {
			return false
		}
	}
	return true
}

func inspectedMergedState(raw json.RawMessage) (bool, error) {
	if len(raw) == 0 {
		return false, errors.New("gh returned unknown merged state")
	}
	if string(raw) == "null" {
		return false, nil
	}
	var mergedAt time.Time
	if err := json.Unmarshal(raw, &mergedAt); err != nil || mergedAt.IsZero() {
		return false, errors.New("gh returned unknown merged state")
	}
	return true, nil
}

func inspectedAutoMergeState(raw json.RawMessage, isDraft *bool) (bool, error) {
	if len(raw) == 0 || isDraft == nil {
		return false, errors.New("gh returned unknown auto-merge state")
	}
	if string(raw) == "null" {
		return false, nil
	}
	var request map[string]json.RawMessage
	if err := json.Unmarshal(raw, &request); err != nil || request == nil || *isDraft {
		return false, errors.New("gh returned unknown auto-merge state")
	}
	return true, nil
}

// DisablePullRequestAutoMerge disarms one freshly verified pull request.
func (c Client) DisablePullRequestAutoMerge(ctx context.Context, repo string, number int) error {
	return c.command(ctx, "pr", "merge", fmt.Sprint(number), "--repo", repo, "--disable-auto")
}

// CommentOnPullRequest records the explanation for abandoning a Run.
func (c Client) CommentOnPullRequest(ctx context.Context, repo string, number int, body string) error {
	return c.command(ctx, "pr", "comment", fmt.Sprint(number), "--repo", repo, "--body", body)
}

// ClosePullRequest closes one freshly verified unmerged pull request.
func (c Client) ClosePullRequest(ctx context.Context, repo string, number int) error {
	return c.command(ctx, "pr", "close", fmt.Sprint(number), "--repo", repo)
}

// AddIssueLabel adds one managed label without replacing unrelated labels.
func (c Client) AddIssueLabel(ctx context.Context, repo string, issue int, label string) error {
	return c.command(ctx, "issue", "edit", fmt.Sprint(issue), "--repo", repo, "--add-label", label)
}

// RemoveIssueLabel removes one managed label without replacing unrelated labels.
func (c Client) RemoveIssueLabel(ctx context.Context, repo string, issue int, label string) error {
	return c.command(ctx, "issue", "edit", fmt.Sprint(issue), "--repo", repo, "--remove-label", label)
}

func (c Client) Completion(ctx context.Context, repo string, issue int, branch string) (CompletionOutcome, error) {
	parts := strings.SplitN(repo, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || branch == "" {
		return CompletionOutcome{}, fmt.Errorf("find pull request: invalid repository or branch identity")
	}
	var pulls []struct {
		Number           int             `json:"number"`
		URL              string          `json:"url"`
		State            string          `json:"state"`
		MergedAt         json.RawMessage `json:"mergedAt"`
		AutoMergeRequest json.RawMessage `json:"autoMergeRequest"`
		IsDraft          *bool           `json:"isDraft"`
		HeadRefName      string          `json:"headRefName"`
		HeadOwner        struct {
			Login string `json:"login"`
		} `json:"headRepositoryOwner"`
		HeadRepository struct {
			NameWithOwner string `json:"nameWithOwner"`
		} `json:"headRepository"`
	}
	var pullsJSON json.RawMessage
	if err := c.jsonCommand(ctx, &pullsJSON, "pr", "list", "--repo", repo, "--state", "all", "--head", branch, "--limit", "1000",
		"--json", "number,url,state,mergedAt,autoMergeRequest,isDraft,headRefName,headRepositoryOwner,headRepository"); err != nil {
		return CompletionOutcome{}, fmt.Errorf("find pull request: %w", err)
	}
	if len(pullsJSON) == 0 || string(pullsJSON) == "null" || json.Unmarshal(pullsJSON, &pulls) != nil {
		return CompletionOutcome{}, errors.New("find pull request: gh returned an unknown pull request list")
	}
	if len(pulls) == 1000 {
		return CompletionOutcome{}, errors.New("find pull request: result reached the inspection limit; completeness is unknown")
	}
	for _, pull := range pulls {
		if pull.Number <= 0 || !resourceURLMatches(pull.URL, repo, "pull", pull.Number) || pull.HeadRefName != branch ||
			!strings.EqualFold(pull.HeadOwner.Login, parts[0]) || !strings.EqualFold(pull.HeadRepository.NameWithOwner, repo) {
			return CompletionOutcome{}, errors.New("find pull request: gh returned incomplete or mismatched pull request identity")
		}
		if !strings.EqualFold(pull.State, "open") && !strings.EqualFold(pull.State, "closed") && !strings.EqualFold(pull.State, "merged") {
			return CompletionOutcome{}, fmt.Errorf("find pull request #%d: unknown state %q", pull.Number, pull.State)
		}
		if _, err := inspectedMergedState(pull.MergedAt); err != nil {
			return CompletionOutcome{}, fmt.Errorf("find pull request #%d: %w", pull.Number, err)
		}
		if _, err := inspectedAutoMergeState(pull.AutoMergeRequest, pull.IsDraft); err != nil {
			return CompletionOutcome{}, fmt.Errorf("find pull request #%d: %w", pull.Number, err)
		}
	}
	outcome := CompletionOutcome{}
	if len(pulls) > 0 {
		sort.SliceStable(pulls, func(i, j int) bool { return pulls[i].Number > pulls[j].Number })
		merged, _ := inspectedMergedState(pulls[0].MergedAt)
		autoMergeArmed, _ := inspectedAutoMergeState(pulls[0].AutoMergeRequest, pulls[0].IsDraft)
		outcome.PRFound = true
		outcome.PullRequest = pulls[0].URL
		outcome.Merged = merged || strings.EqualFold(pulls[0].State, "merged")
		outcome.AutoMergeArmed = autoMergeArmed
	}
	var issueState struct {
		Number int    `json:"number"`
		URL    string `json:"url"`
		State  string `json:"state"`
	}
	if err := c.jsonCommand(ctx, &issueState, "issue", "view", fmt.Sprint(issue), "--repo", repo,
		"--json", "number,state,title,url"); err != nil {
		return CompletionOutcome{}, fmt.Errorf("inspect issue: %w", err)
	}
	if issueState.Number != issue || !resourceURLMatches(issueState.URL, repo, "issues", issue) ||
		(!strings.EqualFold(issueState.State, "open") && !strings.EqualFold(issueState.State, "closed")) {
		return CompletionOutcome{}, errors.New("inspect issue: gh returned incomplete or mismatched issue identity/state")
	}
	outcome.IssueClosed = strings.EqualFold(issueState.State, "closed")
	return outcome, nil
}

func (c Client) command(ctx context.Context, args ...string) error {
	executable := c.Executable
	if executable == "" {
		executable = "gh"
	}
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = c.Dir
	output, err := command.CombinedOutput()
	if err == nil {
		return nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return fmt.Errorf("gh %s: %w", strings.Join(args, " "), contextErr)
	}
	message := strings.TrimSpace(string(output))
	if message != "" {
		return fmt.Errorf("gh %s: %s", strings.Join(args, " "), message)
	}
	return fmt.Errorf("gh %s: %w", strings.Join(args, " "), err)
}

type commandError struct {
	command string
	detail  []byte
	err     error
}

func (e *commandError) Error() string {
	if len(e.detail) > 0 {
		return fmt.Sprintf("gh %s: %s", e.command, e.detail)
	}
	return fmt.Sprintf("gh %s: %v", e.command, e.err)
}

func (e *commandError) Unwrap() error { return e.err }

func newCommandError(command string, err error) *commandError {
	failure := &commandError{command: command, err: err}
	exitError, ok := err.(*exec.ExitError)
	if !ok || len(exitError.Stderr) == 0 {
		return failure
	}
	// Transfer the command-owned stderr allocation into the diagnostic. Clearing
	// ExitError.Stderr preserves exit-status semantics without retaining the same
	// evidence a second time. The full evidence remains invocation-local and is
	// released when its bounded Runner diagnostic record is evicted.
	failure.detail = bytes.TrimSpace(exitError.Stderr)
	exitError.Stderr = nil
	return failure
}

func newCandidateDiscoveryError(operation CandidateDiscoveryOperation, issue int, err error) *CandidateDiscoveryError {
	return &CandidateDiscoveryError{Operation: operation, Issue: issue, Err: err, Cause: conciseCandidateDiscoveryCause(err)}
}

func conciseCandidateDiscoveryCause(err error) string {
	var command *commandError
	if errors.As(err, &command) {
		if len(command.detail) > 0 {
			return boundedCandidateDiscoveryCause(string(command.detail))
		}
		if command.err != nil {
			return boundedCandidateDiscoveryCause(command.err.Error())
		}
	}
	cause := err
	for errors.Unwrap(cause) != nil {
		cause = errors.Unwrap(cause)
	}
	if cause == nil {
		return "unknown error"
	}
	return boundedCandidateDiscoveryCause(cause.Error())
}

func boundedCandidateDiscoveryCause(cause string) string {
	cause = strings.TrimSpace(cause)
	if line, _, found := strings.Cut(cause, "\n"); found {
		cause = strings.TrimSpace(line)
	}
	const limit = 200
	runes := []rune(cause)
	if len(runes) > limit {
		cause = string(runes[:limit-3]) + "..."
	}
	if cause == "" {
		return "unknown error"
	}
	return cause
}

func (c Client) jsonCommand(ctx context.Context, target any, args ...string) error {
	executable := c.Executable
	if executable == "" {
		executable = "gh"
	}
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = c.Dir
	output, err := command.Output()
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("gh %s: %w", strings.Join(args, " "), contextErr)
		}
		return newCommandError(strings.Join(args, " "), err)
	}
	if err := rejectDuplicateJSONFields(output); err != nil {
		return fmt.Errorf("decode gh %s output: %w", strings.Join(args, " "), err)
	}
	if err := json.Unmarshal(output, target); err != nil {
		return fmt.Errorf("decode gh %s output: %w", strings.Join(args, " "), err)
	}
	return nil
}

var canonicalGitHubJSONFields = []string{
	"nameWithOwner", "name", "defaultBranchRef", "number", "title", "createdAt", "url", "body", "state",
	"created_at", "html_url", "labels", "stateReason", "mergedAt", "autoMergeRequest", "isDraft", "headRefName",
	"login", "headRepositoryOwner", "headRepository",
}

func rejectDuplicateJSONFields(output []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(output))
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		fields := make(map[string]struct{})
		for decoder.More() {
			fieldToken, err := decoder.Token()
			if err != nil {
				return err
			}
			field, ok := fieldToken.(string)
			if !ok {
				return errors.New("invalid JSON object field")
			}
			for existing := range fields {
				if strings.EqualFold(existing, field) {
					return fmt.Errorf("duplicate JSON field %q", field)
				}
			}
			for _, canonical := range canonicalGitHubJSONFields {
				if field != canonical && strings.EqualFold(field, canonical) {
					return fmt.Errorf("non-canonical JSON field %q", field)
				}
			}
			fields[field] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return errors.New("invalid JSON object closing delimiter")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return errors.New("invalid JSON array closing delimiter")
		}
	default:
		return errors.New("invalid JSON opening delimiter")
	}
	return nil
}

func appendUniqueBlocker(blockers []scheduler.Blocker, blocker scheduler.Blocker) []scheduler.Blocker {
	for _, existing := range blockers {
		if strings.EqualFold(existing.Owner, blocker.Owner) && strings.EqualFold(existing.Repo, blocker.Repo) && existing.Number == blocker.Number {
			return blockers
		}
	}
	return append(blockers, blocker)
}

func repositoryFromIssueURL(raw string) (string, string) {
	parsed, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(parsed.Host, "github.com") {
		return "", ""
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 4 || parts[2] != "issues" {
		return "", ""
	}
	return parts[0], parts[1]
}

func formatReference(reference dependencies.Reference) string {
	if reference.Owner != "" {
		return fmt.Sprintf("%s/%s#%d", reference.Owner, reference.Repo, reference.Number)
	}
	return fmt.Sprintf("#%d", reference.Number)
}
