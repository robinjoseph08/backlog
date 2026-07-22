package dependencies

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Reference struct {
	Owner  string `json:"owner,omitempty"`
	Repo   string `json:"repo,omitempty"`
	Number int    `json:"number"`
}

const referencePattern = `(?:https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/issues/[0-9]+|[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+#[0-9]+|#[0-9]+)`
const referenceListPattern = referencePattern + `(?:\s*(?:,\s*(?:and\s+)?|and\s+)` + referencePattern + `)*`

var (
	positivePatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\bblocked\s+by\s+(` + referenceListPattern + `)`),
		regexp.MustCompile(`(?i)\bdepends\s+on\s+(` + referenceListPattern + `)`),
		regexp.MustCompile(`(?i)\bwaiting\s+for\s+(` + referenceListPattern + `)`),
	}
	negativePatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\bno\s+longer\s+blocked\s+by\s+(` + referenceListPattern + `)`),
		regexp.MustCompile(`(?i)\bdependency\s+on\s+(` + referenceListPattern + `)\s+(?:(?:was|is)\s+)?(?:removed|resolved)\b`),
		regexp.MustCompile(`(?i)\bblocker\s+(` + referenceListPattern + `)\s+(?:(?:was|is)\s+)?resolved\b`),
		regexp.MustCompile(`(?i)(` + referencePattern + `)\s+no\s+longer\s+blocks\b`),
	}
	referenceFinder = regexp.MustCompile(`(?i)` + referencePattern)
	urlReference    = regexp.MustCompile(`(?i)^https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/issues/([0-9]+)$`)
	repoReference   = regexp.MustCompile(`(?i)^([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)#([0-9]+)$`)
	localReference  = regexp.MustCompile(`^#([0-9]+)$`)
)

// Parse returns the text-derived dependencies still active after applying each
// body or comment in chronological order. Within one entry, explicit removal
// statements win over positive statements for the same reference.
func Parse(entries []string) []Reference {
	return parse(entries, "", "")
}

// ParseForRepository treats local issue numbers as equivalent to explicit
// references and URLs for the current repository.
func ParseForRepository(entries []string, owner, repo string) []Reference {
	return parse(entries, owner, repo)
}

func parse(entries []string, defaultOwner, defaultRepo string) []Reference {
	active := make(map[string]bool)
	seen := make(map[string]Reference)
	order := make(map[string]int)
	nextOrder := 0

	for _, entry := range entries {
		for _, pattern := range positivePatterns {
			for _, match := range pattern.FindAllStringSubmatch(entry, -1) {
				for _, raw := range referenceFinder.FindAllString(match[1], -1) {
					ref, ok := parseReference(raw)
					if !ok {
						continue
					}
					key := ref.key(defaultOwner, defaultRepo)
					if _, exists := seen[key]; !exists {
						seen[key] = ref
						order[key] = nextOrder
						nextOrder++
					}
					active[key] = true
				}
			}
		}
		for _, pattern := range negativePatterns {
			for _, match := range pattern.FindAllStringSubmatch(entry, -1) {
				for _, raw := range referenceFinder.FindAllString(match[1], -1) {
					ref, ok := parseReference(raw)
					if ok {
						active[ref.key(defaultOwner, defaultRepo)] = false
					}
				}
			}
		}
	}

	keys := make([]string, 0, len(active))
	for key, isActive := range active {
		if isActive {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return order[keys[i]] < order[keys[j]] })
	refs := make([]Reference, 0, len(keys))
	for _, key := range keys {
		refs = append(refs, seen[key])
	}
	return refs
}

func parseReference(raw string) (Reference, bool) {
	if match := urlReference.FindStringSubmatch(raw); match != nil {
		number, err := strconv.Atoi(match[3])
		return Reference{Owner: match[1], Repo: match[2], Number: number}, err == nil
	}
	if match := repoReference.FindStringSubmatch(raw); match != nil {
		number, err := strconv.Atoi(match[3])
		return Reference{Owner: match[1], Repo: match[2], Number: number}, err == nil
	}
	if match := localReference.FindStringSubmatch(raw); match != nil {
		number, err := strconv.Atoi(match[1])
		return Reference{Number: number}, err == nil
	}
	return Reference{}, false
}

func (r Reference) key(defaultOwner, defaultRepo string) string {
	owner, repo := r.Owner, r.Repo
	if owner == "" {
		owner, repo = defaultOwner, defaultRepo
	}
	return fmt.Sprintf("%s/%s#%d", strings.ToLower(owner), strings.ToLower(repo), r.Number)
}
