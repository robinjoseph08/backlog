// Package initialprompt compiles and renders the strict templates used for
// fresh Worker prompts.
package initialprompt

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const DefaultTemplate = "/skill:afk {{issue_number}}"

type Values struct {
	IssueNumber   int
	IssueTitle    string
	IssueURL      string
	Repository    string
	DefaultBranch string
	RunID         string
	Branch        string
	Worktree      string
}

type Digest string

func Sum(rendered string) Digest {
	digest := sha256.Sum256([]byte(rendered))
	return Digest(hex.EncodeToString(digest[:]))
}

func (digest Digest) Valid() bool {
	decoded, err := hex.DecodeString(string(digest))
	return err == nil && len(decoded) == sha256.Size
}

func (digest Digest) Matches(content string) bool {
	return strings.EqualFold(string(digest), string(Sum(content)))
}

type part struct {
	literal string
	name    string
}

// Template is a validated prompt template. Render performs one substitution
// pass, so placeholder-like text introduced by a value remains literal.
type Template struct {
	parts []part
}

var placeholderName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

var builtins = map[string]func(Values) string{
	"issue_number":   func(values Values) string { return strconv.Itoa(values.IssueNumber) },
	"issue_title":    func(values Values) string { return values.IssueTitle },
	"issue_url":      func(values Values) string { return values.IssueURL },
	"repository":     func(values Values) string { return values.Repository },
	"default_branch": func(values Values) string { return values.DefaultBranch },
	"run_id":         func(values Values) string { return values.RunID },
	"branch":         func(values Values) string { return values.Branch },
	"worktree":       func(values Values) string { return values.Worktree },
}

// Compile validates all placeholder syntax and names without rendering.
func Compile(source string) (Template, error) {
	if source == "" {
		return Template{}, errors.New("prompt template is empty")
	}
	var parts []part
	remaining := source
	for remaining != "" {
		open := strings.Index(remaining, "{{")
		closeIndex := strings.Index(remaining, "}}")
		if closeIndex >= 0 && (open < 0 || closeIndex < open) {
			return Template{}, errors.New("prompt template contains an unmatched closing delimiter")
		}
		if open < 0 {
			parts = append(parts, part{literal: remaining})
			break
		}
		if open > 0 {
			parts = append(parts, part{literal: remaining[:open]})
		}
		remaining = remaining[open+2:]
		closeIndex = strings.Index(remaining, "}}")
		if closeIndex < 0 {
			return Template{}, errors.New("prompt template contains an unclosed placeholder")
		}
		name := remaining[:closeIndex]
		if name == "" {
			return Template{}, errors.New("prompt template contains an empty placeholder")
		}
		if !placeholderName.MatchString(name) {
			return Template{}, fmt.Errorf("prompt template contains malformed placeholder %q", name)
		}
		if _, ok := builtins[name]; !ok {
			return Template{}, fmt.Errorf("prompt template contains unknown placeholder %q", name)
		}
		parts = append(parts, part{name: name})
		remaining = remaining[closeIndex+2:]
	}
	return Template{parts: parts}, nil
}

func DefaultPrompt(issue int) string {
	return "/skill:afk " + strconv.Itoa(issue)
}

func (template Template) Render(values Values) string {
	var rendered strings.Builder
	for _, part := range template.parts {
		if part.name == "" {
			rendered.WriteString(part.literal)
			continue
		}
		rendered.WriteString(builtins[part.name](values))
	}
	return rendered.String()
}
