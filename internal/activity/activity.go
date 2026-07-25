// Package activity normalizes privacy-safe semantic Activity from Pi Worker JSONL.
package activity

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const CurrentVersion = 1

// Entry is one semantic Worker or Subagent change. It deliberately excludes
// reasoning, tool arguments, and tool results.
type Entry struct {
	Version           int               `json:"version"`
	ObservedAt        time.Time         `json:"observedAt"`
	Kind              string            `json:"kind"`
	Description       string            `json:"description"`
	Operation         string            `json:"operation,omitempty"`
	OperationChanged  bool              `json:"operationChanged,omitempty"`
	TurnDelta         int               `json:"turnDelta,omitempty"`
	ResponseCompleted bool              `json:"responseCompleted,omitempty"`
	TokenDelta        int64             `json:"tokenDelta,omitempty"`
	TokensKnown       bool              `json:"tokensKnown,omitempty"`
	Subagent          *SubagentSnapshot `json:"subagent,omitempty"`
	SuppressFeed      bool              `json:"suppressFeed,omitempty"`
}

// SubagentSnapshot is the latest privacy-safe telemetry for one Agent tool
// call. Pointer counters distinguish a reported zero from unavailable data.
type SubagentSnapshot struct {
	ID             string `json:"id"`
	Description    string `json:"description,omitempty"`
	Status         string `json:"status,omitempty"`
	Activity       string `json:"activity,omitempty"`
	Turns          *int   `json:"turns,omitempty"`
	ToolUses       *int   `json:"toolUses,omitempty"`
	ApproxTokens   *int64 `json:"approxTokens,omitempty"`
	DurationMillis *int64 `json:"durationMillis,omitempty"`
	Active         bool   `json:"active,omitempty"`
	Completed      bool   `json:"completed,omitempty"`
}

// PathForLog returns the observational sidecar path for a raw Worker log.
func PathForLog(logPath string) string {
	extension := filepath.Ext(logPath)
	if extension == "" {
		return logPath + ".activity.jsonl"
	}
	return strings.TrimSuffix(logPath, extension) + ".activity.jsonl"
}

// Projector holds only transient normalization state. Durable lifecycle state
// is not involved in observing Worker events.
type Projector struct {
	messageFingerprint [32]byte
	haveMessage        bool
	toolFingerprints   map[string][32]byte
	toolNames          map[string]string
	toolOrder          []string
	subagents          map[string]*subagentState
}

type subagentState struct {
	snapshot          SubagentSnapshot
	haveSnapshot      bool
	outputFingerprint [32]byte
	haveOutput        bool
	lastFeed          time.Time
	haveFeed          bool
}

// Observe ignores unknown fields and event types. Malformed known or unknown
// JSON is an observation error, not an execution-protocol decision.
func (p *Projector) Observe(record []byte, observedAt time.Time) (Entry, bool, error) {
	var event map[string]json.RawMessage
	if err := json.Unmarshal(record, &event); err != nil {
		return Entry{}, false, fmt.Errorf("decode Worker Activity: %w", err)
	}
	var eventType string
	if err := json.Unmarshal(event["type"], &eventType); err != nil || eventType == "" {
		return Entry{}, false, nil
	}
	entry := Entry{Version: CurrentVersion, ObservedAt: observedAt.UTC()}
	switch eventType {
	case "agent_start":
		entry.Kind, entry.Description = "lifecycle", "Worker started"
		entry.Operation, entry.OperationChanged = "starting", true
	case "agent_end":
		entry.Kind, entry.Description = "lifecycle", "Worker response cycle ended"
		entry.Operation, entry.OperationChanged = "settling", true
	case "agent_settled":
		entry.Kind, entry.Description = "lifecycle", "Worker settled"
		entry.OperationChanged = true
	case "turn_start":
		entry.Kind, entry.Description = "turn", "Worker turn started"
		entry.Operation, entry.OperationChanged = "model", true
	case "turn_end":
		entry.Kind, entry.Description, entry.TurnDelta = "turn", "Worker turn completed", 1
		entry.Operation, entry.OperationChanged = "between turns", true
	case "message_update":
		fingerprint, valid := canonicalFingerprint(event["assistantMessageEvent"])
		if !valid {
			fingerprint = sha256.Sum256([]byte("message_update"))
		}
		if p.haveMessage && fingerprint == p.messageFingerprint {
			return Entry{}, false, nil
		}
		p.messageFingerprint, p.haveMessage = fingerprint, true
		entry.Kind, entry.Description = "model", "Model streaming"
		entry.Operation, entry.OperationChanged = "model streaming", true
	case "message_end":
		message, assistant := decodeAssistantMessage(event["message"])
		if !assistant {
			return Entry{}, false, nil
		}
		entry.Kind, entry.Description = "model", "Assistant response completed"
		entry.ResponseCompleted = true
		if text := visibleText(message.Content); text != "" {
			entry.Description += ": " + text
		}
		if message.Usage != nil && message.Usage.TotalTokens != nil && *message.Usage.TotalTokens >= 0 {
			entry.TokensKnown = true
			entry.TokenDelta = *message.Usage.TotalTokens
		}
		entry.Operation, entry.OperationChanged = "model completed", true
	case "tool_execution_start":
		id := stringField(event, "toolCallId")
		p.ensureTools()
		if _, active := p.toolNames[id]; !active {
			p.toolOrder = append(p.toolOrder, id)
		}
		name := p.resolveToolName(id, stringField(event, "toolName"))
		p.toolNames[id] = name
		entry.Kind, entry.Description = "tool", "Tool "+name+" started"
		entry.Operation, entry.OperationChanged = name, true
	case "tool_execution_update":
		id := stringField(event, "toolCallId")
		name := p.resolveToolName(id, stringField(event, "toolName"))
		if isAgentTool(name) {
			return p.observeSubagent(id, event["partialResult"], observedAt, true, false, false)
		}
		fingerprint, meaningful := semanticFingerprint(event["partialResult"])
		if !meaningful || p.toolFingerprints[id] == fingerprint {
			return Entry{}, false, nil
		}
		p.toolFingerprints[id] = fingerprint
		entry.Kind, entry.Description = "tool", "Tool "+name+" output changed"
		entry.Operation, entry.OperationChanged = name, true
	case "tool_execution_end":
		id := stringField(event, "toolCallId")
		name := p.resolveToolName(id, stringField(event, "toolName"))
		p.finishTool(id)
		if isAgentTool(name) {
			return p.observeSubagent(id, event["result"], observedAt, false, true, boolField(event, "isError"))
		}
		description := "Tool " + name + " completed"
		if boolField(event, "isError") {
			description = "Tool " + name + " failed"
		}
		operation := "model"
		if active := p.activeToolName(); active != "" {
			operation = active
		}
		entry.Kind, entry.Description = "tool", description
		entry.Operation, entry.OperationChanged = operation, true
	case "auto_retry_start":
		entry.Kind = "retry"
		entry.Description = fmt.Sprintf("Worker retry %d started", intField(event, "attempt"))
		if message := shortText(stringField(event, "errorMessage"), 160); message != "" {
			entry.Description += ": " + message
		}
		entry.Operation, entry.OperationChanged = "retrying", true
	case "auto_retry_end":
		entry.Kind = "retry"
		entry.Description = fmt.Sprintf("Worker retry %d ended", intField(event, "attempt"))
		if message := shortText(stringField(event, "finalError"), 160); message != "" {
			entry.Description += ": " + message
		}
		entry.Operation, entry.OperationChanged = "between turns", true
	case "compaction_start":
		entry.Kind, entry.Description = "compaction", "Context compaction started"
		entry.Operation, entry.OperationChanged = "compacting context", true
	case "compaction_end":
		entry.Kind, entry.Description = "compaction", "Context compaction ended"
		if message := shortText(stringField(event, "errorMessage"), 160); message != "" {
			entry.Description += ": " + message
		}
		entry.Operation, entry.OperationChanged = "between turns", true
	case "summarization_retry_scheduled":
		entry.Kind = "retry"
		entry.Description = fmt.Sprintf("Compaction retry %d scheduled", intField(event, "attempt"))
		entry.Operation, entry.OperationChanged = "compacting context", true
	case "summarization_retry_attempt_start":
		entry.Kind, entry.Description = "retry", "Compaction retry started"
		entry.Operation, entry.OperationChanged = "compacting context", true
	case "summarization_retry_finished":
		entry.Kind, entry.Description = "retry", "Compaction retry finished"
		entry.Operation, entry.OperationChanged = "compacting context", true
	default:
		return Entry{}, false, nil
	}
	return entry, true, nil
}

func isAgentTool(name string) bool {
	return strings.EqualFold(name, "Agent")
}

func (p *Projector) observeSubagent(id string, raw json.RawMessage, observedAt time.Time, active, completed, failed bool) (Entry, bool, error) {
	if id == "" {
		id = "unknown"
	}
	if p.subagents == nil {
		p.subagents = make(map[string]*subagentState)
	}
	state := p.subagents[id]
	if state == nil {
		state = &subagentState{}
		p.subagents[id] = state
	}

	snapshot, outputFingerprint, haveOutput := decodeSubagentSnapshot(id, raw)
	if completed && snapshot.Activity == "" {
		snapshot.Activity = state.snapshot.Activity
	}
	if failed {
		snapshot.Status = "failed"
	}
	snapshot.Active = active
	snapshot.Completed = completed
	meaningfulSnapshot := subagentSemanticSnapshot(snapshot)
	previousSnapshot := subagentSemanticSnapshot(state.snapshot)
	outputChanged := haveOutput && (!state.haveOutput || outputFingerprint != state.outputFingerprint)
	meaningful := !state.haveSnapshot || !reflect.DeepEqual(meaningfulSnapshot, previousSnapshot) || outputChanged

	previous := state.snapshot
	state.snapshot = snapshot
	state.haveSnapshot = true
	if haveOutput {
		state.outputFingerprint = outputFingerprint
		state.haveOutput = true
	}
	if !meaningful {
		return Entry{}, false, nil
	}

	statusChanged := previous.Status != snapshot.Status
	turnChanged := !equalInt(previous.Turns, snapshot.Turns)
	entry := Entry{
		Version: CurrentVersion, ObservedAt: observedAt.UTC(), Kind: "subagent",
		Description: describeSubagentChange(snapshot, previous, state.haveFeed, outputChanged),
		Subagent:    &snapshot,
	}
	if completed {
		entry.Operation = "model"
		if operation := p.activeToolName(); operation != "" {
			entry.Operation = operation
		}
		entry.OperationChanged = true
	}
	retainMilestone := !state.haveFeed || statusChanged || turnChanged || completed
	if !retainMilestone && observedAt.Sub(state.lastFeed) < time.Second {
		entry.SuppressFeed = true
	} else {
		state.lastFeed = observedAt
		state.haveFeed = true
	}
	return entry, true, nil
}

func subagentSemanticSnapshot(snapshot SubagentSnapshot) SubagentSnapshot {
	snapshot.DurationMillis = nil
	return snapshot
}

func equalInt(left, right *int) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func decodeSubagentSnapshot(id string, raw json.RawMessage) (SubagentSnapshot, [32]byte, bool) {
	snapshot := SubagentSnapshot{ID: id}
	var result map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &result) != nil {
		return snapshot, [32]byte{}, false
	}
	var details map[string]json.RawMessage
	if json.Unmarshal(result["details"], &details) == nil {
		snapshot.Description = safeString(details["description"], 120)
		snapshot.Status = safeString(details["status"], 40)
		snapshot.Activity = safeString(details["activity"], 120)
		snapshot.Turns = nonnegativeInt(details["turnCount"])
		snapshot.ToolUses = nonnegativeInt(details["toolUses"])
		snapshot.DurationMillis = nonnegativeInt64(details["durationMs"])
		if tokens := safeString(details["tokens"], 40); tokens != "" {
			snapshot.ApproxTokens = parseApproxTokens(tokens)
		}
	}
	fingerprint, haveOutput := visibleContentFingerprint(result["content"])
	return snapshot, fingerprint, haveOutput
}

func describeSubagentChange(current, previous SubagentSnapshot, hadPrevious, outputChanged bool) string {
	label := valueOrUnavailable(current.Description)
	identity := current.ID
	if len(identity) > 16 {
		identity = identity[:16]
	}
	prefix := `Subagent [` + identity + `] "` + label + `"`
	if current.Completed {
		status := valueOrUnavailable(current.Status)
		if status == "failed" {
			return prefix + " failed"
		}
		return prefix + " completed (" + status + ")"
	}
	statusChanged := !hadPrevious || current.Status != previous.Status
	turnChanged := !hadPrevious || !equalInt(current.Turns, previous.Turns)
	activityChanged := !hadPrevious || current.Activity != previous.Activity
	if statusChanged || turnChanged {
		changes := make([]string, 0, 3)
		if statusChanged {
			changes = append(changes, "status: "+valueOrUnavailable(current.Status))
		}
		if activityChanged {
			changes = append(changes, "activity: "+valueOrUnavailable(current.Activity))
		}
		if turnChanged {
			if current.Turns == nil {
				changes = append(changes, "turns: n/a")
			} else {
				changes = append(changes, fmt.Sprintf("reached turn %d", *current.Turns))
			}
		}
		return prefix + " " + strings.Join(changes, "; ")
	}
	if current.Activity != previous.Activity {
		return prefix + " activity: " + valueOrUnavailable(current.Activity)
	}
	if current.Description != previous.Description {
		return prefix + " description changed to " + label
	}
	if !equalInt(current.ToolUses, previous.ToolUses) {
		if current.ToolUses == nil {
			return prefix + " tool uses: n/a"
		}
		return fmt.Sprintf("%s tool uses: %d", prefix, *current.ToolUses)
	}
	if !equalInt64(current.ApproxTokens, previous.ApproxTokens) {
		if current.ApproxTokens == nil {
			return prefix + " approximate tokens: n/a"
		}
		return fmt.Sprintf("%s approximate tokens: ~%d", prefix, *current.ApproxTokens)
	}
	if outputChanged {
		return prefix + " visible output changed"
	}
	return prefix + " telemetry changed"
}

func valueOrUnavailable(value string) string {
	if value == "" {
		return "n/a"
	}
	return value
}

func safeString(raw json.RawMessage, limit int) string {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return shortText(value, limit)
}

func nonnegativeInt(raw json.RawMessage) *int {
	var value int
	if json.Unmarshal(raw, &value) != nil || value < 0 {
		return nil
	}
	return &value
}

func nonnegativeInt64(raw json.RawMessage) *int64 {
	var value int64
	if json.Unmarshal(raw, &value) != nil || value < 0 {
		return nil
	}
	return &value
}

func equalInt64(left, right *int64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

var approximateTokensPattern = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)([kM]?) tokens?$`)

func parseApproxTokens(value string) *int64 {
	matches := approximateTokensPattern.FindStringSubmatch(value)
	if matches == nil {
		return nil
	}
	number, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return nil
	}
	multiplier := float64(1)
	switch matches[2] {
	case "k":
		multiplier = 1_000
	case "M":
		multiplier = 1_000_000
	}
	tokens := number * multiplier
	if tokens > math.MaxInt64 {
		return nil
	}
	result := int64(math.Round(tokens))
	return &result
}

func (p *Projector) ensureTools() {
	if p.toolFingerprints == nil {
		p.toolFingerprints = make(map[string][32]byte)
		p.toolNames = make(map[string]string)
	}
}

func (p *Projector) resolveToolName(id, name string) string {
	p.ensureTools()
	if name == "" {
		name = p.toolNames[id]
	}
	if name == "" {
		name = "unknown"
	}
	return name
}

func (p *Projector) finishTool(id string) {
	delete(p.toolNames, id)
	delete(p.toolFingerprints, id)
	for index, activeID := range p.toolOrder {
		if activeID == id {
			p.toolOrder = append(p.toolOrder[:index], p.toolOrder[index+1:]...)
			return
		}
	}
}

func (p *Projector) activeToolName() string {
	for index := len(p.toolOrder) - 1; index >= 0; index-- {
		if name := p.toolNames[p.toolOrder[index]]; name != "" {
			return name
		}
	}
	return ""
}

type assistantMessage struct {
	Content []json.RawMessage
	Usage   *struct {
		TotalTokens *int64 `json:"totalTokens"`
	}
}

func decodeAssistantMessage(raw json.RawMessage) (assistantMessage, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return assistantMessage{}, false
	}
	var role string
	if json.Unmarshal(fields["role"], &role) != nil || role != "assistant" {
		return assistantMessage{}, false
	}
	var message assistantMessage
	_ = json.Unmarshal(fields["content"], &message.Content)
	var usage struct {
		TotalTokens *int64 `json:"totalTokens"`
	}
	if json.Unmarshal(fields["usage"], &usage) == nil {
		message.Usage = &usage
	}
	return message, true
}

func visibleText(contents []json.RawMessage) string {
	var parts []string
	for _, raw := range contents {
		var content struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(raw, &content) == nil && content.Type == "text" && content.Text != "" {
			parts = append(parts, content.Text)
		}
	}
	return shortText(strings.Join(parts, " "), 240)
}

func shortText(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	if len(value) <= limit {
		return value
	}
	return value[:limit-3] + "..."
}

func stringField(event map[string]json.RawMessage, name string) string {
	var value string
	_ = json.Unmarshal(event[name], &value)
	return value
}

func intField(event map[string]json.RawMessage, name string) int {
	var value int
	_ = json.Unmarshal(event[name], &value)
	return value
}

func boolField(event map[string]json.RawMessage, name string) bool {
	var value bool
	_ = json.Unmarshal(event[name], &value)
	return value
}

var cosmeticFields = map[string]struct{}{
	"duration": {}, "durationms": {}, "elapsed": {}, "elapsedms": {},
	"spinner": {}, "spinnerframe": {}, "frame": {}, "updatedat": {},
}

func canonicalFingerprint(raw json.RawMessage) ([32]byte, bool) {
	var value any
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return [32]byte{}, false
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return [32]byte{}, false
	}
	return sha256.Sum256(encoded), true
}

func visibleContentFingerprint(raw json.RawMessage) ([32]byte, bool) {
	var contents []json.RawMessage
	if json.Unmarshal(raw, &contents) != nil {
		return [32]byte{}, false
	}
	visible := make([]string, 0, len(contents))
	for _, rawContent := range contents {
		var content struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(rawContent, &content) == nil && content.Type == "text" && content.Text != "" {
			visible = append(visible, content.Text)
		}
	}
	if len(visible) == 0 {
		return [32]byte{}, false
	}
	encoded, err := json.Marshal(visible)
	if err != nil {
		return [32]byte{}, false
	}
	return sha256.Sum256(encoded), true
}

func semanticFingerprint(raw json.RawMessage) ([32]byte, bool) {
	var value any
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return [32]byte{}, false
	}
	cleaned, meaningful := removeCosmetic(value)
	if !meaningful {
		return [32]byte{}, false
	}
	encoded, err := json.Marshal(cleaned)
	if err != nil {
		return [32]byte{}, false
	}
	return sha256.Sum256(encoded), true
}

func removeCosmetic(value any) (any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any)
		for key, child := range typed {
			if _, cosmetic := cosmeticFields[strings.ToLower(key)]; cosmetic {
				continue
			}
			cleaned, meaningful := removeCosmetic(child)
			if meaningful {
				result[key] = cleaned
			}
		}
		return result, len(result) > 0
	case []any:
		result := make([]any, 0, len(typed))
		for _, child := range typed {
			cleaned, meaningful := removeCosmetic(child)
			if meaningful {
				result = append(result, cleaned)
			}
		}
		return result, len(result) > 0
	case nil:
		return nil, false
	default:
		return value, true
	}
}
