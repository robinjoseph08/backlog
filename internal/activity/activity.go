// Package activity normalizes privacy-safe semantic Activity from Pi Worker JSONL.
package activity

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const CurrentVersion = 1

// Entry is one semantic Worker change. It deliberately excludes reasoning,
// tool arguments, and tool results.
type Entry struct {
	Version          int       `json:"version"`
	ObservedAt       time.Time `json:"observedAt"`
	Kind             string    `json:"kind"`
	Description      string    `json:"description"`
	Operation        string    `json:"operation,omitempty"`
	OperationChanged bool      `json:"operationChanged,omitempty"`
	TurnDelta        int       `json:"turnDelta,omitempty"`
	TokenDelta       int64     `json:"tokenDelta,omitempty"`
	TokensKnown      bool      `json:"tokensKnown,omitempty"`
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
		if text := visibleText(message.Content); text != "" {
			entry.Description += ": " + text
		}
		if message.Usage != nil && message.Usage.TotalTokens != nil && *message.Usage.TotalTokens >= 0 {
			entry.TokensKnown = true
			entry.TokenDelta = *message.Usage.TotalTokens
		}
		entry.Operation, entry.OperationChanged = "model completed", true
	case "tool_execution_start":
		id, name := stringField(event, "toolCallId"), stringField(event, "toolName")
		if name == "" {
			name = "unknown"
		}
		p.ensureTools()
		p.toolNames[id] = name
		entry.Kind, entry.Description = "tool", "Tool "+name+" started"
		entry.Operation, entry.OperationChanged = name, true
	case "tool_execution_update":
		id, name := stringField(event, "toolCallId"), stringField(event, "toolName")
		p.ensureTools()
		if name == "" {
			name = p.toolNames[id]
		}
		if name == "" {
			name = "unknown"
		}
		fingerprint, meaningful := semanticFingerprint(event["partialResult"])
		if !meaningful || p.toolFingerprints[id] == fingerprint {
			return Entry{}, false, nil
		}
		p.toolFingerprints[id] = fingerprint
		entry.Kind, entry.Description = "tool", "Tool "+name+" output changed"
		entry.Operation, entry.OperationChanged = name, true
	case "tool_execution_end":
		id, name := stringField(event, "toolCallId"), stringField(event, "toolName")
		p.ensureTools()
		if name == "" {
			name = p.toolNames[id]
		}
		if name == "" {
			name = "unknown"
		}
		description := "Tool " + name + " completed"
		if boolField(event, "isError") {
			description = "Tool " + name + " failed"
		}
		delete(p.toolNames, id)
		delete(p.toolFingerprints, id)
		entry.Kind, entry.Description = "tool", description
		entry.Operation, entry.OperationChanged = "model", true
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

func (p *Projector) ensureTools() {
	if p.toolFingerprints == nil {
		p.toolFingerprints = make(map[string][32]byte)
		p.toolNames = make(map[string]string)
	}
}

type assistantMessage struct {
	Role    string            `json:"role"`
	Content []json.RawMessage `json:"content"`
	Usage   *struct {
		TotalTokens *int64 `json:"totalTokens"`
	} `json:"usage"`
}

func decodeAssistantMessage(raw json.RawMessage) (assistantMessage, bool) {
	var message assistantMessage
	if json.Unmarshal(raw, &message) != nil || message.Role != "assistant" {
		return assistantMessage{}, false
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
