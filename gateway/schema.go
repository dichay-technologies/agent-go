package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// JSONSchema represents a subset of JSON Schema Draft 7 / 2020-12 used for
// tool parameter definitions and structured model output validation.
type JSONSchema struct {
	Type                 string                 `json:"type,omitempty"`
	Description          string                 `json:"description,omitempty"`
	Properties           map[string]*JSONSchema `json:"properties,omitempty"`
	Required             []string               `json:"required,omitempty"`
	Items                *JSONSchema            `json:"items,omitempty"`
	Enum                 []any                  `json:"enum,omitempty"`
	AdditionalProperties *bool                  `json:"additionalProperties,omitempty"`
}

// Validate checks whether raw JSON bytes conform to this schema.
func (s *JSONSchema) Validate(raw json.RawMessage) error {
	if s == nil {
		return nil
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return fmt.Errorf("schema validation: empty JSON payload")
	}
	var val any
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	if err := dec.Decode(&val); err != nil {
		return fmt.Errorf("schema validation: invalid JSON: %w", err)
	}
	return s.validateValue("$", val)
}

func (s *JSONSchema) validateValue(path string, val any) error {
	if s == nil {
		return nil
	}
	if len(s.Enum) > 0 {
		matched := false
		for _, allowed := range s.Enum {
			if jsonEqual(val, allowed) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s: value %v is not in enum %v", path, val, s.Enum)
		}
	}

	switch s.Type {
	case "":
		return nil
	case "string":
		if _, ok := val.(string); !ok {
			return fmt.Errorf("%s: expected string, got %T", path, val)
		}
	case "boolean":
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("%s: expected boolean, got %T", path, val)
		}
	case "number":
		if !isJSONNumber(val) {
			return fmt.Errorf("%s: expected number, got %T", path, val)
		}
	case "integer":
		num, ok := val.(json.Number)
		if !ok {
			if f, okF := val.(float64); !okF || f != float64(int64(f)) {
				return fmt.Errorf("%s: expected integer, got %T", path, val)
			}
			return nil
		}
		if strings.ContainsAny(num.String(), ".eE") {
			if _, err := num.Int64(); err != nil {
				return fmt.Errorf("%s: expected integer, got %s", path, num.String())
			}
		}
	case "null":
		if val != nil {
			return fmt.Errorf("%s: expected null, got %T", path, val)
		}
	case "array":
		arr, ok := val.([]any)
		if !ok {
			return fmt.Errorf("%s: expected array, got %T", path, val)
		}
		if s.Items != nil {
			for i, item := range arr {
				if err := s.Items.validateValue(fmt.Sprintf("%s[%d]", path, i), item); err != nil {
					return err
				}
			}
		}
	case "object":
		obj, ok := val.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: expected object, got %T", path, val)
		}
		for _, reqKey := range s.Required {
			if _, exists := obj[reqKey]; !exists {
				return fmt.Errorf("%s: missing required field %q", path, reqKey)
			}
		}
		for k, v := range obj {
			propSchema, known := s.Properties[k]
			if !known {
				if s.AdditionalProperties != nil && !*s.AdditionalProperties {
					return fmt.Errorf("%s: unexpected property %q", path, k)
				}
				continue
			}
			if err := propSchema.validateValue(path+"."+k, v); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%s: unsupported schema type %q", path, s.Type)
	}
	return nil
}

func isJSONNumber(v any) bool {
	switch v.(type) {
	case json.Number, float64, float32, int, int64, int32:
		return true
	default:
		return false
	}
}

func jsonEqual(a, b any) bool {
	if na, okA := a.(json.Number); okA {
		if nb, okB := b.(json.Number); okB {
			return na.String() == nb.String()
		}
		return na.String() == fmt.Sprint(b)
	}
	if sa, okA := a.(string); okA {
		sb, okB := b.(string)
		return okB && sa == sb
	}
	if ba, okA := a.(bool); okA {
		bb, okB := b.(bool)
		return okB && ba == bb
	}
	if slices.Contains([]any{a}, b) {
		return true
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}

