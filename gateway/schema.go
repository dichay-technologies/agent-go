package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
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
	return reflect.DeepEqual(a, b)
}

// OpenAINormalize returns a shallow/deep copy normalized for OpenAI response_format
// and reports whether strict mode is valid (all object properties listed in Required).
func (s *JSONSchema) OpenAINormalize() (*JSONSchema, bool) {
	if s == nil {
		return nil, false
	}
	cp := *s
	strictOK := true
	if cp.Type == "object" {
		reqSet := make(map[string]bool, len(cp.Required))
		for _, r := range cp.Required {
			reqSet[r] = true
		}
		if len(cp.Properties) > 0 {
			props := make(map[string]*JSONSchema, len(cp.Properties))
			for k, v := range cp.Properties {
				if !reqSet[k] {
					strictOK = false
				}
				child, childStrict := v.OpenAINormalize()
				if !childStrict {
					strictOK = false
				}
				props[k] = child
			}
			cp.Properties = props
		}
		if strictOK && cp.AdditionalProperties == nil {
			f := false
			cp.AdditionalProperties = &f
		}
	} else if cp.Type == "array" && cp.Items != nil {
		child, childStrict := cp.Items.OpenAINormalize()
		cp.Items = child
		if !childStrict {
			strictOK = false
		}
	}
	return &cp, strictOK
}

// SchemaFor generates a JSONSchema from a Go type T using reflection.
// It inspects `json` tags for field names and `omitempty` (to omit from Required),
// and `desc` or `description` tags for field descriptions.
func SchemaFor[T any]() *JSONSchema {
	var zero T
	t := reflect.TypeOf(zero)
	if t == nil {
		return &JSONSchema{Type: "object"}
	}
	return schemaForType(t, map[reflect.Type]bool{})
}

func schemaForType(t reflect.Type, seen map[reflect.Type]bool) *JSONSchema {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.String:
		return &JSONSchema{Type: "string"}
	case reflect.Bool:
		return &JSONSchema{Type: "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &JSONSchema{Type: "integer"}
	case reflect.Float32, reflect.Float64:
		return &JSONSchema{Type: "number"}
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 && t.Kind() == reflect.Slice {
			return &JSONSchema{Type: "string"}
		}
		return &JSONSchema{
			Type:  "array",
			Items: schemaForType(t.Elem(), seen),
		}
	case reflect.Map:
		return &JSONSchema{Type: "object"}
	case reflect.Interface:
		return &JSONSchema{}
	case reflect.Struct:
		if seen[t] {
			return &JSONSchema{Type: "object"}
		}
		seen[t] = true
		defer delete(seen, t)

		props := make(map[string]*JSONSchema)
		var required []string

		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			jsonTag := f.Tag.Get("json")
			if jsonTag == "-" {
				continue
			}
			name := f.Name
			omitEmpty := false
			if jsonTag != "" {
				parts := strings.Split(jsonTag, ",")
				if parts[0] != "" {
					name = parts[0]
				}
				for _, opt := range parts[1:] {
					if opt == "omitempty" || opt == "omitzero" {
						omitEmpty = true
					}
				}
			}

			propSchema := schemaForType(f.Type, seen)
			if desc := f.Tag.Get("description"); desc != "" {
				propSchema.Description = desc
			} else if desc := f.Tag.Get("desc"); desc != "" {
				propSchema.Description = desc
			}

			props[name] = propSchema
			if !omitEmpty && f.Type.Kind() != reflect.Pointer {
				required = append(required, name)
			}
		}

		return &JSONSchema{
			Type:       "object",
			Properties: props,
			Required:   required,
		}
	default:
		return &JSONSchema{Type: "object"}
	}
}

