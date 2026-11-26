package gateway

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrRateLimit     = errors.New("gateway: rate limit exceeded")
	ErrContextLength = errors.New("gateway: context length exceeded")
	ErrAuth          = errors.New("gateway: authentication or authorization failed")
	ErrRetryable     = errors.New("gateway: retryable provider error")
)

// ProviderError is a typed error returned by providers so routers and middleware
// can inspect error categories (RateLimit, ContextLength, Auth, Retryable) via errors.Is.
type ProviderError struct {
	Provider   string
	StatusCode int
	Kind       error
	Retry      bool
	Message    string
}

func (e *ProviderError) Error() string {
	if e.Provider != "" {
		return fmt.Sprintf("%s (status %d): %s", e.Provider, e.StatusCode, e.Message)
	}
	return fmt.Sprintf("status %d: %s", e.StatusCode, e.Message)
}

func (e *ProviderError) Unwrap() []error {
	var errs []error
	if e.Kind != nil {
		errs = append(errs, e.Kind)
	}
	if e.Retry {
		errs = append(errs, ErrRetryable)
	}
	return errs
}

func (e *ProviderError) Retryable() bool {
	return e.Retry
}

// ClassifyStatusCode maps an HTTP status code and response message into a *ProviderError.
func ClassifyStatusCode(provider string, statusCode int, msg string) error {
	pe := &ProviderError{
		Provider:   provider,
		StatusCode: statusCode,
		Message:    msg,
	}
	switch {
	case statusCode == 401 || statusCode == 403:
		pe.Kind = ErrAuth
	case statusCode == 429:
		pe.Kind = ErrRateLimit
		pe.Retry = true
	case statusCode == 413 || (statusCode == 400 && isContextLengthMsg(msg)):
		pe.Kind = ErrContextLength
	case statusCode >= 500 || statusCode == 408:
		pe.Kind = ErrRetryable
		pe.Retry = true
	}
	return pe
}

func isContextLengthMsg(msg string) bool {
	low := strings.ToLower(msg)
	return strings.Contains(low, "context length") ||
		strings.Contains(low, "maximum context") ||
		strings.Contains(low, "too many tokens") ||
		strings.Contains(low, "token limit")
}

