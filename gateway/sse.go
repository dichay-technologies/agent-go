package gateway

import (
	"bufio"
	"io"
	"iter"
	"strings"
)

// SSEEvent represents a single Server-Sent Events frame.
type SSEEvent struct {
	Event string
	Data  string
}

// ReadSSE parses a Server-Sent Events (text/event-stream) stream and yields
// complete SSEEvent frames separated by blank lines.
func ReadSSE(r io.Reader) iter.Seq2[SSEEvent, error] {
	return func(yield func(SSEEvent, error) bool) {
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 64*1024), 8<<20)

		var evType string
		var dataLines []string

		flush := func() bool {
			if evType == "" && len(dataLines) == 0 {
				return true
			}
			ev := SSEEvent{
				Event: evType,
				Data:  strings.Join(dataLines, "\n"),
			}
			evType = ""
			dataLines = dataLines[:0]
			return yield(ev, nil)
		}

		for scanner.Scan() {
			line := strings.TrimRight(scanner.Text(), "\r")
			if line == "" {
				if !flush() {
					return
				}
				continue
			}
			if strings.HasPrefix(line, ":") {
				continue
			}
			field, val, hasColon := strings.Cut(line, ":")
			if hasColon {
				val = strings.TrimPrefix(val, " ")
			}
			switch field {
			case "event":
				evType = val
			case "data":
				dataLines = append(dataLines, val)
			}
		}
		if err := scanner.Err(); err != nil {
			yield(SSEEvent{}, err)
			return
		}
		flush()
	}
}

