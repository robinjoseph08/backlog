package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

func readConfirmation(ctx context.Context, reader *bufio.Reader, operation string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	type result struct {
		line string
		err  error
	}
	resultChannel := make(chan result, 1)
	go func() {
		line, err := reader.ReadString('\n')
		resultChannel <- result{line: line, err: err}
	}()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case read := <-resultChannel:
		if errors.Is(read.err, io.EOF) {
			return false, nil
		}
		if read.err != nil {
			return false, fmt.Errorf("read %s confirmation: %w", operation, read.err)
		}
		answer := strings.ToLower(strings.TrimSpace(read.line))
		return answer == "y" || answer == "yes", nil
	}
}
