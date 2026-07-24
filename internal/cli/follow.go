package cli

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

const followPollInterval = 50 * time.Millisecond

type followStateSource interface {
	Preview() (state.State, bool, error)
}

func followCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("follow", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: backlog follow <run-id> --raw [flags]")
		flags.PrintDefaults()
	}
	repoDir := flags.String("repo-dir", ".", "Git repository associated with the Run")
	stateDir := flags.String("state-dir", "", "runner state directory")
	gitExecutable := flags.String("git", "git", "git executable used to identify the repository root")
	raw := flags.Bool("raw", false, "print the Worker's raw JSONL")
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return flags.Parse([]string{arg})
		}
	}
	runID, flagArgs, err := splitFollowArguments(args)
	if err != nil {
		return err
	}
	if err := flags.Parse(flagArgs); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected follow arguments: %s", strings.Join(flags.Args(), " "))
	}
	if !*raw {
		return errors.New("follow currently requires --raw")
	}
	resolved, _, err := resolveStateFromFlags(ctx, *repoDir, *stateDir, *gitExecutable)
	if err != nil {
		return err
	}
	source := state.FileStore{Path: filepath.Join(resolved, "state.json")}
	return followRaw(ctx, source, runID, stdout, followPollInterval)
}

func splitFollowArguments(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, errors.New("usage: backlog follow <run-id> --raw [flags]")
	}
	if !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:], nil
	}
	for index, value := range args {
		if !strings.HasPrefix(value, "-") && (index == 0 || !followFlagTakesValue(args[index-1])) {
			remaining := append([]string{}, args[:index]...)
			remaining = append(remaining, args[index+1:]...)
			return value, remaining, nil
		}
	}
	return "", nil, errors.New("follow requires a Run ID")
}

func followFlagTakesValue(name string) bool {
	if strings.Contains(name, "=") {
		return false
	}
	name = strings.TrimLeft(name, "-")
	return name == "repo-dir" || name == "state-dir" || name == "git"
}

func followRaw(ctx context.Context, source followStateSource, runID string, output io.Writer, pollInterval time.Duration) error {
	selected, err := loadFollowRun(source, runID)
	if err != nil {
		return err
	}
	for selected.LogPath == "" {
		if selected.Status != scheduler.StatusClaimed && selected.Status != scheduler.StatusWorktreeReady {
			return fmt.Errorf("Run %q has no Worker log available", runID)
		}
		if !waitToFollow(ctx, pollInterval) {
			return nil
		}
		selected, err = loadFollowRun(source, runID)
		if err != nil {
			return err
		}
	}

	logPath := selected.LogPath
	logFile, err := os.Open(logPath)
	if err != nil {
		return fmt.Errorf("Run %q Worker log %q is unavailable: %w", runID, logPath, err)
	}
	defer logFile.Close()

	stream := rawLogStream{file: logFile, output: output}
	for {
		if err := stream.emitAvailable(ctx); err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return nil
			}
			return fmt.Errorf("follow Run %q Worker log %q: %w", runID, logPath, err)
		}
		selected, err = loadFollowRun(source, runID)
		if err != nil {
			return err
		}
		if selected.LogPath != logPath {
			return fmt.Errorf("Run %q Worker log changed from %q to %q", runID, logPath, selected.LogPath)
		}
		if scheduler.IsTerminal(selected.Status) && !selected.WorkerLogOpen {
			if err := stream.emitAvailable(ctx); err != nil {
				if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
					return nil
				}
				return fmt.Errorf("finish following Run %q Worker log %q: %w", runID, logPath, err)
			}
			return nil
		}
		if !waitToFollow(ctx, pollInterval) {
			return nil
		}
	}
}

func loadFollowRun(source followStateSource, runID string) (scheduler.Run, error) {
	current, _, err := source.Preview()
	if err != nil {
		return scheduler.Run{}, fmt.Errorf("follow Run %q: read runner state: %w", runID, err)
	}
	for _, run := range current.Runs {
		if run.RunID == runID {
			return run, nil
		}
	}
	return scheduler.Run{}, fmt.Errorf("Run %q was not found", runID)
}

func waitToFollow(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

type rawLogStream struct {
	file          *os.File
	output        io.Writer
	scannedOffset int64
	emittedOffset int64
}

func (s *rawLogStream) emitAvailable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("inspect raw JSONL: %w", err)
	}
	remaining := info.Size() - s.scannedOffset
	buffer := make([]byte, 32*1024)
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		readSize := min(int64(len(buffer)), remaining)
		count, readErr := s.file.Read(buffer[:readSize])
		if count > 0 {
			s.scannedOffset += int64(count)
			remaining -= int64(count)
			if newline := bytes.LastIndexByte(buffer[:count], '\n'); newline >= 0 {
				completeOffset := s.scannedOffset - int64(count-newline-1)
				if err := s.emitThrough(ctx, completeOffset, buffer); err != nil {
					return err
				}
			}
		}
		if readErr != nil {
			return fmt.Errorf("read raw JSONL: %w", readErr)
		}
		if count == 0 {
			return fmt.Errorf("read raw JSONL: %w", io.ErrNoProgress)
		}
	}
	return nil
}

func (s *rawLogStream) emitThrough(ctx context.Context, completeOffset int64, buffer []byte) error {
	reader := io.NewSectionReader(s.file, s.emittedOffset, completeOffset-s.emittedOffset)
	remaining := completeOffset - s.emittedOffset
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		count, err := reader.Read(buffer)
		if count > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if writeErr := writeAllContext(ctx, s.output, buffer[:count]); writeErr != nil {
				return fmt.Errorf("write raw JSONL: %w", writeErr)
			}
			remaining -= int64(count)
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("read complete raw JSONL: %w", err)
		}
		if count == 0 {
			return fmt.Errorf("read complete raw JSONL: %w", io.ErrUnexpectedEOF)
		}
	}
	s.emittedOffset = completeOffset
	return nil
}

func writeAll(writer io.Writer, data []byte) error {
	return writeAllContext(context.Background(), writer, data)
}

func writeAllContext(ctx context.Context, writer io.Writer, data []byte) error {
	for len(data) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
