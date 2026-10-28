package spool

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

var errSpoolClosed = errors.New("spool is closed")

// File operations used by the spool and its recovery journal.
type spoolFile interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Seeker
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(int64) error
	Close() error
}

// Every accepted log is appended and fsynced before the handler reports acceptance.
type Spool struct {
	path    string
	ackPath string
	file    spoolFile
	ackFile spoolFile
	acked   map[string]bool
	spooled map[string]struct{}
	mu      sync.Mutex
}

// Returns a spool with durable acknowledgements in path plus ".acked".
func NewSpool(path string) (*Spool, error) {
	if path == "" {
		return nil, errors.New("spool path must not be empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("creating spool directory: %w", err)
	}
	s := &Spool{path: path, ackPath: path + ".acked", acked: make(map[string]bool), spooled: make(map[string]struct{})}
	var err error
	s.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.ackFile, err = os.OpenFile(s.ackPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		s.Close()
		return nil, err
	}
	for _, target := range []struct {
		file spoolFile
		path string
	}{{s.file, s.path}, {s.ackFile, s.ackPath}} {
		if err = recoverAppend(target.file, target.path); err == nil {
			err = truncatePartialLine(target.file)
		}
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("recovering spool %q: %w", target.path, err)
		}
	}
	if err = syncDirectory(path); err == nil {
		err = s.loadAcks()
	}
	if err == nil {
		var logs []model.RawLog
		logs, err = s.Unacked()
		for _, log := range logs {
			s.spooled[log.LogID] = struct{}{}
		}
	}
	if err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Writes the batch atomically with an undo journal, including across interrupted recovery.
func (spool *Spool) Append(logs []model.RawLog) error {
	if len(logs) == 0 {
		return nil
	}
	var payload bytes.Buffer
	for _, log := range logs {
		if err := json.NewEncoder(&payload).Encode(log); err != nil {
			return fmt.Errorf("marshalling spooled log %q: %w", log.LogID, err)
		}
	}
	spool.mu.Lock()
	defer spool.mu.Unlock()
	if spool.file == nil {
		return errSpoolClosed
	}
	if err := appendDurably(spool.file, spool.path, payload.Bytes()); err != nil {
		return fmt.Errorf("appending spool batch: %w", err)
	}
	for _, log := range logs {
		if !spool.acked[log.LogID] {
			spool.spooled[log.LogID] = struct{}{}
		}
	}
	return nil
}

// Marks one log as durably handled.
func (spool *Spool) Ack(logID string) error {
	return spool.AckAll([]string{logID})
}

// Acknowledges a batch with one acknowledgement write and sync. State changes only after success.
func (spool *Spool) AckAll(logIDs []string) error {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	if spool.ackFile == nil {
		return errSpoolClosed
	}
	var payload strings.Builder
	ids := make(map[string]struct{}, len(logIDs))
	for _, id := range logIDs {
		if _, present := spool.spooled[id]; !present {
			continue
		}
		if _, duplicate := ids[id]; duplicate {
			continue
		}
		if strings.ContainsAny(id, "\r\n") || id == "" {
			return errors.New("invalid spool acknowledgement ID")
		}
		ids[id] = struct{}{}
		payload.WriteString(id + "\n")
	}
	if len(ids) == 0 {
		return nil
	}
	if err := appendDurably(spool.ackFile, spool.ackPath, []byte(payload.String())); err != nil {
		return fmt.Errorf("acknowledging spool batch: %w", err)
	}
	for id := range ids {
		spool.acked[id] = true
		delete(spool.spooled, id)
	}
	return nil
}

// Returns every unacknowledged log, skipping corrupt lines independently.
func (spool *Spool) Unacked() ([]model.RawLog, error) {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	if spool.file == nil {
		return nil, errSpoolClosed
	}
	if err := recoverAppend(spool.file, spool.path); err != nil {
		return nil, err
	}
	if _, err := spool.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	var logs []model.RawLog
	reader := bufio.NewReader(spool.file)
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break // An incomplete record is never replayable.
		}
		if err != nil {
			return nil, err
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var log model.RawLog
		if err := json.Unmarshal(line, &log); err != nil {
			slog.Warn("skipping malformed spool line", "error", err)
			continue
		}
		if !spool.acked[log.LogID] {
			logs = append(logs, log)
		}
	}
	return logs, nil
}

// Truncates only when every known spooled ID has been acknowledged.
func (spool *Spool) Compact() {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	if spool.file == nil || spool.ackFile == nil || len(spool.spooled) != 0 {
		return
	}
	if recoverAppend(spool.file, spool.path) != nil || recoverAppend(spool.ackFile, spool.ackPath) != nil {
		return
	}
	// Persist removal of the logs before removing their acknowledgements.
	if err := spool.file.Truncate(0); err != nil {
		return
	}
	if err := spool.file.Sync(); err != nil {
		return
	}
	if err := spool.ackFile.Truncate(0); err != nil {
		return
	}
	if err := spool.ackFile.Sync(); err != nil {
		return
	}
	spool.acked = make(map[string]bool)
}

func (spool *Spool) Close() error {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	var errs []error
	if spool.file != nil {
		errs = append(errs, spool.file.Close())
		spool.file = nil
	}
	if spool.ackFile != nil {
		errs = append(errs, spool.ackFile.Close())
		spool.ackFile = nil
	}
	return errors.Join(errs...)
}

func (spool *Spool) loadAcks() error {
	if _, err := spool.ackFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(spool.ackFile)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		if id := scanner.Text(); id != "" {
			spool.acked[id] = true
		}
	}
	return scanner.Err()
}

// Discards a torn trailing line so the next append cannot merge with it.
func truncatePartialLine(file spoolFile) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	end := info.Size()
	buffer := make([]byte, 64*1024)
	for end > 0 {
		start := max(int64(0), end-int64(len(buffer)))
		chunk := buffer[:end-start]
		if _, err := file.ReadAt(chunk, start); err != nil {
			return err
		}
		if index := bytes.LastIndexByte(chunk, '\n'); index >= 0 {
			end = start + int64(index) + 1
			break
		}
		end = start
	}
	if end == info.Size() {
		return nil
	}
	if err := file.Truncate(end); err != nil {
		return err
	}
	return file.Sync()
}

// The journal is persisted before modifying the target and retained until rollback is synced.
func appendDurably(file spoolFile, path string, payload []byte) error {
	if err := recoverAppend(file, path); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	offset := info.Size()
	if err := writeJournal(path, offset); err != nil {
		return err
	}
	n, err := file.Write(payload)
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = file.Sync()
	}
	if err == nil {
		err = os.Remove(path + ".pending")
		if err == nil {
			err = syncDirectory(path)
			if err != nil {
				// Restore the undo record if persisting the commit failed.
				err = errors.Join(err, writeJournal(path, offset))
			}
		}
	}
	if err != nil {
		return errors.Join(err, recoverAppend(file, path))
	}
	return nil
}

func writeJournal(path string, offset int64) error {
	journal, err := os.OpenFile(path+".pending.tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := fmt.Fprintf(journal, "%d\n", offset)
	syncErr := journal.Sync()
	closeErr := journal.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err := os.Rename(path+".pending.tmp", path+".pending"); err != nil {
		return err
	}
	return syncDirectory(path)
}

func recoverAppend(file spoolFile, path string) error {
	payload, err := os.ReadFile(path + ".pending")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	offset, err := strconv.ParseInt(strings.TrimSpace(string(payload)), 10, 64)
	if err != nil || offset < 0 {
		return fmt.Errorf("invalid spool undo journal %q", path)
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if offset > info.Size() {
		return fmt.Errorf("spool undo offset exceeds file size for %q", path)
	}
	if err := file.Truncate(offset); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := os.Remove(path + ".pending"); err != nil {
		return err
	}
	return syncDirectory(path)
}

func syncDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
