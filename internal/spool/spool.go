package spool

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/johneliud/log_ingestion_engine/internal/model"
)

var (
	errSpoolClosed = errors.New("spool is closed")
)

// Every accepted log is appended and fsynced here before the handler reports acceptance to the client, so a crash can never silently lose an accepted log.
type Spool struct {
	path     string
	ackPath  string
	file     *os.File
	ackFile  *os.File
	acked    map[string]bool
	spooled  int
	ackedCnt int
	mu       sync.Mutex
}

// Returns a spool that appends accepted logs to path and records handled log identifiers in path plus ".acked".
func NewSpool(path string) (*Spool, error) {
	if path == "" {
		return nil, errors.New("spool path must not be empty")
	}

	if dirError := os.MkdirAll(filepath.Dir(path), 0o755); dirError != nil {
		return nil, fmt.Errorf("creating spool directory %q: %w", filepath.Dir(path), dirError)
	}

	file, openError := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if openError != nil {
		return nil, fmt.Errorf("opening spool file %q: %w", path, openError)
	}

	ackPath := path + ".acked"
	ackFile, ackOpenError := os.OpenFile(ackPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if ackOpenError != nil {
		file.Close()
		return nil, fmt.Errorf("opening spool ack file %q: %w", ackPath, ackOpenError)
	}

	spool := &Spool{
		path:    path,
		ackPath: ackPath,
		file:    file,
		ackFile: ackFile,
		acked:   make(map[string]bool),
	}

	if loadError := spool.loadAcks(); loadError != nil {
		file.Close()
		ackFile.Close()
		return nil, fmt.Errorf("loading spool acknowledgements: %w", loadError)
	}

	if countError := spool.countSpooled(); countError != nil {
		file.Close()
		ackFile.Close()
		return nil, fmt.Errorf("counting spooled logs: %w", countError)
	}

	return spool, nil
}

// Writes every log to the spool and fsyncs before returning.
func (spool *Spool) Append(logs []model.RawLog) error {
	if len(logs) == 0 {
		return nil
	}

	spool.mu.Lock()
	defer spool.mu.Unlock()

	if spool.file == nil {
		return errSpoolClosed
	}

	writer := bufio.NewWriterSize(spool.file, 64*1024)
	for _, log := range logs {
		serialized, marshalError := json.Marshal(log)
		if marshalError != nil {
			return fmt.Errorf("marshalling spooled log %q: %w", log.LogID, marshalError)
		}

		serialized = append(serialized, '\n')
		if _, writeError := writer.Write(serialized); writeError != nil {
			return fmt.Errorf("writing spooled log %q: %w", log.LogID, writeError)
		}
		spool.spooled++
	}
	if flushError := writer.Flush(); flushError != nil {
		return fmt.Errorf("flushing spool: %w", flushError)
	}
	if syncError := spool.file.Sync(); syncError != nil {
		return fmt.Errorf("syncing spool: %w", syncError)
	}
	return nil
}

// Marks a log identifier as durably handled so startup replay skips it.
func (spool *Spool) Ack(logID string) {
	spool.mu.Lock()
	defer spool.mu.Unlock()

	if spool.ackFile == nil {
		return
	}
	if spool.acked[logID] {
		return
	}

	if _, writeError := spool.ackFile.WriteString(logID + "\n"); writeError != nil {
		return
	}
	if syncError := spool.ackFile.Sync(); syncError != nil {
		return
	}
	spool.acked[logID] = true
	spool.ackedCnt++
}

// Returns every spooled log that has not been acknowledged yet.
func (spool *Spool) Unacked() ([]model.RawLog, error) {
	spool.mu.Lock()
	defer spool.mu.Unlock()

	if spool.file == nil {
		return nil, errSpoolClosed
	}

	if _, seekError := spool.file.Seek(0, 0); seekError != nil {
		return nil, fmt.Errorf("seeking spool file: %w", seekError)
	}

	var unacked []model.RawLog
	scanner := bufio.NewScanner(spool.file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var rawLog model.RawLog
		if unmarshalError := json.Unmarshal(line, &rawLog); unmarshalError != nil {
			return nil, fmt.Errorf("parsing spooled log: %w", unmarshalError)
		}

		if !spool.acked[rawLog.LogID] {
			unacked = append(unacked, rawLog)
		}
	}
	if scanError := scanner.Err(); scanError != nil {
		return nil, fmt.Errorf("reading spool file: %w", scanError)
	}
	return unacked, nil
}
