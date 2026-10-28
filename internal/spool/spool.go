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

