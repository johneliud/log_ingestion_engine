package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	_ "modernc.org/sqlite"
)

const createLogsTableStatement = `
CREATE TABLE IF NOT EXISTS logs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	timestamp TEXT NOT NULL,
	service TEXT NOT NULL,
	level TEXT NOT NULL,
	message TEXT NOT NULL,
	received_at TEXT NOT NULL,
	source_ip TEXT NOT NULL,
	env TEXT NOT NULL
)`

const insertLogStatement = `
INSERT INTO logs (timestamp, service, level, message, received_at, source_ip, env)
VALUES (?, ?, ?, ?, ?, ?, ?)`

const receivedAtLayout = "2006-01-02T15:04:05.000000Z07:00"

// Represents one destination database and the statement used to append logs to it.
type destinationDatabase struct {
	mutex           sync.Mutex
	database        *sql.DB
	insertStatement *sql.Stmt
}

// Stores enriched logs in one SQLite database per destination.
type SQLiteLogStore struct {
	databases map[model.Destination]*destinationDatabase
}

