package storage

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/johneliud/log_ingestion_engine/internal/config"
	"github.com/johneliud/log_ingestion_engine/internal/model"
	_ "modernc.org/sqlite"
)

const createLogsTableStatement = `
CREATE TABLE IF NOT EXISTS logs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	log_id TEXT,
	timestamp TEXT NOT NULL,
	service TEXT NOT NULL,
	level TEXT NOT NULL,
	message TEXT NOT NULL,
	received_at TEXT NOT NULL,
	source_ip TEXT NOT NULL,
	env TEXT NOT NULL
)`

const logIDColumnName = "log_id"

const createLogIDUniqueIndexStatement = `
CREATE UNIQUE INDEX IF NOT EXISTS logs_log_id_unique ON logs (log_id)`

const insertLogStatement = `
INSERT INTO logs (timestamp, service, level, message, received_at, source_ip, env, log_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (log_id) DO NOTHING`

const receivedAtLayout = "2006-01-02T15:04:05.000000Z07:00"

// Builds the statement adding the log_id column to logs tables created before it existed.
func migrateLogIDColumnStatement() string {
	return "ALTER TABLE logs ADD COLUMN " + logIDColumnName + " TEXT"
}

// Represents one destination database and the statement used to append logs to it.
type destinationDatabase struct {
	mutex           sync.Mutex
	database        *sql.DB
	insertStatement *sql.Stmt
}

// Stores enriched logs in one SQLite database per destination.
type SQLiteLogStore struct {
	mutex     sync.Mutex
	databases map[model.Destination]*destinationDatabase
	isClosed  bool
}

// Returns a store with one database opened per destination, creating any missing files and tables.
func NewSQLiteLogStore(applicationConfiguration config.ApplicationConfiguration) (*SQLiteLogStore, error) {
	if directoryCreationError := os.MkdirAll(applicationConfiguration.DatabaseDirectoryPath, 0o755); directoryCreationError != nil {
		return nil, fmt.Errorf("creating database directory %q: %w", applicationConfiguration.DatabaseDirectoryPath, directoryCreationError)
	}

	databaseFileNames := map[model.Destination]string{
		model.DestinationService1: config.DatabaseFileService1Name,
		model.DestinationService2: config.DatabaseFileService2Name,
		model.DestinationService3: config.DatabaseFileService3Name,
	}

	databases := make(map[model.Destination]*destinationDatabase, len(databaseFileNames))
	for destination, databaseFileName := range databaseFileNames {
		openedDatabase, openError := openDestinationDatabase(applicationConfiguration.DatabaseDirectoryPath, databaseFileName, applicationConfiguration.DatabaseBusyTimeout.Milliseconds())
		if openError != nil {
			closeDestinationDatabases(databases)
			return nil, openError
		}
		databases[destination] = openedDatabase
	}
	return &SQLiteLogStore{databases: databases}, nil
}

// Opens one destination database, enables WAL, creates the logs table, migrates it and prepares the insert statement.
func openDestinationDatabase(databaseDirectoryPath string, databaseFileName string, busyTimeoutInMilliseconds int64) (*destinationDatabase, error) {
	databasePath := filepath.Join(databaseDirectoryPath, databaseFileName)
	dataSourceQuery := url.Values{}
	dataSourceQuery.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeoutInMilliseconds))
	dataSourceQuery.Add("_pragma", "journal_mode(WAL)")
	dataSourceQuery.Add("_pragma", "synchronous(FULL)")
	dataSourceName := (&url.URL{Scheme: "file", Opaque: databasePath, RawQuery: dataSourceQuery.Encode()}).String()

	database, openError := sql.Open("sqlite", dataSourceName)
	if openError != nil {
		return nil, fmt.Errorf("opening database %q: %w", databasePath, openError)
	}
	database.SetMaxOpenConns(1)

	if _, tableCreationError := database.Exec(createLogsTableStatement); tableCreationError != nil {
		database.Close()
		return nil, fmt.Errorf("creating logs table in database %q: %w", databasePath, tableCreationError)
	}

	if !tableHasColumn(database, "logs", logIDColumnName) {
		if _, migrationError := database.Exec(migrateLogIDColumnStatement()); migrationError != nil {
			database.Close()
			return nil, fmt.Errorf("migrating logs table in database %q: %w", databasePath, migrationError)
		}
	}

	if _, indexCreationError := database.Exec(createLogIDUniqueIndexStatement); indexCreationError != nil {
		database.Close()
		return nil, fmt.Errorf("creating the log id unique index in database %q: %w", databasePath, indexCreationError)
	}

	insertStatement, prepareError := database.Prepare(insertLogStatement)
	if prepareError != nil {
		database.Close()
		return nil, fmt.Errorf("preparing insert statement for database %q: %w", databasePath, prepareError)
	}
	return &destinationDatabase{database: database, insertStatement: insertStatement}, nil
}

// Writes every log for one destination inside a single transaction.
func (sqliteLogStore *SQLiteLogStore) StoreBatch(destination model.Destination, enrichedLogs []model.EnrichedLog) error {
	destinationDatabase, destinationExists := sqliteLogStore.databases[destination]
	if !destinationExists {
		return fmt.Errorf("no database configured for destination %q", destination)
	}

	destinationDatabase.mutex.Lock()
	defer destinationDatabase.mutex.Unlock()

	transaction, transactionError := destinationDatabase.database.Begin()
	if transactionError != nil {
		return fmt.Errorf("beginning transaction for destination %q: %w", destination, transactionError)
	}

	transactionInsertStatement := transaction.Stmt(destinationDatabase.insertStatement)
	for _, enrichedLog := range enrichedLogs {
		_, insertError := transactionInsertStatement.Exec(
			enrichedLog.Timestamp,
			enrichedLog.Service,
			string(enrichedLog.Level),
			enrichedLog.Message,
			enrichedLog.ReceivedAt.UTC().Format(receivedAtLayout),
			enrichedLog.SourceIP,
			enrichedLog.Environment,
			enrichedLog.LogID,
		)
		if insertError != nil {
			transaction.Rollback()
			return fmt.Errorf("inserting log into destination %q database: %w", destination, insertError)
		}
	}

	if commitError := transaction.Commit(); commitError != nil {
		return fmt.Errorf("committing transaction for destination %q: %w", destination, commitError)
	}
	return nil
}

// Closes every open destination database and its insert statement.
func (sqliteLogStore *SQLiteLogStore) Close() error {
	sqliteLogStore.mutex.Lock()
	defer sqliteLogStore.mutex.Unlock()

	if sqliteLogStore.isClosed {
		return nil
	}
	sqliteLogStore.isClosed = true

	var firstCloseError error
	for _, destinationDatabase := range sqliteLogStore.databases {
		if closeError := destinationDatabase.insertStatement.Close(); closeError != nil && firstCloseError == nil {
			firstCloseError = closeError
		}
		if closeError := destinationDatabase.database.Close(); closeError != nil && firstCloseError == nil {
			firstCloseError = closeError
		}
	}
	return firstCloseError
}

func closeDestinationDatabases(databases map[model.Destination]*destinationDatabase) {
	for _, destinationDatabase := range databases {
		destinationDatabase.insertStatement.Close()
		destinationDatabase.database.Close()
	}
}

// Reports whether a table has the named column in its schema.
func tableHasColumn(database *sql.DB, tableName string, columnName string) bool {
	columnNames, columnError := queryColumnNames(database, tableName)
	if columnError != nil {
		return false
	}
	return slices.Contains(columnNames, columnName)
}

// Returns every column name of a table, or an error when the table does not exist.
func queryColumnNames(database *sql.DB, tableName string) ([]string, error) {
	rows, rowsError := database.Query(fmt.Sprintf("SELECT name FROM pragma_table_info(%q)", tableName))
	if rowsError != nil {
		return nil, fmt.Errorf("reading the schema of table %q: %w", tableName, rowsError)
	}
	defer rows.Close()

	var columnNames []string
	for rows.Next() {
		var columnName string
		if scanError := rows.Scan(&columnName); scanError != nil {
			return nil, fmt.Errorf("scanning the schema of table %q: %w", tableName, scanError)
		}
		columnNames = append(columnNames, columnName)
	}
	return columnNames, rows.Err()
}
