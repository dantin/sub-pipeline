package importer

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// Importer represents a standalone importer application.
type Importer struct {
	cfg *Config
}

// NewImporter create a runnable application by given configuration.
func NewImporter(cfg *Config) *Importer {
	return &Importer{
		cfg: cfg,
	}
}

// Run starts application.
func (app *Importer) Run() error {
	db, err := app.conn()
	if err != nil {
		return err
	}
	defer db.Close()

	fmt.Println("Database Connected!")
	// run pipeline.
	return app.runPipeline(db)
}

// conn returns a MySQL database connection.
func (app *Importer) conn() (*sql.DB, error) {
	// Capture connection properties.
	cfg := mysql.Config{
		User:   app.cfg.DBCfg.User,
		Passwd: app.cfg.DBCfg.Password,
		Net:    "tcp",
		Addr:   fmt.Sprintf("%s:%d", app.cfg.DBCfg.Host, app.cfg.DBCfg.Port),
		DBName: app.cfg.DBCfg.Name,
	}
	// Get a database handle.
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}

	// Test databse connection.
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

// runPipeline starts pipeline.
func (app *Importer) runPipeline(db *sql.DB) error {
	t := newTable(app.cfg.DBCfg.Name, app.cfg.tableName)
	t.getColumns(db)

	jobChan := make(chan *dmlStmt, app.cfg.WorkerCount+1)
	doneChan := make(chan struct{}, app.cfg.WorkerCount)

	defer close(doneChan)

	for i := 0; i < app.cfg.WorkerCount; i++ {
		go doJob(db, i+1, jobChan, doneChan)
	}

	app.doLoop(t, jobChan)

	close(jobChan)

	for i := 0; i < app.cfg.WorkerCount; i++ {
		<-doneChan
	}

	return nil
}

// doLoop read data file, parses each line, dispatches DML statement to workers.
func (app *Importer) doLoop(t *table, jobChan chan<- *dmlStmt) {
	file, err := os.Open(app.cfg.dataFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	lineNo := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		lineNo++

		tokens := strings.Split(line, "$")

		if lineNo == 1 {
			err := t.setColumnList(tokens)
			if err != nil {
				log.Fatal(err)
			}
			continue
		}

		stmt, err := t.genDmlStmt(tokens)
		if err != nil {
			continue
		}

		jobChan <- stmt
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
