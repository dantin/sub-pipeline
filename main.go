package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/go-sql-driver/mysql"
)

var db *sql.DB

func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("importer", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.configFile, "config", "", "Config file")
	fs.StringVar(&cfg.tableName, "table", "", "Table name")
	fs.StringVar(&cfg.dataFile, "data", "", "Data file")

	fs.IntVar(&cfg.WorkerCount, "c", 2, "parallel worker count")

	fs.StringVar(&cfg.DBCfg.Host, "h", "127.0.0.1", "set the database host ip")
	fs.StringVar(&cfg.DBCfg.User, "u", "root", "set the database user")
	fs.StringVar(&cfg.DBCfg.Password, "p", "", "set the database password")
	fs.StringVar(&cfg.DBCfg.Name, "D", "test", "set the database name")
	fs.IntVar(&cfg.DBCfg.Port, "P", 3306, "set the database host port")

	return cfg
}

type DBConfig struct {
	Host     string `toml:"host" json:"host"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"password"`
	Name     string `toml:"name" json:"name"`
	Port     int    `toml:"port" json:"port"`
}

func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

type Config struct {
	*flag.FlagSet `json:"-"`

	DBCfg       DBConfig `toml:"db" json:"db"`
	WorkerCount int      `toml:"worker-count" json:"worker-count"`

	configFile string

	tableName string
	dataFile  string
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return err
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return err
	}

	if len(c.FlagSet.Args()) != 0 {
		return fmt.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return err
}

type table struct {
	database   string
	name       string
	columns    map[string]string
	columnList []string
}

func newTable(database string, name string) *table {
	return &table{
		database:   database,
		name:       name,
		columns:    make(map[string]string),
		columnList: make([]string, 0),
	}
}

func (t *table) getColumns(db *sql.DB) error {
	rows, err := db.Query(
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
		t.database,
		t.name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			colName string
			colType string
		)

		if err := rows.Scan(&colName, &colType); err != nil {
			return nil
		}
		t.columns[colName] = colType
	}

	return nil
}

func (t *table) printColumns() string {
	ret := ""
	for col, ct := range t.columns {
		ret += fmt.Sprintf("%v: %v\n", col, ct)
	}

	return ret
}

func (t *table) setColumnList(tokens []string) error {
	for _, col := range tokens {
		if _, ok := t.columns[col]; !ok {
			fmt.Printf("%s not exists in %s, ignore", col, t.name)
			continue
		}
		t.columnList = append(t.columnList, col)
	}
	if len(t.columnList) == 0 {
		return fmt.Errorf("no column found in %s", t.name)
	}
	return nil
}

func (t *table) genDmlStmt(tokens []string) (*dmlStmt, error) {
	var (
		columns      []string
		placeholders []string
		values       []interface{}
	)

	for i, col := range t.columnList {
		columns = append(columns, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		// add values
		var (
			v   string = tokens[i]
			val interface{}
		)

		colType, ok := t.columns[col]
		if v == "" && ok {
			switch colType {
			case "int", "bigint":
				val = 0
			case "decimal":
				val = 0.0
			case "datetime":
				val = sql.NullString{}
			case "char", "varchar":
				val = ""
			default:
				val = ""
			}
		} else {
			//
			if colType == "datetime" {
				val = normDt(v)
			} else {
				val = v
			}
		}
		values = append(values, val)
	}
	sql := fmt.Sprintf(
		"REPLACE INTO `%s` (%s) VALUES (%s)",
		t.name,
		strings.Join(columns[:], ", "),
		strings.Join(placeholders[:], ", "),
	)
	return newDmlStmt(sql, values), nil
}

func normDt(s string) string {
	var ret string = s
	if len(s) == 0 {
		return s
	}
	for len(ret) < 8 {
		ret += "01"
	}
	return ret
}

type dmlStmt struct {
	sql    string
	values []interface{}
}

func newDmlStmt(sql string, values []interface{}) *dmlStmt {
	return &dmlStmt{
		sql:    sql,
		values: values,
	}
}

func doJob(db *sql.DB, id int, jobChan <-chan *dmlStmt, doneChan chan<- struct{}) {
	fmt.Printf("worker %d started\n", id)
	for stmt := range jobChan {
		doInsert(db, stmt)
	}
	doneChan <- struct{}{}
}

func doInsert(db *sql.DB, stmt *dmlStmt) {
	txn, err := db.Begin()
	if err != nil {
		log.Fatal("begin transcation", err)
	}

	_, err = txn.Exec(stmt.sql, stmt.values...)
	if err != nil {
		log.Fatalf("exce %v, %v, %v", stmt.sql, stmt.values, err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal("commit transaction", err)
	}
}

func conn(config DBConfig) (*sql.DB, error) {
	// Capture connection properties.
	cfg := mysql.Config{
		User:   config.User,
		Passwd: config.Password,
		Net:    "tcp",
		Addr:   fmt.Sprintf("%s:%d", config.Host, config.Port),
		DBName: config.Name,
	}
	// Get a database handle.
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}
	fmt.Println("Database Connected!")

	return db, nil
}

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch err {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		fmt.Printf("parse cmd flags, %v", err)
		os.Exit(2)
	}

	db, err := conn(cfg.DBCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	runPipeline(db, cfg)
}

func runPipeline(db *sql.DB, cfg *Config) {
	t := newTable(cfg.DBCfg.Name, cfg.tableName)
	t.getColumns(db)

	jobChan := make(chan *dmlStmt, cfg.WorkerCount+1)
	doneChan := make(chan struct{}, cfg.WorkerCount)
	defer close(doneChan)

	for i := 0; i < cfg.WorkerCount; i++ {
		go doJob(db, i+1, jobChan, doneChan)
	}

	file, err := os.Open(cfg.dataFile)
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

		if lineNo == -1 {
			break
		}
	}

	close(jobChan)

	for i := 0; i < cfg.WorkerCount; i++ {
		<-doneChan
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
