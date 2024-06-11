package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/go-sql-driver/mysql"
)

func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("importer", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.configFile, "config", "", "Config file")

	fs.IntVar(&cfg.WorkerCount, "c", 2, "parallel worker count")

	fs.StringVar(&cfg.DBCfg.Host, "h", "127.0.0.1", "set the database host ip")
	fs.StringVar(&cfg.DBCfg.User, "u", "root", "set the database user")
	fs.StringVar(&cfg.DBCfg.Password, "p", "", "set the database password")
	fs.StringVar(&cfg.DBCfg.Name, "D", "test", "set the database name")
	fs.IntVar(&cfg.DBCfg.Port, "P", 3306, "set the database host port")

	return cfg
}

type deleteToken struct {
	primaryId   interface{}
	caseId      interface{}
	caseVersion interface{}
	fdaDt       interface{}
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

func doJob(db *sql.DB, jobChan <-chan int64, deleteChan chan<- deleteToken, doneChan chan<- struct{}) {
	for caseId := range jobChan {
		fmt.Printf("processing caseid %d\n", caseId)
		tokens := doClean(db, caseId)
		var prev deleteToken
		for _, token := range tokens {
			if prev == token {
				continue
			}
			prev = token
			deleteChan <- token
		}
	}
	doneChan <- struct{}{}
}

/*
func countCaseSize(db *sql.DB, i int64) int {
	var count int
	stmt := `
		SELECT COUNT(*)
      	  FROM demo d
		 WHERE d.caseid = ?`
	err := db.QueryRow(stmt, i).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	return count
}
*/

func doRemove(db *sql.DB, removeChan chan deleteToken, doneChan chan<- struct{}) {
	for t := range removeChan {
		// fmt.Printf("delete demo@primaryid='%v', caseid='%v', caseversion='%v', fda_dt='%v'\n", t.primaryId, t.caseId, t.caseVersion, t.fdaDt)
		txn, err := db.Begin()
		if err != nil {
			log.Fatal("begin transcation", err)
		}
		_, err = db.Exec(`DELETE FROM demo WHERE primaryid = ? AND caseid = ? AND caseversion = ? AND fda_dt = ?`,
			t.primaryId, t.caseId, t.caseVersion, t.fdaDt)
		if err != nil {
			fmt.Printf("fail to delete primaryid='%v', caseid='%v', caseversion='%v', fda_dt='%v', %v\n", t.primaryId, t.caseId, t.caseVersion, t.fdaDt, err)
		}
		err = txn.Commit()
		if err != nil {
			log.Fatal("commit transaction", err)
		}
	}
	doneChan <- struct{}{}
}

func doClean(db *sql.DB, i int64) []deleteToken {
	stmt := `
		SELECT primaryid, caseid, caseversion, fda_dt
      	  FROM demo d
		 WHERE d.caseid = ?
   	  ORDER BY d.fda_dt desc, d.primaryid desc`

	rows, err := db.Query(stmt, i)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	retval := make([]deleteToken, 0)
	seq := 0
	for rows.Next() {
		var (
			primaryId   int64
			caseId      int64
			caseVersion int
			fdaDt       string
		)

		if err := rows.Scan(&primaryId, &caseId, &caseVersion, &fdaDt); err != nil {
			log.Fatal(err)
		}

		if seq > 0 {
			retval = append(retval, deleteToken{primaryId, caseId, caseVersion, fdaDt})
		}

		seq++
	}

	return retval
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

	jobChan := make(chan int64, cfg.WorkerCount+1)
	deleteChan := make(chan deleteToken, 1)
	doneChan := make(chan struct{}, cfg.WorkerCount)
	defer close(doneChan)
	defer close(deleteChan)

	for i := 0; i < cfg.WorkerCount; i++ {
		fmt.Printf("worker %d started\n", i+1)
		go doJob(db, jobChan, deleteChan, doneChan)
	}
	for i := 0; i < 1; i++ {
		fmt.Printf("cleaner %d started\n", i+1)
		go doRemove(db, deleteChan, doneChan)
	}

	stmt := `
        SELECT
		  caseid, COUNT(*) c
		FROM demo
		GROUP BY caseid
		HAVING c > ?
		ORDER BY caseid ASC`

	rows, err := db.Query(stmt, 1)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			caseId int64
			count  int
		)

		if err := rows.Scan(&caseId, &count); err != nil {
			log.Fatal(err)
		}

		jobChan <- caseId
	}

	close(jobChan)

	for i := 0; i < 1+cfg.WorkerCount; i++ {
		<-doneChan
	}
}
