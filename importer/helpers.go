package importer

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

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
