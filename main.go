package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"reflect"
	"time"
)

var sqlConfig string

type Output struct {
	items     [][]string
	maxLength []int
}

func NewOutput() *Output {
	o := new(Output)
	o.items = make([][]string, 0)
	return o
}

func init() {
	flag.StringVar(&sqlConfig, "mysql", "", "")
}

func main() {
	flag.Parse()
	if len(sqlConfig) == 0 {
		log.Fatal("mysql param is missing")
	}
	fmt.Println("sqlconfig:\n" + sqlConfig)
	db, err := sql.Open("mysql", sqlConfig)
	if err != nil {
		panic(err)
	}
	fmt.Println("connected")
	defer db.Close()
	// See "Important settings" section.
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("input sql statements:")
		if sqlstate, e := reader.ReadString('\n'); e == nil {
			query(db, sqlstate)
		}
	}
}

func query(db *sql.DB, stat string) {
	fmt.Printf("query:\n%s\n", stat)
	rows, err := db.Query(stat)
	if err != nil {
		log.Println(err)
		return
	}
	o := NewOutput()
	colums, err := rows.Columns()
	if err != nil {
		log.Println(err)
	}
	o.items = append(o.items, colums)
	columstypes, err := rows.ColumnTypes()
	if err != nil {
		log.Println(err)
	}
	for rows.Next() {
		items := make([]interface{}, len(colums))
		for index, column := range colums {
			ctype := columstypes[index]
			switch ctype.ScanType().Kind() {
			case reflect.String:
				var t string
				items[index] = &t
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				var val int
				items[index] = &val
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				var val int
				items[index] = &val
			case reflect.Slice, reflect.Struct:
				t := make([]byte, length(ctype))
				items[index] = &t
			default:
				log.Println("not handle", column, ctype.ScanType().Kind())
			}
		}
		err = rows.Scan(items...)
		if err != nil {
			fmt.Printf("scan error:%s\n", err)
		}
		descs := make([]string, len(colums))
		for i, t := range items {
			var desc string
			switch t.(type) {
			case *int:
				desc = fmt.Sprintf("%d", *t.(*int))
			case *[]uint8:
				s := t.(*[]uint8)
				desc = string(*s)
			}
			descs[i] = desc
		}
		o.items = append(o.items, descs)
	}
	printfOutput(o)
}

func printfOutput(o *Output) {
	o.maxLength = make([]int, len(o.items[0]))
	if len(o.maxLength) == 0 {
		return
	}
	for _, value := range o.items {
		for j, v := range value {
			if o.maxLength[j] < len(v) {
				o.maxLength[j] = len(v)
			}
		}
	}

	for _, value := range o.items {
		for j, v := range value {
			f := fmt.Sprintf("| %%-%ds ", o.maxLength[j])
			fmt.Printf(f, v)
		}
		fmt.Println("|")
	}
}

func length(columnType *sql.ColumnType) int {
	switch columnType.DatabaseTypeName() {
	case "DATETIME":
		return 19
	default:
		return 255
	}
}
