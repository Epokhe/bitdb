package main

import (
	"fmt"
	"os"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	fmt.Println("Initializing db!")

	dbname := "main.db"
	writer, err := os.OpenFile(dbname, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	check(err)
	defer writer.Close()

	err = set(writer, "13", "val2")
	check(err)

	err = set(writer, "42", "test")
	check(err)

	data, err := get(dbname, "13")
	fmt.Println(data, err)

	data, err = get(dbname, "14")
	fmt.Println(data, err)

}
