package main

import (
	"fmt"
	"github.com/epokhe/lsm-tree/db"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	fmt.Println("Initializing db!")

	testDb, err := db.Open("main.db")
	defer testDb.Close()

	err = testDb.Set("13", "val2")
	check(err)

	err = testDb.Set("42", "test")
	check(err)

	data, err := testDb.Get("13")
	fmt.Println(data, err)

	data, err = testDb.Get("14")
	fmt.Println(data, err)

}
