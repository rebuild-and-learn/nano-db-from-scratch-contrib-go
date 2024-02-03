package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {

	pager := NewPager("hello")

	fmt.Print("nano-db >> ")
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		input := scanner.Text()

		switch strings.ToUpper(strings.Split(input, " ")[0]) {
		case "INIT":
			f, _ := os.OpenFile("hello", os.O_WRONLY, 0664)
			f.Write(make([]byte, PAGE_SIZE))
			f.Sync()
			f.Close()

		case "INSERT":

			data := strings.Split(input, " ")[1]
			n, _ := strconv.Atoi(strings.Split(input, " ")[2])

			for x := 0; x < n; x++ {

				id := strconv.Itoa(time.Now().Nanosecond())

				err := pager.InsertRowData([]byte(fmt.Sprintf("%s) %s", id, data)))
				if err != nil {
					fmt.Println(err)
				}
			}
			break
		case "FROM":
			offset, _ := strconv.Atoi(strings.Split(input, " ")[1])
			limit, _ := strconv.Atoi(strings.Split(input, " ")[2])

			pager.ReadPages(int64(offset), int64(limit))

			break
		case "UPDATE":
			pageOffset, _ := strconv.Atoi(strings.Split(input, " ")[1])
			nRow, _ := strconv.Atoi(strings.Split(input, " ")[2])
			newRow := strings.Split(input, " ")[3]

			pager.UpdateRowData(int64(pageOffset), int64(nRow), []byte(newRow))

			break
		case "DELETE":

			pageOffset, _ := strconv.Atoi(strings.Split(input, " ")[1])
			nRow, _ := strconv.Atoi(strings.Split(input, " ")[2])

			pager.UpdateRowData(int64(pageOffset), int64(nRow), make([]byte, MAX_TUPLE_SIZE))

			break
		default:
			fmt.Println("~")
			break
		}

		fmt.Print("nano-db >> ")
	}
}
