package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"syscall"
)

// in bytes
const (
	PAGE_SIZE        = 1024      // 1 kb
	MAX_BLOCK_SIZE   = 104857600 // 100 mb
	MAX_TUPLE_SIZE   = 100       // 100 bytes
	PAGE_HEADER_SIZE = 8         // for uint64
)

type Pager struct {
	blockpath string
	fd        int
}

func NewPager(blockPath string) *Pager {

	fd, err := syscall.Open(blockPath, 0x4000|syscall.O_RDWR, 0664)
	if err != nil {
		panic(err)
	}

	return &Pager{
		blockpath: blockPath,
		fd:        fd,
	}
}

func (self *Pager) ReadFD(offset int64, limit int64) ([]byte, error) {

	byteData := make([]byte, limit)

	if _, err := syscall.Seek(self.fd, offset, io.SeekCurrent); err != nil {
		return nil, err
	}

	if _, err := syscall.Read(self.fd, byteData); err != nil {
		return nil, err
	}

	if _, err := syscall.Seek(self.fd, io.SeekStart, io.SeekStart); err != nil {
		return nil, err
	}

	return byteData, nil
}

func (self *Pager) Rewrite(offset int64, data []byte) error {
	if _, err := syscall.Seek(self.fd, offset, io.SeekCurrent); err != nil {
		return err
	}

	if _, err := syscall.Write(self.fd, data); err != nil {
		return err
	}

	if _, err := syscall.Seek(self.fd, io.SeekStart, io.SeekStart); err != nil {
		return err
	}

	return nil
}

func (self *Pager) GetLastPageStartOffset() (int64, error) {

	st := syscall.Stat_t{}
	if err := syscall.Stat(self.blockpath, &st); err != nil {
		return -1, err
	}

	return st.Size - PAGE_SIZE, nil
}

func (self *Pager) InsertRowData(rowData []byte) error {
	if len(rowData) > MAX_TUPLE_SIZE {
		return errors.New("MAX TUPLE SIZE")
	}

	currentPagePointer, err := self.GetLastPageStartOffset()
	if err != nil {
		return err
	}

	currentPage, err := self.ReadFD(currentPagePointer, PAGE_SIZE)
	if err != nil {
		return err
	}

	var totalRowExist int64
	binary.Read(bytes.NewReader(currentPage[0:PAGE_HEADER_SIZE]), binary.LittleEndian, &totalRowExist)

	if (totalRowExist*MAX_TUPLE_SIZE)+PAGE_HEADER_SIZE+MAX_TUPLE_SIZE > PAGE_SIZE {
		currentPagePointer += PAGE_SIZE

		if err := self.Rewrite(currentPagePointer, make([]byte, PAGE_SIZE)); err != nil {
			return err
		}

		currentPage, err = self.ReadFD(currentPagePointer, PAGE_SIZE)
		if err != nil {
			return err
		}
		totalRowExist = 0
	}

	// do manipulation
	// pad data to 100 bytes
	rowDataResized := make([]byte, 100)
	for x := 0; x < len(rowDataResized); x++ {
		rowDataResized[x] = byte('#')
	}
	for x := 0; x < len(rowData); x++ {
		rowDataResized[x] = rowData[x]
	}

	availableRowPointer := int64(totalRowExist*MAX_TUPLE_SIZE) + PAGE_HEADER_SIZE

	totalRowExist += 1
	var headerInBytes bytes.Buffer

	if err := binary.Write(&headerInBytes, binary.LittleEndian, totalRowExist); err != nil {
		return err
	}

	copy(currentPage[0:PAGE_HEADER_SIZE], headerInBytes.Bytes())

	copy(currentPage[availableRowPointer:int(availableRowPointer)+len(rowDataResized)], rowDataResized)

	if err := self.Rewrite(currentPagePointer, currentPage); err != nil {
		return err
	}

	return nil

}

func (self *Pager) UpdateRowData(pageOffset int64, nRow int64, newRow []byte) error {

	currentPage, err := self.ReadFD(pageOffset*PAGE_SIZE, PAGE_SIZE)
	if err != nil {
		return err
	}

	var totalRowExist int64
	fmt.Println("==========", binary.Read(bytes.NewReader(currentPage[0:PAGE_HEADER_SIZE]), binary.LittleEndian, &totalRowExist))

	// skip row total
	pageHeader := currentPage[0:PAGE_HEADER_SIZE]

	rowDataResized := make([]byte, 100)
	for x := 0; x < len(rowDataResized); x++ {
		rowDataResized[x] = byte('*')
	}
	for x := 0; x < len(newRow); x++ {
		rowDataResized[x] = newRow[x]
	}
	copy(currentPage[:PAGE_HEADER_SIZE], pageHeader)
	copy(currentPage[nRow*MAX_TUPLE_SIZE+PAGE_HEADER_SIZE:nRow*MAX_TUPLE_SIZE+MAX_TUPLE_SIZE+PAGE_HEADER_SIZE], rowDataResized)

	if err := self.Rewrite(pageOffset*PAGE_SIZE, currentPage); err != nil {
		return err
	}

	return nil
}

func (self *Pager) ReadPages(offset int64, limit int64) {

	// iterate pages
	pagePointer := 0
	for x := offset; x < offset+limit; x++ {

		page, _ := self.ReadFD(x*PAGE_SIZE, PAGE_SIZE)
		if len(page) <= 0 {
			fmt.Println("=====End of Page")
			break
		}

		var totalRowExist int64
		fmt.Println("==========", binary.Read(bytes.NewReader(page[0:PAGE_HEADER_SIZE]), binary.LittleEndian, &totalRowExist))

		page = page[PAGE_HEADER_SIZE:]
		currRow := 0
		for currRow < int(totalRowExist) {

			fmt.Println(string(page[currRow*MAX_TUPLE_SIZE : currRow*MAX_TUPLE_SIZE+MAX_TUPLE_SIZE]))
			currRow += 1
		}

		pagePointer += PAGE_SIZE

	}

}
