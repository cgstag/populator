package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Pallinder/go-randomdata"

	"github.com/google/uuid"
)

var size = flag.Int("size", 8, "file size in GiB")
var file = flag.String("file", "accounts.csv", "filename and format")

func main() {

	// You can get individual args with normal indexing.
	flag.Parse()
	fSize := int64(*size)
	fFilename := string(*file)

	err := populate(fSize, fFilename)
	if err != nil {
		fmt.Fprintln(os.Stderr, fSize, err)
	}
}


func populate(fSize int64, fFilename string) error {

	var wg sync.WaitGroup
	accountChannel := make(chan string,fSize)

	// Write to File
	wg.Add(1)
	// Generate Data
	go func(size int64) {
		for i := int64(0); i < fSize; i++ {
			data := randomAccount()
			accountChannel <- data
		}
		wg.Done()
	}(fSize)

	fName := `/tmp/` + fFilename // test file
	//defer os.Remove(fName)
	f, err := os.Create(fName)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	start := time.Now()
	written := int64(0)
	fmt.Printf("Prepared to write %d lines", fSize)

	// Write to File
	wg.Add(1)
	go func() {
		for i := int64(0); i < fSize; i++ {
			nextWrite := <- accountChannel //read from oreChannel
			nn, err := w.WriteString(nextWrite)
			written += int64(nn)
			if err != nil {
				panic(err)
			}
			fmt.Println(nextWrite)
		}
		wg.Done()
	}()

	wg.Wait()

	err = w.Flush()
	if err != nil {
		return err
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	since := time.Since(start)

	err = f.Close()
	if err != nil {
		return err
	}
	fmt.Printf("written: %dB %dns %.2fGB %.2fs %.2fMB/s\n",
		written, since,
		float64(written)/1000000000, float64(since)/float64(time.Second),
		(float64(written)/1000000)/(float64(since)/float64(time.Second)),
	)

return nil
}

func randomAccount() string{
	var str strings.Builder
	str.WriteString(uuid.New().String())
	str.WriteString(randomdata.FirstName(2))
	str.WriteString(randomdata.LastName())
	str.WriteString(time.Now().Format(time.UnixDate))
	segment:= strconv.Itoa(rand.Intn(3))
	str.WriteString(segment)
	segmentType := ""
	switch segment {
	case "0":
		segmentType = "Varejo"
	case "1":
		segmentType = "Uniclass"
	case "2":
		segmentType = "Personalité"
	}
	str.WriteString(segmentType)
	integerPart := strconv.Itoa(rand.Intn(20000))
	decimal := rand.Intn(99)
	decimalString := ""
	if decimal < 10 {
		decimalString = "0" + strconv.Itoa(decimal)
	} else {
		decimalString = strconv.Itoa(decimal)
	}
	str.WriteString(integerPart+"."+decimalString)
	str.WriteString("\n")
	return str.String()
}



