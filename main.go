package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

// Function to split the file into chunks
func splitFileIntoChunks(filePath string, chunkSize int) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	buffer := make([]byte, chunkSize)

	for {
		bytesRead, err := reader.Read(buffer)
		if err != nil && err != io.EOF {
			return err
		}

		if bytesRead == 0 {
			break
		}

		// Process or save the chunk
		// fmt.Println(string(buffer[:bytesRead]))
	}

	return nil
}

func readWithBufio(filePath string, batchSize int) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	count := 0
	lines := []string{}

	for scanner.Scan() {
		count++
		lines = append(lines, scanner.Text())

		if count%batchSize == 0 {
			count = 0
			lines = []string{}
		}
	}
}

func main() {
	filePath := "input/measurements.txt" // Replace with your file path
	chunkSize := 128                     // Define your chunk size here

	tChunks := time.Now()
	err := splitFileIntoChunks(filePath, chunkSize)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Chunks: ", time.Since(tChunks))

	tBufio := time.Now()
	readWithBufio(filePath, chunkSize)
	fmt.Println("Bufio: ", time.Since(tBufio))

	fmt.Println("File split into chunks successfully!")
}
