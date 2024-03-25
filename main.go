package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type City struct {
	Min   int
	Max   int
	Avg   float32
	Count int
}

func producer(filePath string, chunkSize int) <-chan []byte {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}

	dataStream := make(chan []byte)

	reader := bufio.NewReader(file)
	buffer := make([]byte, chunkSize)
	var remainder []byte

	go func() {
		defer close(dataStream)
		defer file.Close()

		for {
			bytesRead, err := reader.Read(buffer)
			if err != nil && err != io.EOF {
				panic(err)
			}

			if bytesRead == 0 {
				break
			}

			// Append remainder from previous chunk
			chunk := append(remainder, buffer[:bytesRead]...)

			// Find the last newline character in the chunk
			lastNewlineIndex := bytesLastIndex(chunk, []byte("\n"))

			if lastNewlineIndex != -1 {
				// Process or save the chunk until the last newline
				// fmt.Println(string(chunk[:lastNewlineIndex]))
				dataStream <- chunk[:lastNewlineIndex]
				// Store the remainder for the next iteration
				remainder = chunk[lastNewlineIndex+1:]
			} else {
				// If no newline found, save the entire chunk as remainder
				remainder = chunk
			}
		}

		// Process or save the remainder if any
		if len(remainder) > 0 {
			dataStream <- remainder
		}
	}()

	return dataStream
}

func consumer(
	dataStream <-chan []byte,
	wg *sync.WaitGroup,
) <-chan map[string]*City {

	cities := make(map[string]*City)
	outputStream := make(chan map[string]*City, 1)

	go func() {
		defer wg.Done()
		defer close(outputStream)

		for batch := range dataStream {
			for _, line := range strings.Split(string(batch), "\n") {
				cityName, num := parseLine(line)

				if city, ok := cities[cityName]; ok {
					newCity := city
					if city.Max < num {
						newCity.Max = num
					}
					if city.Min > num {
						newCity.Min = num
					}

					newCity.Avg = (city.Avg*float32(city.Count) + float32(num)) / (float32(city.Count) + 1)

					newCity.Count += 1

					cities[cityName] = newCity

				} else {
					newCity := &City{
						Min:   num,
						Max:   num,
						Avg:   float32(num),
						Count: 1,
					}

					cities[cityName] = newCity
				}
			}
		}
		outputStream <- cities
	}()

	return outputStream
}

func aggregate(aggregateChannels []<-chan map[string]*City) {

	cities := make(map[string]City)

	for i := 0; i < len(aggregateChannels); i++ {
		for name, c := range <-aggregateChannels[i] {
			if city, ok := cities[name]; ok {
				newCity := cities[name]

				if city.Max > cities[name].Max {
					newCity.Max = city.Max
				}

				if city.Min < cities[name].Min {
					newCity.Min = city.Min
				}

				newCity.Avg = (cities[name].Avg + city.Avg) / (float32(cities[name].Count) + float32(city.Count))

				cities[name] = newCity
			} else {
				cities[name] = City{
					Min:   c.Min,
					Max:   c.Max,
					Avg:   c.Avg,
					Count: c.Count,
				}
			}
		}
	}
}

func main() {
	ts := time.Now()

	CHUNK_SIZE := 500000
	WORKERS := 200

	dataStream := producer("input/measurements_medium.txt", CHUNK_SIZE)
	inputChannels := make([]chan []byte, WORKERS)
	aggregateChannels := make([]<-chan map[string]*City, WORKERS)

	var wg sync.WaitGroup
	wg.Add(WORKERS)

	for i := 0; i < WORKERS; i++ {
		// create n workers, each worker reads data from it's inputStream, calclulates City
		// then feeds the citiesMap into outputStream, which will be then collected by aggregate func
		inputStream := make(chan []byte)
		outputStream := consumer(inputStream, &wg)

		inputChannels[i] = inputStream
		aggregateChannels[i] = outputStream
	}

	var index int
	for batch := range dataStream {
		inputChannels[index] <- batch

		index++
		if index >= WORKERS {
			index = 0
		}
	}

	for i := 0; i < WORKERS; i++ {
		close(inputChannels[i])
	}
	wg.Wait()

	aggregate(aggregateChannels)

	fmt.Println(time.Since(ts))
}

// bytesLastIndex finds the index of the last occurrence of sep in s.
// If sep is not found, it returns -1.
func bytesLastIndex(s, sep []byte) int {
	for i := len(s) - len(sep); i >= 0; i-- {
		if bytes.Equal(s[i:i+len(sep)], sep) {
			return i
		}
	}
	return -1
}

func parseLine(line string) (string, int) {
	lineSplit := strings.Split(line, ";")

	city := lineSplit[0]
	temp := lineSplit[1]

	tempStr := strings.TrimRight(temp, "\r")
	tempStr = strings.Replace(tempStr, ".", "", -1)

	num, _ := strconv.Atoi(tempStr)

	return city, num
}
