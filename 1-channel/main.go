package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type City struct {
	Name  string
	Min   int
	Max   int
	Avg   float32
	Count int
}

func producer(scanner *bufio.Scanner) <-chan string {
	dataStream := make(chan string)
	go func() {
		defer close(dataStream)
		for scanner.Scan() {
			dataStream <- scanner.Text()
		}
	}()

	return dataStream
}

func consumer(
	cities map[string]City,
	dataStream <-chan string,
	wg *sync.WaitGroup,
	mu *sync.Mutex,
) {
	defer wg.Done()
	mu.Lock()
	defer mu.Unlock()

	for line := range dataStream {
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
				Name:  cityName,
				Min:   num,
				Max:   num,
				Avg:   float32(num),
				Count: 0,
			}

			cities[cityName] = *newCity
		}
	}
}

func main() {
	ts := time.Now()

	// chunkSize := flag.Int("chunkSize", 10, "chunk size")
	flag.Parse()

	file, err := os.Open("input/measurements_medium.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	cities := make(map[string]City)

	scanner := bufio.NewScanner(file)
	dataStream := producer(scanner)

	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go consumer(cities, dataStream, &wg, &mu)
	}

	wg.Wait()

	fmt.Println(time.Since(ts))
}

func parseLine(line string) (string, int) {
	lineSplit := strings.Split(line, ";")

	city := lineSplit[0]
	temp := lineSplit[1]
	tempStr := strings.Replace(temp, ".", "", -1)
	num, _ := strconv.Atoi(tempStr)

	return city, num
}
