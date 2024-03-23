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

func producer(batchSize int, file *os.File) <-chan []string {
	scanner := bufio.NewScanner(file)
	dataStream := make(chan []string)
	go func() {
		defer close(dataStream)

		count := 0
		lines := []string{}

		for scanner.Scan() {
			count++
			lines = append(lines, scanner.Text())

			if count%batchSize == 0 {
				count = 0
				dataStream <- lines
				lines = []string{}
			}
		}
	}()

	return dataStream
}

func consumer(
	dataStream <-chan []string,
	output chan map[string]*City,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	cities := make(map[string]*City)

	for batch := range dataStream {
		for _, line := range batch {
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
					Count: 1,
				}

				cities[cityName] = newCity
			}
		}
	}
	output <- cities
}

func main() {
	ts := time.Now()

	// chunkSize := flag.Int("chunkSize", 10, "chunk size")
	flag.Parse()

	file, err := os.Open("input/measurements_small.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	BATCH_SIZE := 10
	WORKERS := 2
	dataStream := producer(BATCH_SIZE, file)

	channels := make([]chan []string, WORKERS)
	aggregateChannels := make([]chan map[string]*City, WORKERS)

	var wg sync.WaitGroup
	wg.Add(WORKERS)

	for i := 0; i < WORKERS; i++ {
		input := make(chan []string)
		output := make(chan map[string]*City, 1)

		go consumer(input, output, &wg)

		channels[i] = input
		aggregateChannels[i] = output
	}

	var index int
	for batch := range dataStream {
		channels[index] <- batch

		index++
		if index >= WORKERS {
			index = 0
		}
	}

	for i := 0; i < WORKERS; i++ {
		close(channels[i])
	}
	wg.Wait()
	for i := 0; i < WORKERS; i++ {
		close(aggregateChannels[i])
	}
	for i := 0; i < WORKERS; i++ {
		for name, c := range <-aggregateChannels[i] {
			fmt.Println(name, c)
		}
	}

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
