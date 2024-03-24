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

func aggregate(aggregateChannels []chan map[string]*City) {
	for i := 0; i < len(aggregateChannels); i++ {
		close(aggregateChannels[i])
	}

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

	// chunkSize := flag.Int("chunkSize", 10, "chunk size")
	flag.Parse()

	file, err := os.Open("input/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	BATCH_SIZE := 10000
	WORKERS := 8
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

	aggregate(aggregateChannels)

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
