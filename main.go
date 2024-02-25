package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Temperature struct {
	Min               int
	Max               int
	Avg               int
	NumOfTemperatures int
}

func main() {
	inputFile := flag.String("input_file", "", "Log file name.")
	flag.Parse()

	ts := time.Now()

	file, err := os.Open(*inputFile)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	temperatureMap := make(map[string]Temperature)

	var count = 0

	for scanner.Scan() {
		count += 1

		parts := strings.Split(scanner.Text(), ";")
		city := parts[0]
		temperature, _ := strconv.Atoi(strings.Replace(parts[1], ".", "", 1))

		if temp, ok := temperatureMap[city]; ok {
			t := temperatureMap[city]

			t.NumOfTemperatures += 1
			t.Avg = (t.Avg*t.NumOfTemperatures + temperature) / (t.NumOfTemperatures + 1)

			if temp.Min > temperature {
				t.Min = temperature
			}
			if temp.Max < temperature {
				t.Max = temperature
			}

			temperatureMap[city] = t

		} else {
			newTemp := Temperature{
				Min:               temperature,
				Max:               temperature,
				Avg:               temperature,
				NumOfTemperatures: 1,
			}
			temperatureMap[city] = newTemp
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error:", err)
	}

	// for city, temp := range temperatureMap {
	// 	fmt.Printf("key[%s] value[%+v]\n", city, temp)
	// }

	fmt.Println("Exec time:", time.Since(ts).String(), count)
}
