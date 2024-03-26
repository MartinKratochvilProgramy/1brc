package main

import (
	"fmt"
	"sync"
	"time"
)

func isPrime(n int) bool {
	for i := 2; i < n; i++ {
		time.Sleep(time.Millisecond)
		if n%i == 0 {
			return false
		}
	}

	return true
	// if n <= 1 {
	// 	return false
	// }
	// if n <= 3 {
	// 	return true
	// }
	// if n%2 == 0 || n%3 == 0 {
	// 	return false
	// }
	// i := 5
	// for i*i <= n {
	// 	if n%i == 0 || n%(i+2) == 0 {
	// 		return false
	// 	}
	// 	i += 6
	// }
	// return true
}

func generator(
	done <-chan interface{},
	num int,
) <-chan int {
	intStream := make(chan int)
	go func() {
		defer close(intStream)
		for i := 2; i < num; i++ {
			select {
			case <-done:
				return
			case intStream <- i: // rand.Intn(100000):
			}
		}
	}()
	return intStream
}

func primeFinder(
	intStream <-chan int,
	done <-chan interface{},
	resultStream chan<- int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for i := range intStream {
		select {
		case <-done:
			return
		default:
			if isPrime(i) {
				resultStream <- i
			}
		}
	}
}

func main() {
	ts := time.Now()

	done := make(chan interface{})
	defer close(done)

	intStream := generator(done, 500)
	primeStream := make(chan int)

	nWorkers := 10

	var wg sync.WaitGroup
	wg.Add(nWorkers)

	for i := 0; i < nWorkers; i++ {
		go primeFinder(intStream, done, primeStream, &wg)
	}

	go func() {
		wg.Wait()
		close(primeStream)
	}()

	for p := range primeStream {
		fmt.Println(p)
	}

	fmt.Println("Exec time:", time.Since(ts))
}
