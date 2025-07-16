package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	mode     = flag.String("mode", "seq", "seq | rand | mix-shared | mix-split")
	filePath = flag.String("file", "segment.dat", "file to read")
	duration = flag.Duration("dur", 15*time.Second, "run time")
	seqBS    = flag.Int64("seqbs", 1<<20, "sequential block size (bytes)")
	randBS   = flag.Int64("randbs", 4<<10, "random block size (bytes)")
	randRate = flag.Int("randrate", 0, "limit random reads per second (0 = unlimited)")
	randSeed = flag.Int64("seed", time.Now().UnixNano(), "PRNG seed")
)

func main() {
	flag.Parse()

	info, err := os.Stat(*filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stat: %v\n", err)
		os.Exit(1)
	}
	fileSize := info.Size()

	switch *mode {
	case "seq":
		runSeq(fileSize)
	case "rand":
		runRand(fileSize)
	case "mix-shared":
		runMixed(fileSize, false)
	case "mix-split":
		runMixed(fileSize, true)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", *mode)
		os.Exit(1)
	}
}

// ---------------- one-shot helpers ----------------

func openRO(path string) *os.File {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	return f
}

func mib(b int64, d time.Duration) float64 {
	return float64(b) / (1024 * 1024) / d.Seconds()
}

// ---------------- pure sequential ----------------

func runSeq(fileSize int64) {
	f := openRO(*filePath)
	defer f.Close() // nolint:errcheck

	buf := make([]byte, *seqBS)
	deadline := time.Now().Add(*duration)
	var reads int64

	for time.Now().Before(deadline) {
		for off := int64(0); off < fileSize && time.Now().Before(deadline); off += *seqBS {
			if _, err := f.ReadAt(buf, off); err != nil {
				fmt.Fprintf(os.Stderr, "seq read: %v\n", err)
				os.Exit(1)
			}
			reads++
		}
	}

	total := reads * *seqBS
	fmt.Printf("Sequential: %.2f MiB/s (%d reads)\n", mib(total, *duration), reads)
}

// ---------------- pure random ----------------

func runRand(fileSize int64) {
	f := openRO(*filePath)
	defer f.Close() // nolint:errcheck

	buf := make([]byte, *randBS)
	r := rand.New(rand.NewSource(*randSeed))
	deadline := time.Now().Add(*duration)
	var reads int64

	// optional throttling
	var ticker *time.Ticker
	if *randRate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(*randRate))
		defer ticker.Stop()
	}

	for time.Now().Before(deadline) {
		if ticker != nil {
			<-ticker.C
		}
		off := r.Int63n(fileSize - *randBS)
		if _, err := f.ReadAt(buf, off); err != nil {
			fmt.Fprintf(os.Stderr, "rand read: %v\n", err)
			os.Exit(1)
		}
		reads++
	}

	total := reads * *randBS
	fmt.Printf("Random: %.2f MiB/s (%d reads)\n", mib(total, *duration), reads)
}

// ---------------- mixed ----------------

func runMixed(fileSize int64, splitFD bool) {
	seqF := openRO(*filePath)
	defer seqF.Close() // nolint:errcheck
	rndF := seqF
	if splitFD {
		rndF = openRO(*filePath) // second descriptor
		defer rndF.Close() // nolint:errcheck
	}

	var seqBytes, rndBytes int64
	deadline := time.Now().Add(*duration)
	r := rand.New(rand.NewSource(*randSeed))
	var wg sync.WaitGroup

	// sequential goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, *seqBS)
		for time.Now().Before(deadline) {
			for off := int64(0); off < fileSize && time.Now().Before(deadline); off += *seqBS {
				if _, err := seqF.ReadAt(buf, off); err != nil {
					fmt.Fprintf(os.Stderr, "seq read: %v\n", err)
					os.Exit(1)
				}
				atomic.AddInt64(&seqBytes, *seqBS)
			}
		}
	}()

	// optional throttle
	var ticker *time.Ticker
	if *randRate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(*randRate))
		defer ticker.Stop()
	}

	// random goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, *randBS)
		for time.Now().Before(deadline) {
			if ticker != nil {
				<-ticker.C
			}
			off := r.Int63n(fileSize - *randBS)
			if _, err := rndF.ReadAt(buf, off); err != nil {
				fmt.Fprintf(os.Stderr, "rand read: %v\n", err)
				os.Exit(1)
			}
			atomic.AddInt64(&rndBytes, *randBS)
		}
	}()

	wg.Wait()

	fmt.Printf("%s: Seq %.2f MiB/s  Rand %.2f MiB/s\n",
		map[bool]string{false: "Mixed-shared", true: "Mixed-split"}[splitFD],
		mib(seqBytes, *duration),
		mib(rndBytes, *duration),
	)
}

