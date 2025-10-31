package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	logPath := os.Getenv("STRATEGY_LOG_FILE")
	if logPath == "" {
		logPath = "/app/data/strategy.log"
	}

	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		log.Fatalf("failed to ensure log dir: %v", err)
	}

	fmt.Printf("Strategy Screen - following %s\n", logPath)

	// Simple tail: reopen on rotation, poll for new lines
	var offset int64
	for {
		f, err := os.Open(logPath)
		if err != nil {
			fmt.Printf("waiting for log file... (%v)\n", err)
			time.Sleep(1 * time.Second)
			continue
		}

		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// On first run, start at end to show live only
		if offset == 0 {
			offset = info.Size()
		}

		// Seek to current offset
		_, _ = f.Seek(offset, 0)
		r := bufio.NewReader(f)

		for {
			line, err := r.ReadString('\n')
			if len(line) > 0 {
				fmt.Print(line)
				offset += int64(len(line))
			}
			if err != nil {
				_ = f.Close()
				break
			}
		}

		time.Sleep(500 * time.Millisecond)
	}
}

