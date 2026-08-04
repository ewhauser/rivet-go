package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ewhauser/rivet-go/internal/devengine"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "rivet-go-dev:", err)
		os.Exit(1)
	}
}

func run() error {
	var dataDir string
	var port int
	flag.StringVar(&dataDir, "data-dir", ".rivet-go", "local engine data directory")
	flag.IntVar(&port, "port", 6420, "engine guard/gateway port")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	binary, err := devengine.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire pinned engine %s: %w", devengine.Tag, err)
	}
	absoluteDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return fmt.Errorf("resolve data directory: %w", err)
	}
	engine, err := devengine.New(binary, absoluteDataDir, port)
	if err != nil {
		return err
	}
	if err := engine.Start(ctx); err != nil {
		return err
	}
	fmt.Printf("Rivet Engine %s is ready at %s\n", devengine.Version, engine.Endpoint)
	fmt.Printf("data: %s\nlog:  %s\n", engine.StorageDir, engine.LogPath)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return engine.Stop(shutdownCtx)
}
