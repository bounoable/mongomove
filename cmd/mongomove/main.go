package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/bounoable/mongomove"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	source := flag.String("source", "mongodb://127.0.0.1:27017", "Source URI")
	target := flag.String("target", "mongodb://127.0.0.1:27018", "Target URI")
	prefix := flag.String("prefix", "", "Database prefix (filter)")
	drop := flag.Bool("drop", false, "Drop target databases before import")
	skipConfirm := flag.Bool("confirm", false, "Don't ask for confirmation")
	parallel := flag.Int("parallel", runtime.NumCPU(), "Control parallelism")
	batchSize := flag.Int("batch", 100, "Batch inserts")
	verbose := flag.Bool("verbose", false, "Log debug info")

	// short flags
	flag.StringVar(source, "s", "mongodb://127.0.0.1:27017", "Source URI")
	flag.StringVar(target, "t", "mongodb://127.0.0.1:27018", "Target URI")
	flag.BoolVar(drop, "d", false, "Drop target databases before import")
	flag.BoolVar(skipConfirm, "c", false, "Don't ask for confirmation")
	flag.IntVar(parallel, "p", runtime.NumCPU(), "Control parallelism")
	flag.IntVar(batchSize, "b", 100, "Batch inserts")
	flag.BoolVar(verbose, "v", false, "Log debug info")

	flag.Parse()

	if *source == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *target == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, os.Interrupt)
	go func() {
		defer cancel()
		<-quit
	}()

	sourcec, err := mongo.Connect(ctx, options.Client().ApplyURI(*source))
	if err != nil {
		fmt.Printf("Failed to connect to source: %v\n", err)
		os.Exit(1)
	}
	defer sourcec.Disconnect(context.Background())

	targetc, err := mongo.Connect(ctx, options.Client().ApplyURI(*target))
	if err != nil {
		fmt.Printf("Failed to connect to target: %v\n", err)
		os.Exit(1)
	}
	defer targetc.Disconnect(context.Background())

	i := mongomove.New(sourcec, targetc)

	var opts []mongomove.ImportOption
	if *prefix != "" {
		opts = append(opts, mongomove.FilterDatabases(mongomove.HasPrefix(*prefix)))
	}
	if *drop {
		opts = append(opts, mongomove.Drop(true))
	}
	if *skipConfirm {
		opts = append(opts, mongomove.SkipConfirm(true))
	}
	if *verbose {
		opts = append(opts, mongomove.Verbose(true))
	}
	opts = append(opts, mongomove.Parallel(*parallel), mongomove.BatchSize(*batchSize))

	start := time.Now()
	if err := i.Import(ctx, opts...); err != nil {
		fmt.Printf("Failed to do import: %v\n", err)
		os.Exit(1)
	}
	end := time.Now()
	dur := end.Sub(start)

	fmt.Printf("Import done after %s.\n", dur)
}
