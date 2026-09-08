package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/S1933/personal-radar/internal/app"
	"github.com/S1933/personal-radar/internal/config"
	"github.com/S1933/personal-radar/internal/logging"
)

func main() {
	configPath := flag.String("config", "config/radar.yaml", "path to configuration file")
	flag.Parse()

	cmd := flag.Arg(0)
	if cmd == "" {
		usage()
		os.Exit(2)
	}

	log := logging.New("radar", logging.InfoLevel)

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Error("load config", "error", err, "path", *configPath)
		os.Exit(1)
	}

	if lvl, err := logging.ParseLevel(cfg.LogLevel); err == nil {
		log = logging.New("radar", lvl)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.New(ctx, cfg, log)
	if err != nil {
		log.Error("init app", "error", err)
		os.Exit(1)
	}
	defer a.Close()

	switch cmd {
	case "migrate":
		if err := a.Migrate(ctx); err != nil {
			log.Error("migrate", "error", err)
			os.Exit(1)
		}
		fmt.Println("migrations applied")

	case "collect":
		n, err := a.CollectOnce(ctx)
		if err != nil {
			log.Error("collect", "error", err)
			os.Exit(1)
		}
		fmt.Printf("collected %d new items\n", n)

	case "rank":
		n, err := a.RankPending(ctx)
		if err != nil {
			log.Error("rank", "error", err)
			os.Exit(1)
		}
		fmt.Printf("ranked %d items\n", n)

	case "briefing":
		b, err := a.Briefing(ctx)
		if err != nil {
			log.Error("briefing", "error", err)
			os.Exit(1)
		}
		fmt.Println(b)

	case "run":
		if err := a.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error("run", "error", err)
			os.Exit(1)
		}

	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, strings.TrimSpace(`
usage: radar [-config path] <command>

commands:
  migrate    apply database migrations
  collect    run one collection cycle across all enabled collectors
  rank       score pending items
  briefing   generate the daily briefing (persisted in the briefings table)
  run        start the scheduler (collect + briefing slots)
`))
}
