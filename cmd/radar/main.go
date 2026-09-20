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
	"github.com/S1933/personal-radar/internal/textutil"
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

	case "compare":
		n := 15
		if arg := flag.Arg(1); arg != "" {
			if _, err := fmt.Sscanf(arg, "%d", &n); err != nil || n <= 0 {
				fmt.Fprintf(os.Stderr, "compare: invalid count %q\n", arg)
				os.Exit(2)
			}
		}
		if err := runCompare(ctx, a, n); err != nil {
			log.Error("compare", "error", err)
			os.Exit(1)
		}

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
  compare [N]  score the newest N scored items with Jev and print both
             rankings side by side (read-only, writes nothing)
  briefing   generate the daily briefing (persisted in the briefings table)
  run        start the scheduler (collect + briefing slots)
`))
}

// compareThresholds are the notify.score_threshold candidates shown in the
// compare summary; the live threshold is read by notifications.py, not here.
var compareThresholds = []float64{0.5, 0.6, 0.7}

// runCompare prints the stored score next to a fresh Jev verdict for the newest
// scored items. Read-only: nothing is written, so it is safe to run against the
// live database before switching rank_engine.
func runCompare(ctx context.Context, a *app.App, n int) error {
	rows, err := a.RankCompare(ctx, n)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Println("no scored items in the last 7 days")
		return nil
	}

	fmt.Printf("%3s %-9s %7s %7s | %5s %5s %5s %5s | %7s %6s  %s\n",
		"#", "source", "stored", "jev", "rel", "imp", "nov", "act", "include", "ms", "title")

	var storedPass, jevPass [3]int
	var msTotal, tokens, scored int
	for i, r := range rows {
		if r.Err != nil {
			fmt.Printf("%3d %-9s  error: %v\n", i+1, r.Item.Source, r.Err)
			continue
		}
		scored++
		msTotal += int(r.Jev.Latency.Milliseconds())
		tokens += r.Jev.InputTokens
		for j, th := range compareThresholds {
			if r.Stored.Final >= th {
				storedPass[j]++
			}
			if r.Jev.Score.Final >= th {
				jevPass[j]++
			}
		}
		fmt.Printf("%3d %-9s %7.3f %7.3f | %5.2f %5.2f %5.2f %5.2f | %7.2f %6d  %s\n",
			i+1, r.Item.Source, r.Stored.Final, r.Jev.Score.Final,
			r.Jev.Score.Relevance, r.Jev.Score.Importance,
			r.Jev.Score.Novelty, r.Jev.Score.Actionability,
			r.Jev.Include, r.Jev.Latency.Milliseconds(),
			textutil.Truncate(r.Item.Title, 58, "…"))
	}
	if scored == 0 {
		return nil
	}
	fmt.Println()
	for j, th := range compareThresholds {
		fmt.Printf("final >= %.2f : stored %2d/%d · jev %2d/%d\n",
			th, storedPass[j], scored, jevPass[j], scored)
	}
	fmt.Printf("jev: mean %d ms/item · %d input tokens total · ~$%.6f at $42/Btok\n",
		msTotal/scored, tokens, float64(tokens)/1e6*0.042)
	return nil
}
