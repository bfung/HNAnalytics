package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"hnanalytics/internal/ingest/bigquery"
	"hnanalytics/internal/whoishiring"
)

const dateLayout = "2006-01-02"

func main() {
	ctx := context.Background()

	source := flag.String("source", "api", "ingestion source: api or bigquery")
	dbName := flag.String("db", "whoishiring.db", "datastore base name")
	startDate := flag.String("start-date", "", "start date (YYYY-MM-DD), inclusive")
	endDate := flag.String("end-date", "", "end date (YYYY-MM-DD), inclusive")
	incremental := flag.Bool("incremental", false, "run incremental sync mode")
	projectID := flag.String("gcp-project", "", "GCP project for BigQuery jobs")
	location := flag.String("gcp-location", "", "BigQuery job location, e.g. US")
	maxBytesBilled := flag.Int64("max-bytes-billed", 0, "optional BigQuery max bytes billed")
	flag.Parse()

	svc, err := whoishiring.NewService(ctx, *dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := svc.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	switch *source {
	case "api":
		runAPISource(ctx, svc)
	case "bigquery":
		cfg := bigquery.Config{ProjectID: *projectID, Location: *location, MaxBytesBilled: *maxBytesBilled}
		if err := runBigQuerySource(ctx, svc, cfg, *startDate, *endDate, *incremental); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unsupported source %q", *source)
	}
}

func runAPISource(ctx context.Context, svc *whoishiring.Service) {
	users := []string{"whoishiring", "_whoishiring"}
	for _, user := range users {
		scrapedUser, err := svc.ScrapeUser(ctx, user)
		if err != nil {
			log.Fatal(err)
		}

		items, err := svc.QueueItems(ctx, scrapedUser.Submitted)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("There are %d items to scrape.\n", len(items))

		if err := svc.ScrapeItems(ctx, items, 16); err != nil {
			log.Fatal(err)
		}
	}

	items, err := svc.QueueItems(ctx, []int{2719028, 3300290})
	if err != nil {
		log.Fatal(err)
	}
	if err := svc.ScrapeItems(ctx, items, 16); err != nil {
		log.Fatal(err)
	}

	analyticItems, err := svc.ScrapeToAnalyticItems(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Processed %d analytic items\n", len(analyticItems))
}

func runBigQuerySource(ctx context.Context, svc *whoishiring.Service, cfg bigquery.Config, startDate, endDate string, incremental bool) error {
	source, err := bigquery.New(ctx, cfg)
	if err != nil {
		return err
	}
	defer source.Close()

	if incremental {
		return runBigQueryIncremental(ctx, svc, source, endDate)
	}
	if startDate == "" || endDate == "" {
		return fmt.Errorf("start-date and end-date are required for backfill")
	}
	start, end, err := parseDateRange(startDate, endDate)
	if err != nil {
		return err
	}

	items, watermark, err := source.BackfillByDateRange(ctx, start, end)
	if err != nil {
		return err
	}
	inserted := svc.UpsertAnalyticItems(ctx, items)
	if len(items) > 0 {
		svc.SetCheckpoint(ctx, "bigquery", whoishiring.Checkpoint{Time: watermark.Time, ID: watermark.ID})
	}
	fmt.Printf("Backfilled %d items (%d inserted new rows).\n", len(items), inserted)
	return nil
}

func runBigQueryIncremental(ctx context.Context, svc *whoishiring.Service, source *bigquery.Source, endDate string) error {
	watermark := bigquery.Watermark{}
	if cp, ok := svc.GetCheckpoint(ctx, "bigquery"); ok {
		watermark = bigquery.Watermark{Time: cp.Time, ID: cp.ID}
	}

	upperBound := time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	if endDate != "" {
		end, err := parseDate(endDate)
		if err != nil {
			return err
		}
		upperBound = end.Add(24 * time.Hour)
	}

	items, latest, err := source.SyncIncremental(ctx, watermark, upperBound)
	if err != nil {
		return err
	}
	inserted := svc.UpsertAnalyticItems(ctx, items)
	if len(items) > 0 {
		svc.SetCheckpoint(ctx, "bigquery", whoishiring.Checkpoint{Time: latest.Time, ID: latest.ID})
	}
	fmt.Printf("Incremental sync returned %d items (%d inserted new rows).\n", len(items), inserted)
	return nil
}

func parseDateRange(startDate, endDate string) (time.Time, time.Time, error) {
	start, err := parseDate(startDate)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	end, err := parseDate(endDate)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	endExclusive := end.Add(24 * time.Hour)
	if !start.Before(endExclusive) {
		return time.Time{}, time.Time{}, fmt.Errorf("start-date must be before or equal to end-date")
	}
	return start, endExclusive, nil
}

func parseDate(value string) (time.Time, error) {
	parsed, err := time.Parse(dateLayout, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid date %q: %w", value, err)
	}
	return parsed.UTC(), nil
}
