package bigquery

import (
	"context"
	"fmt"
	"math"
	"time"

	gcbigquery "cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	"hnanalytics/internal/whoishiring"
)

const (
	defaultDataset = "bigquery-public-data.hacker_news"
	defaultTable   = "full"
)

type Config struct {
	ProjectID      string
	Location       string
	MaxBytesBilled int64
	Dataset        string
	Table          string
}

type Watermark struct {
	Time time.Time
	ID   int
}

type Source struct {
	client *gcbigquery.Client
	cfg    Config
}

type row struct {
	ID    int64                 `bigquery:"id"`
	Time  time.Time             `bigquery:"time"`
	Title gcbigquery.NullString `bigquery:"title"`
	Dead  gcbigquery.NullBool   `bigquery:"dead"`
	Kids  []int64               `bigquery:"kids"`
}

func New(ctx context.Context, cfg Config) (*Source, error) {
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("project id is required")
	}
	if cfg.Dataset == "" {
		cfg.Dataset = defaultDataset
	}
	if cfg.Table == "" {
		cfg.Table = defaultTable
	}

	client, err := gcbigquery.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, err
	}

	return &Source{client: client, cfg: cfg}, nil
}

func (s *Source) Close() error { return s.client.Close() }

func (s *Source) BackfillByDateRange(ctx context.Context, startInclusive, endExclusive time.Time) ([]whoishiring.AnalyticItem, Watermark, error) {
	if !startInclusive.Before(endExclusive) {
		return nil, Watermark{}, fmt.Errorf("start must be before end")
	}

	query := fmt.Sprintf("\nSELECT\n  id,\n  time,\n  title,\n  dead,\n  kids\nFROM `%s.%s`\nWHERE time >= @start_time\n  AND time < @end_time\nORDER BY time ASC, id ASC", s.cfg.Dataset, s.cfg.Table)

	rows, err := s.readRows(ctx, query, []gcbigquery.QueryParameter{
		{Name: "start_time", Value: startInclusive},
		{Name: "end_time", Value: endExclusive},
	})
	if err != nil {
		return nil, Watermark{}, err
	}
	return toAnalyticItems(rows)
}

func (s *Source) SyncIncremental(ctx context.Context, watermark Watermark, upperBoundExclusive time.Time) ([]whoishiring.AnalyticItem, Watermark, error) {
	query := fmt.Sprintf("\nSELECT\n  id,\n  time,\n  title,\n  dead,\n  kids\nFROM `%s.%s`\nWHERE (time > @watermark_time OR (time = @watermark_time AND id > @watermark_id))\n  AND time < @upper_bound\nORDER BY time ASC, id ASC", s.cfg.Dataset, s.cfg.Table)

	rows, err := s.readRows(ctx, query, []gcbigquery.QueryParameter{
		{Name: "watermark_time", Value: watermark.Time},
		{Name: "watermark_id", Value: watermark.ID},
		{Name: "upper_bound", Value: upperBoundExclusive},
	})
	if err != nil {
		return nil, watermark, err
	}
	return toAnalyticItems(rows)
}

func (s *Source) readRows(ctx context.Context, queryText string, params []gcbigquery.QueryParameter) ([]row, error) {
	query := s.client.Query(queryText)
	query.Parameters = params
	if s.cfg.Location != "" {
		query.Location = s.cfg.Location
	}
	if s.cfg.MaxBytesBilled > 0 {
		query.MaxBytesBilled = s.cfg.MaxBytesBilled
	}

	it, err := query.Read(ctx)
	if err != nil {
		return nil, err
	}

	rows := make([]row, 0)
	for {
		var r row
		err := it.Next(&r)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, r)
	}
	return rows, nil
}

func toAnalyticItems(rows []row) ([]whoishiring.AnalyticItem, Watermark, error) {
	analytics := make([]whoishiring.AnalyticItem, 0, len(rows))
	wm := Watermark{}
	for _, r := range rows {
		if r.Dead.Valid && r.Dead.Bool {
			continue
		}
		if r.ID > math.MaxInt {
			return nil, Watermark{}, fmt.Errorf("row id %d overflows int", r.ID)
		}

		var title *string
		if r.Title.Valid {
			t := r.Title.StringVal
			title = &t
		}

		analytic := whoishiring.AnalyticItemFromFields(int(r.ID), r.Time.UTC(), title, len(r.Kids))
		analytics = append(analytics, analytic)
		wm = Watermark{Time: r.Time.UTC(), ID: int(r.ID)}
	}
	return analytics, wm, nil
}
