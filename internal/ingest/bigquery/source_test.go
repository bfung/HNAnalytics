package bigquery

import (
	"testing"
	"time"

	gcbigquery "cloud.google.com/go/bigquery"
)

func TestToAnalyticItems(t *testing.T) {
	title := "Ask HN: Who is hiring? (June 2024)"
	rows := []row{
		{ID: 10, Time: time.Unix(100, 0), Title: gcbigquery.NullString{StringVal: title, Valid: true}, Kids: []int64{1, 2}},
		{ID: 11, Time: time.Unix(101, 0), Dead: gcbigquery.NullBool{Bool: true, Valid: true}},
	}

	items, watermark, err := toAnalyticItems(rows)
	if err != nil {
		t.Fatalf("toAnalyticItems() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].WHType != "hiring" {
		t.Fatalf("expected hiring item type, got %q", items[0].WHType)
	}
	if watermark.ID != 10 {
		t.Fatalf("expected watermark id 10, got %d", watermark.ID)
	}
}
