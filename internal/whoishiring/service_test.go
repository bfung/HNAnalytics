package whoishiring

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestValidateDBName(t *testing.T) {
	tests := []struct {
		name    string
		dbName  string
		wantErr bool
	}{
		{name: "simple", dbName: "whoishiring.db", wantErr: false},
		{name: "invalid", dbName: "-bad", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateDBName(tt.dbName)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateDBName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestToAnalyticItem(t *testing.T) {
	title := "Ask HN: Who is hiring? (June 2024)"
	item := Item{ID: 1, Time: time.Unix(10, 0), Kids: []int{1, 2, 3}, Title: &title}

	analytic := toAnalyticItem(item)
	if analytic.WHType != "hiring" {
		t.Fatalf("expected hiring, got %s", analytic.WHType)
	}
	if analytic.NumKids != 3 {
		t.Fatalf("expected 3 kids, got %d", analytic.NumKids)
	}
}

func TestUpsertAnalyticItemsAndCheckpoint(t *testing.T) {
	dir := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWD) })

	svc, err := NewService(context.Background(), "testdb")
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	inserted := svc.UpsertAnalyticItems(context.Background(), []AnalyticItem{{ID: 1}, {ID: 1}})
	if inserted != 1 {
		t.Fatalf("expected 1 new insert, got %d", inserted)
	}

	svc.SetCheckpoint(context.Background(), "bigquery", Checkpoint{ID: 10, Time: time.Unix(100, 0).UTC()})
	cp, ok := svc.GetCheckpoint(context.Background(), "bigquery")
	if !ok {
		t.Fatal("expected checkpoint to exist")
	}
	if cp.ID != 10 {
		t.Fatalf("expected checkpoint id 10, got %d", cp.ID)
	}
}
