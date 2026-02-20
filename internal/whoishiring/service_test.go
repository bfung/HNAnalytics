package whoishiring

import (
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
