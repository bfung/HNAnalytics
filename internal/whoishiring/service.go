package whoishiring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	freelancerTitle = "ask hn: freelancer? seeking freelancer?"
	seekingTitle    = "ask hn: who wants to be hired?"
	hiringTitle     = "ask hn: who is hiring?"
)

var dbNamePattern = regexp.MustCompile(`^\w+`)

type Service struct {
	path       string
	httpClient *http.Client
	mu         sync.Mutex
	store      dataStore
}

type dataStore struct {
	ScrapeUsers   map[string][]userRecord `json:"scrape_users"`
	ScrapeItems   map[int]itemRecord      `json:"scrape_items"`
	AnalyticItems map[int]AnalyticItem    `json:"analytic_items"`
	Checkpoints   map[string]Checkpoint   `json:"checkpoints"`
}

type Checkpoint struct {
	Time      time.Time `json:"time"`
	ID        int       `json:"id"`
	UpdatedAt time.Time `json:"updated_at"`
}

type userRecord struct {
	JSON       string    `json:"json"`
	ScrapeTime time.Time `json:"scrape_time"`
}

type itemRecord struct {
	JSON       string    `json:"json"`
	ScrapeTime time.Time `json:"scrape_time"`
}

type User struct {
	ID         string
	ScrapeTime time.Time
	About      *string
	Created    time.Time
	Karma      int
	Submitted  []int
}

type Item struct {
	ID          int
	ScrapeTime  time.Time
	Deleted     bool
	Type        string
	By          *string
	Time        time.Time
	Text        *string
	Dead        bool
	Kids        []int
	Title       *string
	Descendants *int
}

type AnalyticItem struct {
	ID         int       `json:"id"`
	CreateTime time.Time `json:"create_time"`
	WHType     string    `json:"wh_type"`
	NumKids    int       `json:"num_kids"`
}

type hnUserResponse struct {
	About     *string `json:"about"`
	Created   int64   `json:"created"`
	Karma     int     `json:"karma"`
	Submitted []int   `json:"submitted"`
}

type hnItemResponse struct {
	Deleted     bool    `json:"deleted"`
	Type        string  `json:"type"`
	By          *string `json:"by"`
	Time        int64   `json:"time"`
	Text        *string `json:"text"`
	Dead        bool    `json:"dead"`
	Kids        []int   `json:"kids"`
	Title       *string `json:"title"`
	Descendants *int    `json:"descendants"`
}

func ValidateDBName(dbName string) (string, error) {
	if dbNamePattern.MatchString(dbName) {
		return dbName, nil
	}
	return "", fmt.Errorf("dbname %q is not alphanumeric", dbName)
}

func NewService(_ context.Context, dbName string) (*Service, error) {
	validated, err := ValidateDBName(dbName)
	if err != nil {
		return nil, err
	}

	svc := &Service{
		path:       filepath.Clean(validated + ".json"),
		httpClient: &http.Client{Timeout: 20 * time.Second},
		store: dataStore{
			ScrapeUsers:   map[string][]userRecord{},
			ScrapeItems:   map[int]itemRecord{},
			AnalyticItems: map[int]AnalyticItem{},
			Checkpoints:   map[string]Checkpoint{},
		},
	}

	if err := svc.load(); err != nil {
		return nil, err
	}

	return svc, nil
}

func (s *Service) Close() error { return s.persist() }

func (s *Service) load() error {
	content, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(content, &s.store); err != nil {
		return err
	}

	if s.store.ScrapeUsers == nil {
		s.store.ScrapeUsers = map[string][]userRecord{}
	}
	if s.store.ScrapeItems == nil {
		s.store.ScrapeItems = map[int]itemRecord{}
	}
	if s.store.AnalyticItems == nil {
		s.store.AnalyticItems = map[int]AnalyticItem{}
	}
	if s.store.Checkpoints == nil {
		s.store.Checkpoints = map[string]Checkpoint{}
	}

	return nil
}

func (s *Service) persist() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf, err := json.MarshalIndent(s.store, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, buf, 0o644)
}

func (s *Service) shouldScrapeUser(userID string) bool {
	records := s.store.ScrapeUsers[userID]
	if len(records) == 0 {
		return true
	}
	last := records[len(records)-1]
	if time.Since(last.ScrapeTime.UTC()) < 14*24*time.Hour {
		fmt.Println("No need to scrape right now. Try again in two weeks.")
		return false
	}
	return true
}

func (s *Service) ScrapeUser(ctx context.Context, userID string) (*User, error) {
	s.mu.Lock()
	should := s.shouldScrapeUser(userID)
	s.mu.Unlock()

	if should {
		url := fmt.Sprintf("https://hacker-news.firebaseio.com/v0/user/%s.json", userID)
		payload, err := s.fetch(ctx, url)
		if err != nil {
			return nil, err
		}

		s.mu.Lock()
		s.store.ScrapeUsers[userID] = append(s.store.ScrapeUsers[userID], userRecord{JSON: string(payload), ScrapeTime: time.Now().UTC()})
		s.mu.Unlock()
	}

	s.mu.Lock()
	records := s.store.ScrapeUsers[userID]
	s.mu.Unlock()
	if len(records) == 0 {
		return nil, fmt.Errorf("no records for user %s", userID)
	}
	latest := records[len(records)-1]
	return parseUser(userID, latest.JSON, latest.ScrapeTime)
}

func parseUser(id string, rawJSON string, scrapeTime time.Time) (*User, error) {
	var resp hnUserResponse
	if err := json.Unmarshal([]byte(rawJSON), &resp); err != nil {
		return nil, err
	}

	return &User{ID: id, ScrapeTime: scrapeTime, About: resp.About, Created: time.Unix(resp.Created, 0).UTC(), Karma: resp.Karma, Submitted: resp.Submitted}, nil
}

func (s *Service) QueueItems(_ context.Context, userItems []int) ([]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range userItems {
		if _, exists := s.store.ScrapeItems[id]; !exists {
			s.store.ScrapeItems[id] = itemRecord{}
		}
	}
	var ids []int
	for id, rec := range s.store.ScrapeItems {
		if rec.JSON == "" {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	return ids, nil
}

func (s *Service) ScrapeItems(ctx context.Context, itemIDs []int, concurrency int) error {
	if concurrency < 1 {
		concurrency = 1
	}
	jobs := make(chan int)
	errCh := make(chan error, concurrency)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for id := range jobs {
			if err := s.scrapeItem(ctx, id); err != nil {
				errCh <- err
				return
			}
		}
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker()
	}
	for _, id := range itemIDs {
		jobs <- id
	}
	close(jobs)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) scrapeItem(ctx context.Context, itemID int) error {
	payload, err := s.fetch(ctx, fmt.Sprintf("https://hacker-news.firebaseio.com/v0/item/%d.json", itemID))
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.store.ScrapeItems[itemID] = itemRecord{JSON: string(payload), ScrapeTime: time.Now().UTC()}
	s.mu.Unlock()
	return nil
}

func parseItem(id int, rawJSON string, scrapeTime time.Time) (*Item, error) {
	var resp hnItemResponse
	if err := json.Unmarshal([]byte(rawJSON), &resp); err != nil {
		return nil, err
	}

	return &Item{ID: id, ScrapeTime: scrapeTime, Deleted: resp.Deleted, Type: resp.Type, By: resp.By, Time: time.Unix(resp.Time, 0).UTC(), Text: resp.Text, Dead: resp.Dead, Kids: resp.Kids, Title: resp.Title, Descendants: resp.Descendants}, nil
}

func (s *Service) ScrapeToAnalyticItems(_ context.Context) ([]AnalyticItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	analytics := make([]AnalyticItem, 0)
	for id, rec := range s.store.ScrapeItems {
		if _, exists := s.store.AnalyticItems[id]; exists || rec.JSON == "" {
			continue
		}
		item, err := parseItem(id, rec.JSON, rec.ScrapeTime)
		if err != nil {
			return nil, err
		}
		if item.Dead {
			continue
		}
		analytic := toAnalyticItem(*item)
		s.store.AnalyticItems[id] = analytic
		analytics = append(analytics, analytic)
	}
	return analytics, nil
}

func toAnalyticItem(item Item) AnalyticItem {
	return AnalyticItemFromFields(item.ID, item.Time, item.Title, len(item.Kids))
}

func AnalyticItemFromFields(id int, createTime time.Time, title *string, kidsCount int) AnalyticItem {
	whType := "other"

	if title != nil {
		title := strings.ToLower(*title)
		switch {
		case strings.HasPrefix(title, freelancerTitle):
			whType = "freelancer"
		case strings.HasPrefix(title, seekingTitle):
			whType = "seeking"
		case strings.HasPrefix(title, hiringTitle):
			whType = "hiring"
		}
	}

	return AnalyticItem{ID: id, CreateTime: createTime, WHType: whType, NumKids: kidsCount}
}

func (s *Service) UpsertAnalyticItems(_ context.Context, items []AnalyticItem) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for _, item := range items {
		if _, exists := s.store.AnalyticItems[item.ID]; !exists {
			count++
		}
		s.store.AnalyticItems[item.ID] = item
	}

	return count
}

func (s *Service) GetCheckpoint(_ context.Context, key string) (Checkpoint, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp, ok := s.store.Checkpoints[key]
	return cp, ok
}

func (s *Service) SetCheckpoint(_ context.Context, key string, checkpoint Checkpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()

	checkpoint.UpdatedAt = time.Now().UTC()
	s.store.Checkpoints[key] = checkpoint
}

func (s *Service) fetch(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed with status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if string(body) == "null" {
		return nil, errors.New("api returned null payload")
	}
	return body, nil
}
