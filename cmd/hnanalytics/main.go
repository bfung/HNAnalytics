package main

import (
	"context"
	"fmt"
	"log"

	"hnanalytics/internal/whoishiring"
)

func main() {
	ctx := context.Background()

	svc, err := whoishiring.NewService(ctx, "whoishiring.db")
	if err != nil {
		log.Fatal(err)
	}
	defer svc.Close()

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
