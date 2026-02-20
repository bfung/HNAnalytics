# About

A Go-based toolset for analyzing https://news.ycombinator.com data.

## whoishiring scraper

The scraper pulls data from the official Hacker News Firebase API and stores it in a local datastore file (`whoishiring.db.json`). It maintains three logical collections:

- `scrape_users`
- `scrape_items`
- `analytic_items`

Run it with:

```bash
go run ./cmd/hnanalytics
```

## Development

- Go 1.22+
- `make` (optional convenience commands)

### Useful commands

```bash
make run
make test
make fmt
```
