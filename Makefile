.PHONY: run test fmt tidy

run:
	go run ./cmd/hnanalytics

test:
	go test ./...

fmt:
	gofmt -w ./cmd ./internal

tidy:
	go mod tidy
