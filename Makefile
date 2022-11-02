.PHONY: scrape lint black flake8 mypy

scrape:
	poetry run python hn_analytics/whoishiring.py

lint: black flake8 mypy

black:
	poetry run black hn_analytics/

flake8:
	poetry run flake8 hn_analytics/

mypy:
	poetry run mypy hn_analytics/