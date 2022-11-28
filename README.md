# About
Collection of scripts and programs to analyze https://news.ycombinator.com

## hn_analytics/whoishiring.py

A script that scrapes whoishiring posts and transforms into analytical format.

Running the script will create a `whoishiring.db` sqlite3 database locally.

    $ poetry run python hn_analytics/whoishiring.py

Simple analytic result currently published by querying the data and
dumping it to this [google sheet](https://docs.google.com/spreadsheets/d/18kLWHkrEedpl1eXzAht_se8GEKd0G9zrlz7SutP2uLQ/edit?usp=sharing)

## Development

* Python 3.11+ (use pyenv)
* poetry (https://python-poetry.org/docs/#installing-with-the-official-installer)

### Notes

I was messing with async python stuff.