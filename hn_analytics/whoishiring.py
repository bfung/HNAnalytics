#!/usr/bin/env python

import requests
import sqlite3

from datetime import datetime, timedelta, timezone

SCRAPE_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS scrape_users (
        id TEXT,
        json TEXT,
        scrape_time TEXT DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT pk_id_scrape_time PRIMARY KEY (id, scrape_time)
          ON CONFLICT ABORT
       ) STRICT;
    """,
    """CREATE TABLE IF NOT EXISTS scrape_items (
        id TEXT,
        json TEXT,
        scrape_time TEXT DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT pk_id_scrape_time PRIMARY KEY (id, scrape_time)
          ON CONFLICT ABORT
       ) STRICT;
    """,
]


def run_migration(dbname: str):
    with sqlite3.connect(dbname) as con:
        for ddl in SCRAPE_SCHEMA:
            con.execute(ddl)


def scrape_user(user_id: str, dbname: str):
    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)

    last_scape_time = None
    with con:
        max = con.execute(
            "SELECT max(scrape_time) FROM scrape_users where id = ?", (user_id,)
        )
        row = max.fetchone()
        if row[0]:
            last_scape_time = datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S.%f%z")
            print(f"Last scraped: {last_scape_time}")
        else:
            print("No existing records")

    if last_scape_time:
        elapsed = datetime.now(timezone.utc) - last_scape_time
        if elapsed < timedelta(days=14):
            print("Update too early, run this again in a week.")
            return

    user_url = f"https://hacker-news.firebaseio.com/v0/user/{user_id}.json"
    r = requests.get(user_url)

    try:
        with con:
            con.execute(
                "INSERT INTO scrape_users (id, json, scrape_time) VALUES (?, ?, ?)",
                (user_id, r.text, datetime.now(timezone.utc).isoformat()),
            )
    except sqlite3.Error as e:
        print(e)
    finally:
        con.close()


def main():
    run_migration("whoishiring.db")
    scrape_user("whoishiring", "whoishiring.db")


if __name__ == "__main__":
    main()
