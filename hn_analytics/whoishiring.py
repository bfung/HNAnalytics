#!/usr/bin/env python

from dataclasses import dataclass
import re
import requests

from datetime import datetime, timedelta

from sqlalchemy import (
    create_engine,
    func,
    select,
    insert,
    Table,
    Column,
    String,
    DateTime,
    MetaData,
)
from sqlalchemy.future import Engine


METADATA_OBJ = MetaData()

TBL_SCRAPE_USERS = Table(
    "scrape_users",
    METADATA_OBJ,
    Column("id", String, primary_key=True, autoincrement=False),
    Column("json", String),
    Column(
        "scrape_time",
        DateTime,
        primary_key=True,
        autoincrement=False,
        default=datetime.utcnow(),
    ),
)


@dataclass
class ScrapedUser:
    id: str
    json: str
    scrape_time: datetime = datetime.utcnow()


def validateDbName(dbname: str) -> str:
    """
    Throws ValueError if dbname is not alphanumeric.
    """
    match = re.match(r"\w+", dbname)
    if not match:
        raise ValueError(f"dbname '{dbname}' is not alphanumeric!")
    return dbname


def init_db(dbname: str, echo=False) -> Engine:
    validated = validateDbName(dbname)
    engine = create_engine(f"sqlite+pysqlite:///{validated}", echo=echo, future=True)
    METADATA_OBJ.create_all(engine)
    return engine


def should_scrape(user_id: str, engine: Engine) -> bool:
    last_scape_time = None
    with engine.connect() as conn:
        alias = "last_scraped_time"
        result = conn.execute(
            select(func.max(TBL_SCRAPE_USERS.c.scrape_time).label(alias)).where(
                TBL_SCRAPE_USERS.c.id == user_id
            )
        )
        row = result.first()
        last_scape_time = row[alias]

    if last_scape_time:
        elapsed = datetime.utcnow() - last_scape_time
        if elapsed < timedelta(days=14):
            print("Update too early, run this again in a week.")
            return False

    return True


def scrape_user(user_id: str, engine: Engine) -> ScrapedUser:
    scraped_user = None
    stmt = (
        select(TBL_SCRAPE_USERS.columns)
        .where(TBL_SCRAPE_USERS.c.id == user_id)
        .order_by(TBL_SCRAPE_USERS.c.scrape_time.desc())
        .limit(1)
    )
    if should_scrape(user_id, engine):
        user_url = f"https://hacker-news.firebaseio.com/v0/user/{user_id}.json"
        r = requests.get(user_url)

        with engine.connect() as conn:
            conn.execute(insert(TBL_SCRAPE_USERS).values(id=user_id, json=r.text))
            conn.commit()
            q = conn.execute(stmt).one()
            scraped_user = ScrapedUser(q[0], q[1], q[2])
    else:
        with engine.connect() as conn:
            q = conn.execute(stmt).one()
            scraped_user = ScrapedUser(q[0], q[1], q[2])

    return scraped_user


def main():
    engine = init_db("whoishiring.db")
    scraped_user = scrape_user("whoishiring", engine)
    print(scraped_user)

    #update_scrape_item_queue(scraped_user.json, engine)


if __name__ == "__main__":
    main()
