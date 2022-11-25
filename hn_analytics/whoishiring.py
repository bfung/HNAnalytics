#!/usr/bin/env python

from dataclasses import dataclass
import json
import re
from typing import List
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


class ScrapedUser():

    def __init__(self, id: str, json_str: str, scrape_time: datetime = datetime.utcnow()) -> None:
        self._id = id
        self._scrape_time = scrape_time

        j = json.loads(json_str)
        self._about = j["about"]
        self._created = datetime.utcfromtimestamp(j["created"])
        self._karma = j["karma"]
        self._submitted = j["submitted"]

    @property
    def about(self) -> str:
        return self._about

    @property
    def created(self) -> datetime:
        return self._created 

    @property
    def karma(self) -> int:
        return self._karma

    @property
    def submitted(self) -> List[int]:
        return self._submitted


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


def should_scrape_user(user_id: str, engine: Engine) -> bool:
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
            print("No need to scrape right now. Try again in two weeks.")
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
    if should_scrape_user(user_id, engine):
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
    print(scraped_user.submitted)

    #update_scrape_item_queue(scraped_user.json, engine)


if __name__ == "__main__":
    main()
