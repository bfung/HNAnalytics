#!/usr/bin/env python

import asyncio
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
    update,
    Table,
    Column,
    Integer,
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


class ScrapedUser:
    def __init__(
        self, id: str, json_str: str, scrape_time: datetime = datetime.utcnow()
    ) -> None:
        self._id = id
        self._scrape_time = scrape_time

        j = json.loads(json_str)
        self._about = j.get("about")
        self._created = datetime.utcfromtimestamp(j["created"])
        self._karma = j["karma"]
        self._submitted = j["submitted"]

    @property
    def id(self) -> str:
        return self._id

    @property
    def scrape_time(self) -> datetime:
        return self._scrape_time

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


TBL_SCRAPE_ITEMS = Table(
    "scrape_items",
    METADATA_OBJ,
    Column("id", String, primary_key=True, autoincrement=False),
    Column("json", String),
    Column(
        "scrape_time",
        DateTime,
        autoincrement=False,
        default=datetime.utcnow(),
    ),
)


class ScrapedItem:
    def __init__(
        self, id: str, json_str: str, scrape_time: datetime = datetime.utcnow()
    ) -> None:
        self._id = id
        self._scrape_time = scrape_time

        j = json.loads(json_str)
        self._deleted = j.get("deleted")  # in the hn API spec, but never in the data!
        self._type = j["type"]
        self._by = j.get("by")
        self._time = datetime.utcfromtimestamp(j.get("time")) if j.get("time") else None
        self._text = j.get("text")
        self._dead = j.get("dead") if j.get("dead") else False
        self._kids = j.get("kids")  # not available in all item types
        self._title = j.get("title")
        self._decendants = j.get("decendants")

    @property
    def id(self) -> str:
        return self._id

    @property
    def type(self) -> str:
        return self._type

    @property
    def by(self) -> str:
        return self._by

    @property
    def time(self) -> datetime:
        return self._time

    @property
    def dead(self) -> bool:
        return self._dead

    @property
    def kids(self) -> List[int]:
        return self._kids

    @property
    def title(self) -> str:
        return self._title

    @property
    def decendants(self) -> int | None:
        return self._decendants


TBL_ANALYTIC_ITEMS = Table(
    "analytic_items",
    METADATA_OBJ,
    Column("id", String, primary_key=True, autoincrement=False),
    Column("create_time", DateTime),
    Column("wh_type", String),  # who's hiring type: hiring, seeking, freelance, other
    Column("num_kids", Integer),
)

FREELANCER_TITLE = "Ask HN: Freelancer? Seeking freelancer?".lower()
SEEKING = "Ask HN: Who wants to be hired?".lower()
HIRING = "Ask HN: Who is hiring?".lower()


class AnalyticItem:
    def __init__(self, scraped_item: ScrapedItem) -> None:
        self._id = scraped_item.id
        self._create_time = scraped_item.time
        self._num_kids = len(scraped_item.kids) if scraped_item.kids else 0

        if scraped_item.title:
            title = scraped_item.title.lower()
            if title.startswith(FREELANCER_TITLE):
                self._wh_type = "freelancer"
            elif title.startswith(SEEKING):
                self._wh_type = "seeking"
            elif title.startswith(HIRING):
                self._wh_type = "hiring"
            else:
                self._wh_type = "other"
        else:
            self._wh_type = "other"

    @property
    def id(self) -> str:
        return self._id

    @property
    def create_time(self) -> datetime:
        return self._create_time

    @property
    def wh_type(self) -> str:
        return self._wh_type

    @property
    def num_kids(self) -> int:
        return self._num_kids


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


def scrape_user(user_id: str, engine: Engine) -> ScrapedUser | None:
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


def queue_items(user_items: List[int], engine: Engine) -> List[int]:
    to_scrape: List[int] = []
    with engine.connect() as conn:
        conn.execute(
            insert(TBL_SCRAPE_ITEMS)
            .values([{"id": x, "scrape_time": None} for x in user_items])
            .prefix_with("OR IGNORE")
        )
        conn.commit()
        result = conn.execute(
            select(TBL_SCRAPE_ITEMS.columns).where(TBL_SCRAPE_ITEMS.c.json.is_(None))
        )
        for row in result:
            to_scrape.append(row["id"])

    return to_scrape


async def scrape_item(item_id: int, engine: Engine) -> ScrapedItem | None:
    item_url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
    r = requests.get(item_url)

    item: ScrapedItem
    with engine.connect() as conn:
        conn.execute(
            update(TBL_SCRAPE_ITEMS)
            .where(TBL_SCRAPE_ITEMS.c.id == item_id)
            .values(json=r.text, scrape_time=datetime.utcnow())
        )
        conn.commit()
        q = conn.execute(
            select(TBL_SCRAPE_ITEMS.columns).where(TBL_SCRAPE_ITEMS.c.id == item_id)
        ).one()
        item = ScrapedItem(q[0], q[1], q[2])

    return item


async def scrape_items(item_ids: List[int], engine: Engine):
    async with asyncio.TaskGroup() as tg:
        for item_id in item_ids:
            tg.create_task(scrape_item(item_id, engine))
    return


def scrape_2_analytic_items(engine: Engine) -> List[AnalyticItem]:
    items: List[ScrapedItem] = []
    subq = (
        select(TBL_ANALYTIC_ITEMS.c.id).where(
            TBL_SCRAPE_ITEMS.c.id == TBL_ANALYTIC_ITEMS.c.id
        )
    ).exists()
    with engine.connect() as conn:
        result = conn.execute(select(TBL_SCRAPE_ITEMS.columns).where(~subq))
        for row in result:
            items.append(ScrapedItem(row[0], row[1], row[2]))

    analytic_items = [AnalyticItem(x) for x in items if not x.dead]
    with engine.connect() as conn:
        conn.execute(
            insert(TBL_ANALYTIC_ITEMS)
            .values(
                [
                    {
                        "id": a.id,
                        "create_time": a.create_time,
                        "wh_type": a.wh_type,
                        "num_kids": a.num_kids,
                    }
                    for a in analytic_items
                ]
            )
            .prefix_with("OR IGNORE")
        )
        conn.commit()
    return analytic_items


async def main():
    engine = init_db("whoishiring.db", echo=True)
    users = ["whoishiring", "_whoishiring"]
    for user in users:
        scraped_user = scrape_user(user, engine)
        items = queue_items(scraped_user.submitted, engine)
        print(f"There are {len(items)} items to scrape.")

        await scrape_items(items, engine)

    analytic_items = scrape_2_analytic_items(engine)
    print(f"Processed {len(analytic_items)} analytic items")


if __name__ == "__main__":
    asyncio.run(main())
