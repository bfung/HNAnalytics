#!/usr/bin/env python

import requests
import sqlite3

migrations = [
	"CREATE TABLE IF NOT EXISTS submitted;",
]

r = requests.get("https://hacker-news.firebaseio.com/v0/user/whoishiring.json")

items = r.json()["submitted"]

for i in items:
	print(i)
