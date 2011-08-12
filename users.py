#!/usr/bin/env python

"""Not sure what this does yet.

But it will do something.
"""

from datetime import timedelta, datetime
import logging
import os.path
import sqlite3
import thriftdb
import time

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

ch.setFormatter(formatter)

logger.addHandler(ch)

#module constant
_BUCKET = 'api.hnsearch.com'

def scrape(searchApi, start, limit, low_dt, high_dt, db):
	#lucene style filter[queries][]
	#range query [] is inclusive, {} is exclusive, 
	#so {] is exclude/include
	data = searchApi.search(_BUCKET, 'users',
		{
			'start' : start,
			'limit' : limit,
			'sortby' : 'create_ts asc',
			'filter[queries][]' : 'create_ts:[%s TO %s]' % (low_dt, high_dt)
		})
	hits = data['hits']
	items = data['results']
	
	for item in items:
		ui = item['item']
		#dt = datetime.strptime(ui['create_ts'], '%Y-%m-%dT%H:%M:%SZ')
		#file_obj.write('%s,%s\n' %(ui['username'], ui['create_ts']))
		db.execute("INSERT OR IGNORE INTO users (username, create_ts) VALUES (?, ?)",
						(ui['username'], ui['create_ts']))
	db.commit()
	
	item_cnt = len(items)
	left = hits - item_cnt
	new_low_dt = items[-1]['item']['create_ts']
	return (left, items, new_low_dt)	
	                    
def check_db(users_db_file = 'hn_users.sqlite'):
	db = sqlite3.connect(users_db_file)
	db.execute(
		"""CREATE TABLE IF NOT EXISTS users (
				username TEXT PRIMARY KEY ASC ON CONFLICT FAIL,
				create_ts DATETIME)
		""")
	db.execute(
		"""CREATE INDEX IF NOT EXISTS users_create_ts_index1 ON users (
				create_ts ASC)
		""")
	db.commit()
	db.close()

def toJSON(ts):
	return ts.strftime('%Y-%m-%dT%H:%M:%SZ')
	
def download_loop(db, last_dt=None):
	searchApi = thriftdb.SearchAPI()
	
	start = 0
	limit = 100
	low_dt = toJSON(datetime(1900, 1, 1)) if not last_dt else last_dt
	high_dt = toJSON(datetime.now() + timedelta(days=5))

	logger.debug('start: %i, limit: %i, low_dt: %s, high_dt: %s' % (start, limit, low_dt, high_dt))
	left, items, last_dt = scrape(searchApi, start, limit, low_dt, high_dt, db)
	items_cnt = len(items)
	remaining = left - start
	logger.info('Processed %i items, %i items remaining.' % (items_cnt, remaining))
	start += items_cnt
	if start >= 1000:
		low_dt = last_dt
		start = 0
	while remaining > 0:
		#try not to hammer the site
		time.sleep(1)
		logger.debug('start: %i, limit: %i, low_dt: %s, high_dt: %s' % (start, limit, low_dt, high_dt))
		left, items, last_dt = scrape(searchApi, start, limit, low_dt, high_dt, db)
		items_cnt = len(items)
		remaining = left - start
		logger.info('Processed %i items, %i items remaining.' % 
					(items_cnt, remaining))
		start += items_cnt
		if start >= 1000:
			low_dt = last_dt
			start = 0
					
def main(db_file = 'hn_users.sqlite'):
	start_time = time.time()
	
	check_db(db_file)
	
	db = sqlite3.connect(db_file)
	cur = db.cursor()
	cur.execute('SELECT MAX(create_ts) FROM users;')
	last_dt = cur.fetchone()[0]
	cur.close()
	
	download_loop(db, last_dt)
	
	db.close()
	print 'Finished getting items, took %s.' % timedelta(seconds=(time.time() - start_time))

if __name__ == '__main__':
	main()

