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
	data = searchApi.search(_BUCKET, 'items',
		{
			'start' : start,
			'limit' : limit,
			'sortby' : 'create_ts asc',
			'filter[queries][]' : 'create_ts:[%s TO %s]' % (low_dt, high_dt),
			'filter[fields][type]' : 'submission'
		})
	hits = data['hits']
	items = data['results']
	
	for item in items:
		ui = item['item']
		#dt = datetime.strptime(ui['create_ts'], '%Y-%m-%dT%H:%M:%SZ')
		#file_obj.write('%s,%s\n' %(ui['username'], ui['create_ts']))
        cur = db.cursor()
        cur.execute(
            """SELECT COUNT(id) FROM submissions WHERE id = ?""", (ui['id'],))
        row = cur.fetchone()
        if row and row[0] > 0:
            cur.execute("""UPDATE OR IGNORE submissions SET points = ?
                WHERE id = ?""", (ui['points'], ui['id']))
        else:
            cur.execute(
			    """INSERT OR IGNORE INTO submissions (
					id,
					points,
					username, 
					url,
					domain,
					title,
					text,
					create_ts) 
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
				(ui['id'], ui['points'], ui['username'], ui['url'], 
				 ui['domain'], ui['title'], ui['text'], ui['create_ts']))
        cur.close()
	db.commit()
	
	item_cnt = len(items)
	left = hits - item_cnt
	new_low_dt = items[-1]['item']['create_ts']
	return (left, items, new_low_dt)	
	                    
def check_db(users_db_file = 'hn_submissions.sqlite'):
	db = sqlite3.connect(users_db_file)
	db.execute(
		"""CREATE TABLE IF NOT EXISTS submissions (
				id INTEGER PRIMARY KEY ASC ON CONFLICT FAIL,
				points INTEGER,
				username TEXT,
				url TEXT,
				domain TEXT,
				title TEXT,
				text TEXT,
				create_ts DATETIME)
		""")
	db.execute(
		"""CREATE INDEX IF NOT EXISTS submissions_create_ts_index1 ON submissions (
				create_ts ASC)
		""")
	db.execute(
		"""CREATE INDEX IF NOT EXISTS submissions_username_index1 ON submissions (
				username ASC)
		""")
	db.execute(
		"""CREATE INDEX IF NOT EXISTS submissions_points_index1 ON submissions (
				points ASC)
		""")
	db.execute(
		"""CREATE INDEX IF NOT EXISTS submissions_domain_index1 ON submissions (
				domain ASC)
		""")
	db.commit()
	db.close()

def toJSON(ts):
	return ts.strftime('%Y-%m-%dT%H:%M:%SZ')
	
def download_loop(db, last_dt=None):
	searchApi = thriftdb.SearchAPI()
	
	start = 0
	limit = 100
    
	low_dt = toJSON(datetime(1900, 1, 1))
	if last_dt is not None:
		dt = datetime.strptime(last_dt, '%Y-%m-%dT%H:%M:%SZ')
		low_dt = toJSON(dt + timedelta(days=-5))
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
					
def main(db_file = 'hn_submissions.sqlite'):
	start_time = time.time()
	
	check_db(db_file)
	
	db = sqlite3.connect(db_file)
	cur = db.cursor()
	cur.execute('SELECT MAX(create_ts) FROM submissions;')
	last_dt = cur.fetchone()[0]
	cur.close()
	
	download_loop(db, last_dt)
	
	db.close()
	print 'Finished getting items, took %s.' % timedelta(seconds=(time.time() - start_time))

if __name__ == '__main__':
	main()

