#!/usr/bin/env python

"""Not sure what this does yet.

But it will do something.
"""

from datetime import timedelta
from datetime import datetime
import logging
import os.path
import thriftdb
import time

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s- %(levelname)s - %(message)s')

ch.setFormatter(formatter)

logger.addHandler(ch)

#module constant
_BUCKET = 'api.hnsearch.com'

class HNSearchAPI(object):
	"""Abstraction over the datasource for Hacker News
	
	Blah?
	"""
	def __init__(self):
		self._searchApi = thriftdb.SearchAPI()
	
	def users(self, args=None):
		if not args:
			args = {}
		return self._searchApi.search(_BUCKET, 'users', args)
		
	def user_count(self, args=None):
		data = self.users(args)
		return data['hits']
	
	def item_count(self, args=None):
		if not args: 
			args = {}
		data = self._searchApi.search(_BUCKET, 'items', args)
		return data['hits']
		
	def submission_count(self):
		return self.item_count({'filter[fields][type]' : 'submission'})
	
	def comment_count(self):
		return self.item_count({'filter[fields][type]' : 'comment'})

class User(object):
	"""Users"""
	def __init__(self, username, create_ts):
		self.username = username
		self.create_ts = create_ts
		
	def create_ts_JSON(self):
		return self.create_ts.strftime('%Y-%m-%dT%H:%M:%SZ')
	
	def __str__(self):
		return self.username

def toJSON(ts):
	return ts.strftime('%Y-%m-%dT%H:%M:%SZ')

def scrape_users(hnsearch, low_dt, high_dt):
	#lucene style filter[queries][]
	#range query [] is inclusive, {} is exclusive, 
	#so {] is exclude/include
	data = hnsearch.users(
		{
			'limit' : 100,
			'sortby' : 'create_ts asc',
			'filter[queries][]' : 'create_ts:{%s TO %s]' % (low_dt, high_dt)
		})
	hits = data['hits']
	items = data['results']
	left = hits - len(items)
	new_low_dt = items[-1]['item']['create_ts']
	return (left, items, new_low_dt)

def dump_users(file_obj, items):
	for item in items:
		ui = item['item']
		dt = datetime.strptime(ui['create_ts'], '%Y-%m-%dT%H:%M:%SZ')
		file_obj.write('%s, %s\n' %(ui['username'], dt))

def tail(f, n, offset=None):
	"""Reads a n lines from f with an offset of offset lines.  The return
	value is a tuple in the form ``(lines, has_more)`` where `has_more` is
	an indicator that is `True` if there are more lines in the file.
	"""
	avg_line_length = 74
	to_read = n + (offset or 0)

	while 1:
		try:
			f.seek(-(avg_line_length * to_read), 2)
		except IOError:
			try:
				# woops.  apparently file is smaller than what we want
				# to step back, go to the beginning instead
				f.seek(0)
			except IOError:
				return []
		pos = f.tell()
		lines = f.read().splitlines()
		if len(lines) >= to_read or pos == 0:
			return lines[-to_read:offset and -offset or None]
		avg_line_length *= 1.3

def main(users_file='hn_users.csv'):
	start_time = time.time()
	hnsearch = HNSearchAPI()
	
	high_dt = toJSON(datetime.now() + timedelta(days=5))
	low_dt = toJSON(datetime(1900, 1, 1))
	if os.path.exists(users_file):
		with open(users_file, 'r') as f:
			last_line = tail(f, 1)
			if len(last_line) == 1:
				parts = last_line[0].split(' ')
				low_dt = '%sT%sZ' % (parts[1], parts[2])

	with open(users_file, 'a') as f:
		left, items, low_dt = scrape_users(hnsearch, low_dt, high_dt)
		dump_users(f, items)
		logger.info('Processed %i users, %i users remaining.' % (len(items), left))
		while left > 0:
			#try not to hammer the site
			time.sleep(0.9)
			left, items, low_dt = scrape_users(hnsearch, low_dt, high_dt)
			dump_users(f, items)
			logger.info('Processed %i users, %i users remaining.' % (len(items), left))
	
	print 'Finished, took %s.' % timedelta(seconds=(time.time() - start_time))
	
if __name__ == '__main__':
	main()

