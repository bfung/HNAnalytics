#!/usr/bin/env python

"""Not sure what this does yet.

But it will do something.
"""

from datetime import timedelta
from datetime import datetime
import logging
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

if __name__ == '__main__':
	hnsearch = HNSearchAPI()
	left = -1
	init = True
	now_dt = toJSON(datetime.now() + timedelta(days=5))
	low_dt = datetime(1900, 1, 1)
	start_time = time.time()
	with open('hn_users.csv', 'w') as f:
		while left > 0 or init:
			#lucene style filter[queries][]
			#range query [] is inclusive, {} is exclusive, 
			#so {] is exclude/include
			data = hnsearch.users(
			{
				'limit' : 100,
				'sortby' : 'create_ts asc',
				'filter[queries][]' : 'create_ts:{%s TO %s]' % (toJSON(low_dt), now_dt)
			})
			user_count = data['hits']
			user_items = data['results']
			left = user_count
			if user_count <= 0:
				break
			if init:
				init = False
			for user_item in user_items:
				ui = user_item['item']
				dt = datetime.strptime(ui['create_ts'], '%Y-%m-%dT%H:%M:%SZ')
				if dt > low_dt:
					low_dt = dt
				#u = User(ui['username'], dt)
				f.write('%s, %s\n' %(ui['username'], dt))
			scraped = len(user_items)
			logger.info('Scraped %i, %i users left to process...' % (scraped, left - scraped))
	print 'took %s seconds.' % (time.time() - start_time)
