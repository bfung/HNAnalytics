#!/usr/bin/env python

"""Python client library for ThriftDB.

Designed to be an abstraction over the REST api exposed by ThriftDB.

"""

import httplib
import urllib
import logging

# Find a JSON parser
try:
    import json
    _parse_json = lambda s: json.loads(s)
except ImportError:
    try:
        import simplejson
        _parse_json = lambda s: simplejson.loads(s)
    except ImportError:
        # For Google AppEngine
        from django.utils import simplejson
        _parse_json = lambda s: simplejson.loads(s)

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s- %(levelname)s - %(message)s')

ch.setFormatter(formatter)

logger.addHandler(ch)

#module constant
_THRIFTDB_API_DOMAIN = "api.thriftdb.com"

class BucketAPI(object):
	"""Abstraction of the ThriftDB Bucket API

		see http://www.thriftdb.com/documentation/rest-api/bucket-api
	"""
	def __init__(self, domain=_THRIFTDB_API_DOMAIN):
		self.domain = domain

	def _bucket_api(self, bucket, method):
		conn = httplib.HTTPConnection(self.domain)
		conn.request(method, urllib.quote("/" + bucket))
		try:
			response = conn.getresponse()
			logger.info(response.read())
		finally:
			conn.close()
		logger.info("Response status code: %i" % (response.status))
		return response.status

	def create(self, bucket):
		"""Returns true if bucket created else false if there was an error"""
		status = self._bucket_api(bucket, 'PUT')		
		return status == httplib.CREATED

	def read(self, bucket):
		"""Returns true if bucket exists else false if there was an error"""
		status = self._bucket_api(bucket, 'GET')
		return status == httplib.OK

	def delete(self, bucket):
		"""Returns true if bucket deleted else false if there was an error"""
		status = self._bucket_api(bucket, 'DELETE')
		return status == httplib.OK

class SearchAPI(object):
	"""Abstraction for ThriftDB Search API	
	"""
	def __init__(self, domain=_THRIFTDB_API_DOMAIN):
		self.domain = domain

	def search(self, bucket, collection, query=None):
		"""Calls the ThriftDB Search API.

		see http://www.thriftdb.com/documentation/rest-api/search-api
		"""
		if not query: query={}
		conn = httplib.HTTPConnection(self.domain)
		resource = '/%s/%s/_search?%s' % (
						urllib.quote(bucket), 
						urllib.quote(collection),
						urllib.urlencode(query))
		conn.request('GET', resource)
		try:
			response = conn.getresponse()
			data = _parse_json(response.read())
		finally:
			conn.close()
		return data

if __name__ == '__main__':
	#api.create_bucket("test_bucket")
	#api.delete_bucket("test_bucket")
	searchApi = SearchAPI()
	print searchApi.search('api.hnsearch.com', 'items', 
			{
				'sortby' : 'create_ts asc', 
				'filter[fields][type]' : 'submission'
			})
