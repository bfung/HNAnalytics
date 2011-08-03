About
=====
Scrapes [Hacker News](http://news.ycombinator.com) users and submissions 
using [HNSearch](http://www.hnsearch.com)'s api and stores the data in sqlite 
databases.

The scripts incrementally scrape based on the last date scraped.  A full 
download currently takes:

users : 77,000 items, ~25 minutes, 6 MB database (w/indicies)
submissions : 430,000 items, ~3.5 hours, 147 MB database (w/indicies)

Database downloads will be available later.

An example of things to do with the data can be seen at [this google spreadsheet](https://spreadsheets.google.com/spreadsheet/ccc?key=0AsoY_yr0BJCVdG5jME83ckxvN1pTZlJ2VUNJYUd0Tnc&hl=en_US)
