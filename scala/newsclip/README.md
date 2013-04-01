A newspaper clipping service using Cascading (in Scala). We start with a single website, use Bixo to fetch the site to a depth of 5, then analyze the site to find keywords. The keywords are then used as queries to the Bing API and domains collected. If a domain results from two or more such queries, then the domains are added back into Bixo's seed list.

We also use the Apache Cassandra database to store extracted keywords as {url: keyword} pairs so we don't do duplicate work.

This is a toy application inspired by part of an ELance job description to help me learn Cascading.


========

http://www.bing.com/developers/s/APIBasics.html
https://developers.google.com/custom-search/v1/overview
