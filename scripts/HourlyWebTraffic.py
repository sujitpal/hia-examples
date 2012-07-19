#!/usr/bin/python
import re
import os

p1 = re.compile("\A\w{3}\s\d{1,2},\s\d{4}\s\d{2}:\d{2}:\d{2}\s\w{2}\s")
p2 = re.compile("webapp=/solr")
dir = "../data/logs"
files = os.listdir(dir)
prefix = None
for file in files:
  filename = "/".join([dir, file])
  f = open(filename, 'r')
  for line in f:
    if prefix != None and p2.search(line) != None:
      print " ".join([prefix[:-1], line[:-1]])
    if p1.match(line):
      prefix = line
    else:
      prefix = None
  f.close()
