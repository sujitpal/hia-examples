#!/usr/bin/python

import random

file = open("timeseries.txt", 'w')
for i in range(1, 1500):
  value = random.random() * 100
  file.write("%f\n" % (value))
file.close()
