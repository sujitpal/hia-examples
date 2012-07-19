#!/usr/bin/python

import random

file1 = open("matrix1", 'w')
for i in range(1, 2500):
  value = random.random()
  if value >= 0.5:
    file1.write("%d,%f\n" % (i, value))
file1.close()
file2 = open("matrix2", 'w')
for i in range(1, 2500):
  value = random.random()
  if value < 0.25 or value > 0.75:
    file2.write("%d,%f\n" % (i, value))
file.close()
