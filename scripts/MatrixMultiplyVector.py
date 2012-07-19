#!/usr/bin/python

import random

mfile = open("mult_matrix.txt", 'w')
for row in range(0, 500):
  for col in range(0, 500):
    value = random.random()
    if value < 0.25 or value > 0.75:
      mfile.write("%d,%d,%f\n" % (row, col, value))
mfile.close()
vfile = open("mult_vector.txt", 'w')
for row in range(0,500):
  value = random.random()
  if value > 0.5:
    vfile.write("%d,%f\n" % (row, value))
vfile.close()
