#!/usr/bin/python
import random
import datetime

def gen_data(f, legend, n):
  xsig = 1
  ysig = 1
  for i in range(0,n):
    x = random.random()
    y = random.random()
    mics = datetime.datetime.utcnow().microsecond
    xmult = int((x ** 2) * 10)
    if xmult % 2 == 0:
      if mics % 2 == 0:
        xsig = xsig * (-1)
    else:
      if mics %2 != 0:
        xsig = xsig * (-1)
    mics = datetime.datetime.utcnow().microsecond
    ymult = int((y ** 2) * 10)
    if ymult % 2 == 0:
      if mics % 2 != 0:
        ysig = ysig * (-1)
    else:
      if mics % 2 == 0:
        ysig = ysig * (-1)
    x = x * (10 ** xmult) * xsig
    y = y * (10 ** ymult) * ysig
    f.write("%s,%f,%f\n" % (legend, x, y))

def main():
  foofile = open("/tmp/foos.txt", 'w')
  #gen_data(foofile, "FOO", 500000)
  gen_data(foofile, "FOO", 10000)
  foofile.close()
  #barfile = open("/tmp/bars.txt", 'w')
  barfile = open("/tmp/bars.txt", 'w')
  gen_data(barfile, "BAR", 100)
  barfile.close()

if __name__ == "__main__":
  main()
