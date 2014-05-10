import os
import sys

def usage():
  print "Usage: %s infile outfile" % (sys.argv[0])
  print "where:"
  print "infile - input CSV file without line number"
  print "outfile - output CSV file with line number as first col"
  sys.exit(-1)

def main():
  if len(sys.argv) != 3 \
      or not os.path.isfile(sys.argv[1]):
    usage()
  fin = open(sys.argv[1], 'rb')
  fout = open(sys.argv[2], 'wb')
  lno = 0
  for line in fin:
    fout.write("%d,%s" % (lno, line))
    lno += 1
  fin.close()
  fout.close()

if __name__ == "__main__":
  main()
