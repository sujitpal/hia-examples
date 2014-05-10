import os
import sys

def usage():
  print "Usage: %s indir outfile skipheader" % (sys.argv[0])
  print "where:"
  print "indir - input directory of files to concat"
  print "outfile - outfile file to concat to"
  print "skipheader - true if header to be skipped else false"
  sys.exit(-1)

def main():
  if len(sys.argv) != 4 \
     or not os.path.isdir(sys.argv[1]) \
     or sys.argv[3] not in ["true", "false"]:
    usage()
  fout = open(sys.argv[2], 'wb')
  for fn in os.listdir(sys.argv[1]):
    print "Now processing: %s" % (fn)
    fin = open(os.path.join(sys.argv[1], fn), 'rb')
    should_skip_line = sys.argv[3] == "true"
    for line in fin:
      if should_skip_line: 
        should_skip_line = False
        continue
      fout.write(line)
    fin.close()
  fout.close()

if __name__ == "__main__":
  main()
