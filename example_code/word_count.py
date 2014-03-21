import sys
from collections import defaultdict
import simplejson

line = None
while True:
  line = sys.stdin.readline()
  line = line[:-1]

  if( line == "ANT_JOB_DONE" ):
    break

  words = line.split( ' ' )

  counts = defaultdict( int ) 

  for w in words:
    counts[w] += 1

  print simplejson.dumps( counts )
  sys.stdout.flush()
