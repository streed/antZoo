#~/opt/hadoop/bin/hadoop jar ~/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.2.0.jar grep /input out3 '([a-zA-Z]{4,}), (\w+)'
import sys
from collections import defaultdict
import simplejson
import re
 
no = False
if( len( sys.argv ) == 2 ):
  no = sys.argv[1] == "--no"

r = re.compile( r"([a-zA-Z]{4,}), (\w+)" )
c = 0

line = None
while True:
  line = sys.stdin.readline()
  line = line[:-1]

  if( line == "ANT_JOB_DONE" ):
    break

  if( r.match( line ) != None ):
    sys.stdout.write( "%d %s\n" % ( c, line ) )
  else:
    if( not no ):
      sys.stdout.write( "\n" )
  c += 1
  sys.stdout.flush()
