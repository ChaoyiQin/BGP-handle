#! /usr/bin/python -u
import sys, getopt, datetime
from handle import handle

opts, args = getopt.getopt(sys.argv[1:], "hs:e:")
date_s = ''
date_e = ''
dir_bgp = '/home/hitnis/qcy/BGP/analysis/dailyresults/'
help_doc = "\
Usage: process.py [option] [arg]\n\
Options and arguments:\n\
-e date	: end date of processing\n\
-h	: print this help document and exit\n\
-s date	: start date of processing\
"

for op, value in opts:
  if op == '-h':
    print help_doc
    sys.exit()
  elif op == '-s':
    date_s = value
  elif op == '-e':
    date_e = value

if date_s == '' or date_e == '':
  print "Please input start date and end date!"
  print help_doc
  sys.exit()

date_s = datetime.datetime.strptime(date_s, "%Y%m%d")
date_e = datetime.datetime.strptime(date_e, "%Y%m%d")

for i in range((date_e - date_s).days + 1):
  day = (date_s + datetime.timedelta(days = i)).strftime('%Y%m%d')
  print "%s: Started processing %s" % (datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'), day)
  handle(dir_bgp, day)
  print "%s: Finished processing %s" % (datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S'), day)
