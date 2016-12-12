#!/usr/bin/python

import datetime, MySQLdb, ConfigParser

def handle():
  with open('handle.log', 'a') as log:
    try:
      cfg = ConfigParser.ConfigParser()
      with open('mysql.conf', 'r') as mysql_conf:
        cfg.readfp(mysql_conf)
        db_conf = cfg.items('db')
        print db_conf
      print 'Test'
    except IOError, e:
      log.write('%s: IOError %s, %s.\n' % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1]))
    except ConfigParser.Error, e:
      log.write('%s: ConfigParserError %s.\n' % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e))

if __name__ == '__main__':
  handle()
  print 'OK'
