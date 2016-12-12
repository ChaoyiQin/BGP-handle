#!/usr/bin/python

import datetime, MySQLdb, ConfigParser

def handle():
  with open('error.log', 'a') as log:
    try:
      cfg = ConfigParser.ConfigParser()
      with open('mysql.conf', 'r') as mysql_conf:
        cfg.readfp(mysql_conf)
        db_host = cfg.get('db', 'db_host')
        db_port = cfg.get('db', 'db_port')
        db_user = cfg.get('db', 'db_user')
        db_pass = cfg.get('db', 'db_pass')
        try:
          db_conn = MySQLdb.connect(host = db_host, user = db_user, passwd = db_pass, port = int(db_port), db = 'bgp')
          db_cursor = db_conn.cursor()
        finally:
          if 'db_cursor' in locals():
            db_cursor.close()
          if 'db_conn' in locals():
            db_conn.close()
    except IOError, e:
#      log.write('%s: IOError %s, %s.\n' % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1]))
      log.write('%s: IOError %s.\n' % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e))
    except ConfigParser.Error, e:
      log.write('%s: ConfigParserError %s.\n' % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e))

if __name__ == '__main__':
  handle()
  print 'OK'
