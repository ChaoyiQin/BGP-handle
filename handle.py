#!/usr/bin/python

import datetime, MySQLdb, ConfigParser, hashlib, time

def handle(directory, date):
  with open('error.log', 'a') as log:
    try:
      md5 = hashlib.md5()
      timestamp = time.mktime(time.strptime(date, '%Y%m%d'))
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
          dir_msgs = ''.join([directory, date, '.messages'])
          dir_links = ''.join([directory, date, '.links'])
          dir_mons = ''.join([directory, date, '.monitors'])
          dir_orig = ''.join([directory, date, '.origins'])
          with open(dir_msgs, 'r') as file_msgs:
            lines = file_msgs.readlines()
            for line in lines:
              content = line.strip('\n').split('\t', 2)
              msg_no = content[0]
              if msg_no is not 'F':
                if content[1][-1] is '|':
                  msg_cont = content[1].split('|', 2)
                  msg_timestamp = msg_cont[1]
                  md5.update(msg_cont[2])
                else:
                  msg_cont = content[1]
                  msg_timestamp = timestamp
                  md5.update(msg_cont)
                msg_md5 = md5.hexdigest()
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
  handle('/home/hitnis/qcy/BGP/analysis/dailyresults/','20160301')
  print 'OK'
