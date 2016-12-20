#!/usr/bin/python -u

import datetime, MySQLdb, ConfigParser, hashlib, time

def handle(directory, date):
  with open('error.log', 'a') as log:
    try:
      md5 = hashlib.md5()
      timestamp = time.mktime(time.strptime(date, '%Y%m%d'))
      sql_msg_insert = "insert into messages(no,content,first,last,frequency,md5,day) values(%s,%s,%s,%s,1,%s,%s) on duplicate key update frequency=frequency+1,no=if(last<values(last),values(no),no),day=if(last<values(last),values(day),day),first=if(first>values(first),values(first),first),last=if(last<values(last),values(last),last)"
      list_msg_insert = []
      count_insert = 0
      cfg = ConfigParser.ConfigParser()
      with open('mysql.conf', 'r') as mysql_conf:
        cfg.readfp(mysql_conf)
        db_host = cfg.get('db', 'db_host')
        db_port = cfg.get('db', 'db_port')
        db_user = cfg.get('db', 'db_user')
        db_pass = cfg.get('db', 'db_pass')
        try:
          db_conn = MySQLdb.connect(host = db_host, user = db_user, passwd = db_pass, port = int(db_port), db = 'bgp')
          db_cur = db_conn.cursor()
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
                  msg_content = content[1].split('|', 2)
                  msg_timestamp = msg_content[1]
                  md5.update(msg_content[2])
                  msg_cont = content[1]
                else:
                  msg_cont = content[1]
                  msg_timestamp = timestamp
                  md5.update(msg_cont)
                msg_md5 = md5.hexdigest()
                msg_data = (msg_no, msg_cont, msg_timestamp, msg_timestamp, msg_md5, date)
                list_msg_insert.append(msg_data)
                count_insert += 1
                if count_insert > 999:
                  db_cur.executemany(sql_msg_insert, list_msg_insert)
                  db_conn.commit()
                  list_msg_insert = []
                  count_insert = 0
            if count_insert > 0:
              db_cur.executemany(sql_msg_insert, list_msg_insert)
              db_conn.commit()
              list_msg_insert = []
              count_insert = 0
               
          '''
          with open(dir_links, 'r') as file_links:
            lines = file_links.readlines()
            for line in lines:
              content = line.strip('\n').split('\t')
              link_as1, link_as2, link_type, link_mons, link_msg = content
          '''
        finally:
          if 'db_cur' in locals():
            db_cur.close()
          if 'db_conn' in locals():
            db_conn.close()
    except IOError, e:
#      log.write('%s: IOError %s, %s.\n' % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1]))
      log.write('%s: IOError %s.\n' % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e))
    except ConfigParser.Error, e:
      log.write('%s: ConfigParserError %s.\n' % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e))

if __name__ == '__main__':
  print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  handle('/home/hitnis/qcy/BGP/analysis/dailyresults/','20160301')
  print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
