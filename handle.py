#!/usr/bin/python -u

import datetime, MySQLdb, ConfigParser, hashlib, time

def handle(directory, date):
  with open('error.log', 'a') as log:
    try:
      md5 = hashlib.md5()
      timestamp = time.mktime(time.strptime(date, '%Y%m%d'))
      count_insert = 0

      # Sql and list of inserting messages
      sql_msg_insert = "insert into messages(no,content,first,last,frequency,md5,day) values(%s,%s,%s,%s,1,%s,%s) on duplicate key update frequency=frequency+1,no=values(no),day=values(day),first=if(first>values(first),values(first),first),last=if(last<values(last),values(last),last)"
      list_msg_insert = []
      total_msg = 0

      # Sql of searching message
      sql_msg_select = "select id, first, last from messages where no = %s and day = '%s'"
      count_msg = 0

      # Sql and list of inserting links
      sql_link_insert = "insert into links values(%s,%s,%s,%s,%s,%s,%s,1) on duplicate key update frequency=frequency+1,monitors=if(last<values(last),values(monitors),monitors),message=if(last<values(last),values(message),message),first=if(first>values(first),values(first),first),last=if(last<values(last),values(last),last)"
      list_link_insert = []
      total_link = 0

      # Sql and list of inserting monitors
      sql_mon_insert = "insert into monitors values(%s,%s,%s,%s,%s,%s,%s,%s,%s,1) on duplicate key update frequency=frequency+1,prefixes=if(last<values(last),values(prefixes),prefixes),message=if(last<values(last),values(message),message),first=if(first>values(first),values(first),first),last=if(last<values(last),values(last),last)"
      list_mon_insert = []
      total_mon = 0

      # Sql and list of inserting origins
      sql_orig_insert = "insert into origins values(%s,%s,%s,%s,%s,%s,%s,1) on duplicate key update frequency=frequency+1,monitors=if(last<values(last),values(monitors),monitors),message=if(last<values(last),values(message),message),first=if(first>values(first),values(first),first),last=if(last<values(last),values(last),last)"
      list_orig_insert = []
      total_orig = 0

      # Sql of searching asset
      sql_asset_select = "select id from asset where md5 = '%s'"
      sql_asset_ai = "select auto_increment from information_schema.tables where table_name = 'asset'" 
      asset_ai = 0
      count_select = 0

      # Sql and list of inserting asset
      count_asset = 0
      sql_asset_insert = "insert into asset(asset, md5) values(%s, %s)"
      list_asset_insert = []

      # Loading configuration of database
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
          dir_origs = ''.join([directory, date, '.origins'])
          # Inserting messages
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
                total_msg += 1
                if count_insert > 999:

                  try:
                    db_cur.executemany(sql_msg_insert, list_msg_insert)
                    db_conn.commit()
                  except MySQLdb.Error, e:
                    print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], total_msg - 1000, total_msg, "messages")
                    return 0

                  list_msg_insert = []
                  count_insert = 0
            if count_insert > 0:

              try:
                db_cur.executemany(sql_msg_insert, list_msg_insert)
                db_conn.commit()
              except MySQLdb.Error, e:
                print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], total_msg - 1000, total_msg, "messages")
                return 0

              list_msg_insert = []
              count_insert = 0
          print "%s: Finished messages" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
          # Inserting links
          with open(dir_links, 'r') as file_links:
            db_cur.execute(sql_asset_ai)
            asset_ai = db_cur.fetchone()[0]
            lines = file_links.readlines()
            for line in lines:
              content = line.strip('\n').split('\t')
              link_as1, link_as2, link_type, link_mons, link_msg = content
              if link_as1.find('{') >= 0:
                md5.update(link_as1)
                asset_md5 = md5.hexdigest()
                count_select = db_cur.execute(sql_asset_select % asset_md5) 
                if count_select > 0:
                  asset_id = db_cur.fetchone()[0]
                  link_as1 = '#' + str(asset_id)
                else:
                  asset_data = (link_as1, asset_md5)
                  list_asset_insert.append(asset_data)
                  count_asset += 1
                  link_as1 = '#' + str(asset_ai)
                  asset_ai += 1
                  if count_asset > 999:

                    try:
                      db_cur.executemany(sql_asset_insert, list_asset_insert)
                      db_conn.commit()
                    except MySQLdb.Error, e:
                      print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], asset_ai - 1000, asset_ai, "asset")
                      db_cur.execute(sql_asset_ai)
                      asset_ai = db_cur.fetchone()[0]

                    list_asset_insert = []
                    count_asset = 0
              if link_as2.find('{') >= 0:
                md5.update(link_as2)
                asset_md5 = md5.hexdigest()
                count_select = db_cur.execute(sql_asset_select % asset_md5)
                if count_select > 0:
                  asset_id = db_cur.fetchone()[0]
                  link_as2 = '#' + str(asset_id)
                else:
                  asset_data = (link_as2, asset_md5)
                  list_asset_insert.append(asset_data)
                  count_asset += 1
                  link_as2 = '#' + str(asset_ai)
                  asset_ai += 1
                  if count_asset > 999:

                    try:
                      db_cur.executemany(sql_asset_insert, list_asset_insert)
                      db_conn.commit()
                    except MySQLdb.Error, e:
                      print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], asset_ai - 1000, asset_ai, "asset")
                      db_cur.execute(sql_asset_ai)
                      asset_ai = db_cur.fetchone()[0]

                    list_asset_insert = []
                    count_asset = 0
              count_msg = db_cur.execute(sql_msg_select % (int(link_msg), date)) 
              if count_msg > 0:
                result_msg = db_cur.fetchone()
                link_msg, link_first, link_last = result_msg
              link_data = (link_as1, link_as2, link_type, link_mons, link_msg, link_first, link_last)
              list_link_insert.append(link_data)
              count_insert += 1
              total_link += 1
              if count_insert > 999:

                try:
                  db_cur.executemany(sql_link_insert, list_link_insert)
                  db_conn.commit()
                except MySQLdb.Error, e:
                  print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], total_link - 1000, total_link, "links")

                list_link_insert = []
                count_insert = 0
            if count_asset > 0:

              try:
                db_cur.executemany(sql_asset_insert, list_asset_insert)
                db_conn.commit()
              except MySQLdb.Error, e:
                print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], asset_ai - 1000, asset_ai, "asset")
                db_cur.execute(sql_asset_ai)
                asset_ai = db_cur.fetchone()[0]

              list_asset_insert = []
              count_asset = 0
            if count_insert > 0:

              try:
                db_cur.executemany(sql_link_insert, list_link_insert)
                db_conn.commit()
              except MySQLdb.Error, e:
                print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], total_link - 1000, total_link, "links")

              list_link_insert = []
              count_insert = 0
          print "%s: Finished links" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

          # Inserting monitors
          with open(dir_mons, 'r') as file_mons:
            db_cur.execute(sql_asset_ai)
            asset_ai = db_cur.fetchone()[0]
            lines = file_mons.readlines()
            for line in lines:
              content = line.strip('\n').split('\t')
              mon_nexthop, mon_asn, mon_peer, mon_peerasn, mon_type, mon_prefixes, mon_msg = content
              count_msg = db_cur.execute(sql_msg_select % (int(mon_msg), date)) 
              if count_msg > 0:
                result_msg = db_cur.fetchone()
                mon_msg, mon_first, mon_last = result_msg
              mon_data = (mon_nexthop, mon_asn, mon_peer, mon_peerasn, mon_type, mon_prefixes, mon_msg, mon_first, mon_last)
              list_mon_insert.append(mon_data)
              count_insert += 1
              total_mon += 1
              if count_insert > 999:

                try:
                  db_cur.executemany(sql_mon_insert, list_mon_insert)
                  db_conn.commit()
                except MySQLdb.Error, e:
                  print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], total_mon - 1000, total_mon, "monitors")

                list_mon_insert = []
                count_insert = 0
            if count_insert > 0:

              try:
                db_cur.executemany(sql_mon_insert, list_mon_insert)
                db_conn.commit()
              except MySQLdb.Error, e:
                print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], total_mon - 1000, total_mon, "monitors")

              list_mon_insert = []
              count_insert = 0
          print "%s: Finished monitors" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

          # Inserting origins
          with open(dir_origs, 'r') as file_origs:
            db_cur.execute(sql_asset_ai)
            asset_ai = db_cur.fetchone()[0]
            lines = file_origs.readlines()
            for line in lines:
              content = line.strip('\n').split('\t')
              orig_prefix, orig_origin, orig_type, orig_mons, orig_msg = content
              if orig_origin.find('{') >= 0:
                md5.update(orig_origin)
                asset_md5 = md5.hexdigest()
                count_select = db_cur.execute(sql_asset_select % asset_md5)
                if count_select > 0:
                  asset_id = db_cur.fetchone()[0]
                  orig_origin = '#' + str(asset_id)
                else:
                  asset_data = (orig_origin, asset_md5)
                  list_asset_insert.append(asset_data)
                  count_asset += 1
                  orig_origin = '#' + str(asset_ai)
                  asset_ai += 1
                  if count_asset > 999:

                    try:
                      db_cur.executemany(sql_asset_insert, list_asset_insert)
                      db_conn.commit()
                    except MySQLdb.Error, e:
                      print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], asset_ai - 1000, asset_ai, "asset")
                      db_cur.execute(sql_asset_ai)
                      asset_ai = db_cur.fetchone()[0]

                    list_asset_insert = []
                    count_asset = 0
              count_msg = db_cur.execute(sql_msg_select % (int(orig_msg), date)) 
              if count_msg > 0:
                result_msg = db_cur.fetchone()
                orig_msg, orig_first, orig_last = result_msg
              orig_data = (orig_prefix, orig_origin, orig_type, orig_mons, orig_msg, orig_first, orig_last)
              list_orig_insert.append(orig_data)
              count_insert += 1
              total_orig += 1
              if count_insert > 999:

                try:
                  db_cur.executemany(sql_orig_insert, list_orig_insert)
                  db_conn.commit()
                except MySQLdb.Error, e:
                  print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], total_orig - 1000, total_orig, "origins")

                list_orig_insert = []
                count_insert = 0
            if count_asset > 0:

              try:
                db_cur.executemany(sql_asset_insert, list_asset_insert)
                db_conn.commit()
              except MySQLdb.Error, e:
                print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], asset_ai - 1000, asset_ai, "asset")
                db_cur.execute(sql_asset_ai)
                asset_ai = db_cur.fetchone()[0]
          
              list_asset_insert = []
              count_asset = 0
            if count_insert > 0:

              try:
                db_cur.executemany(sql_orig_insert, list_orig_insert)
                db_conn.commit()
              except MySQLdb.Error, e:
                print "%s, error %d:%s, between %d-%d in %s." % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e.args[0], e.args[1], total_orig - 1000, total_orig, "origins")

              list_orig_insert = []
              count_insert = 0
          print "%s: Finished origins" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

          # Delete messages that are not used by links, monitors and origins
          db_cur.execute('delete from messages where not exists (select * from (select message from links union select message from monitors union select message from origins) as used where messages.id  = used.message)')
          db_conn.commit()
              
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
