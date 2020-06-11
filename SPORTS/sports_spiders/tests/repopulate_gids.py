import MySQLdb

conn   = MySQLdb.connect(host="10.4.18.183", db="SPORTSDB", user="root")
cursor = conn.cursor()

cursor.execute("select gid from sports_participants where gid not like 'TEAM%' and gid not like 'PL%'")
gids = cursor.fetchall()

counter = 0
for gid in gids:
  gid = gid[0]
  cursor.execute("select * from sports_participants where gid like 'TEAM%s' or gid like 'PL%s'" %(str(gid), str(gid)))
  value = cursor.fetchall()
  if not value:
      counter += 1
      print counter
      if counter == 1000:
          break
      cursor.execute("update sports_participants set gid = 'PL%s' where gid = %s" %(str(gid), str(gid)))
