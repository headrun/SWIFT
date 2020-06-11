import MySQLdb

conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSRADARDB", charset="utf8", use_unicode=True)
cur = conn.cursor()

qry = 'select radar_gid, sportsdb_gid from sports_radar_merge_gids'
cur.execute(qry)
rows = cur.fetchall()
for row in rows:
    rad , spd = row
    qry = 'select id from sports_players where gid = %s'
    vals = (rad.strip('SRPL'))
    cur.execute(qry, vals)
    radar_id = cur.fetchone()[0]
    qry = 'select id from SPORTSDB.sports_participants where gid = %s'
    vals = (spd)
    cur.execute(qry, vals)
    sp_id = cur.fetchone()[0]
    qry = 'insert ignore into sports_radar_merge_gids_bkp (radar_id, sportsdb_id, type, created_at, modified_at) values (%s, %s, %s, now(), now())'
    vals = (radar_id, sp_id, 'player')
    cur.execute(qry, vals)
cur.close()
conn.close()

