import MySQLdb

sp_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
cur = sp_cur.cursor()

lines = open('tennis_thuuz_merge.txt', 'r').readlines()
_file = open('filed_parts.txt', 'wb')

for line in lines:
    ti = line.split(',')[0].strip(' %')
    tou_id = line.split(',')[-1].strip(' \n')

    qry = 'select id from sports_participants where title like "%s"'
    cur.execute(qry%ti)
    try:
        par_id = cur.fetchone()[0]
    except:
        par_id = ''

    if par_id:
        print ti, par_id, tou_id
        in_qry = 'insert into sports_tournaments_participants ( participant_id, tournament_id, status, status_remarks, standing, seed, season, created_at, modified_at) \
                 values (%s, %s, "active", "", "", 0, "2017", now(), now()) on duplicate key update modified_at = now()'
        vals = (par_id, int(tou_id))
        cur.execute(in_qry, vals)
    else:
        _file.write('%s\n'%line)

cur.close()
sp_cur.close()
