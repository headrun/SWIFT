import MySQLdb

conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset="utf8", use_unicode=True)
cur = conn.cursor()


qry = "select id, reference_url from sports_participants where reference_url like '%http://twitter.com/%'"
cur.execute(qry)

rows = cur.fetchall()

#http://twitter.com/KendrickPerkinshttp://www.nba.com/playerfile/kendrick_perkins

for row in rows:
    p_id = row[0]
    _link = 'http:' + row[1].split('http:')[-1]
    _qry = 'update sports_participants set reference_url = %s where id = %s'
    vals = (_link, p_id)
    cur.execute(_qry, vals)
