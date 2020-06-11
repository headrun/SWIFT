import MySQLdb


#conn = MySQLdb.connect(host = '10.4.18.183', user = 'root', db = 'SPORTSDB')
conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd='veveo123', db="SPORTSDB", charset='utf8', use_unicode=True)
cursor = conn.cursor()

outfile =  open('player_ids', 'w+')
query = 'select distinct(title), count(*) from sports_participants where game = "Athletics" group by title having count(*) > 1 order by count(*) desc;'
cursor.execute(query)
records = cursor.fetchall()
for record in records:
    title, count = record
    query = 'select id from sports_participants where game = "Athletics" and title = "%s"' % title
    cursor.execute(query)
    player_titles = cursor.fetchall()
    for ptitles in player_titles:
        pid = ptitles[0]
        pquery = 'delete from sports_participants where game = "Athletics" and id = %s limit 1' % pid
        cursor.execute(pquery)
        plquery = 'delete from sports_players where participant_id = %s limit 1' % pid
        cursor.execute(plquery)
        squery = 'delete from sports_source_keys where entity_type = "participant" and entity_id = %s and source = "cw_glasgow" limit 1' % pid
        cursor.execute(squery)
        tquery = 'delete from sports_tournaments_participants where participant_id = %s limit 1' % pid
        cursor.execute(tquery)

        outfile.write('%s' % repr(pid))

