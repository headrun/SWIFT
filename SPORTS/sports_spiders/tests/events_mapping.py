import MySQLdb

conn1 = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB")
cursor1 = conn1.cursor()
conn2 = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB")
cursor2 = conn2.cursor()
query = "select id, event_id from sports_games"
cursor2.execute(query)
ids = cursor2.fetchall()
for game_id, event_id in ids:
    query = "update sports_games set event_id = %s where id = %s" %(str(event_id), str(game_id))
    try:
        cursor1.execute(query)
    except:
        pass
