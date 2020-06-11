import MySQLdb

connection = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
cursor = connection.cursor()
query = "select G.gid, G.game_datetime, G.game, R.game_id, count(*) from sports_games G, sports_games_results R where G.id = R.game_id and R.result_type = 'winner' and G.game = 'soccer' and game_datetime >= (curdate() - interval 70 day) group by game_id  having count(*) > 1"
cursor.execute(query)
records = cursor.fetchall()
for record in records:
    game_id = record[3]
    query = "delete from sports_games_results where participant_id != '' and result_type = 'winner' and game_id = %s limit 1"
    values = (game_id)
    cursor.execute(query, values)
