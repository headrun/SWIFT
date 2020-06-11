import MySQLdb

cursor = MySQLdb.connect(db="SPORTSDB", host="10.28.218.81", user="veveo", passwd="veveo123").cursor()


query = 'select game_id, program_id from sports_rovi_games_merge_may14'
cursor.execute(query)
records = [ record for record in cursor.fetchall() ]


for record in records:
	game_id, program_id = record
	query = 'insert into sports_rovi_games_merge(game_id, program_id, schedule_id, modified_at, created_at) values(%s, %s, 0, now(), now()) on duplicate key update modified_at=now(), game_id=%s'
	cursor.execute(query, (game_id, program_id, game_id))
