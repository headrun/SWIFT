import MySQLdb


db = MySQLdb.connect(host = "10.28.218.81", user = "veveo", passwd = "veveo123",db = "SPORTSDB",charset='utf8', use_unicode=True)
cur = db.cursor()

cur.execute("select id, group_name from sports_tournaments_groups where tournament_id =461 order by group_name")
tournaments = cur.fetchall()
count=0
query  =  'insert into sports_tournaments_results(tournament_id, participant_id, season, id_type, result_type, result_sub_type, result_value, modified_at) values(%s, %s, %s, %s,%s,%s,%s, now()) on duplicate key update modified_at=now()'
for rec in tournaments:
      participant_id   = rec[0]
      c_title = rec[1]
      tournament_id = '461'
      season = '2016'
      result_type = 'standings'
      result_value = 0
      id_type = "group"
      count = count +1
      values = (tournament_id,participant_id,season,id_type,result_type,'rank',count)
      cur.execute(query,values)
      result_sub_type_list = ['gold','silver','bronze','total']
      for result_sub_type in result_sub_type_list:
          values = (tournament_id,participant_id,season,id_type,result_type,result_sub_type,'0')
          cur.execute(query,values)
cur.close()
