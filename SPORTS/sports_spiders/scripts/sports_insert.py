import MySQLdb
import time
import datetime

conn = MySQLdb.connect(host="10.28.218.81", user = "veveo", passwd='veveo123', db = "SPORTSDB", charset="utf8", use_unicode=True)
cur = conn.cursor()


q1 = 'insert ignore into sports_games_participants (game_id, participant_id, is_home, group_number, created_at, modified_at) values("%s", "%s", 0, "%s", now(), now() )'

q2 = "insert ignore into sports_games_results (game_id, participant_id ,result_type, result_value, created_at, modified_at ) values ('%s', '%s',  '%s', '%s', now(), now())"

q3 = "update sports_games set status= 'completed' where id = '%s' "

game_id = 264925
p_ids = {'147346' : ['1', 'gold', '5:58.61'] , '145060' : ['1', 'gold', '5:58.61'], '145051' : ['1', 'gold', '5:58.61'], '144996' : ['1', 'gold', '5:58.61'],
          '146643' : ['2', 'silver', '6:00.44'], '146642' : ['2', 'silver', '6:00.44'], '146641' : ['2', 'silver', '6:00.44'], '150466' : ['2', 'silver', '6:00.44'],
            '144304' : ['3', 'bronze', '6:03.85'], '144390' : ['3', 'bronze', '6:03.85'] , '144308' : ['3', 'bronze', '6:03.85'], '144389' : ['3', 'bronze', '6:03.85']}
'''
p_ids = {'143362' : ['1', 'gold', '7:04.73'], '143406' : ['1', 'gold', '7:04.73'],
           '146958': ['2', 'silver', '7:05.88'],  '146961' : ['2', 'silver', '7:05.88'],
            '147022': ['3', 'bronze', '7:06.49'], '150052' : ['3', 'bronze', '7:06.49']}
'''

p_ids = {'145317' : ['1', 'gold', '92.365'], '147796' : ['2', 'silver', '92.266'], '142611' : ['3', 'bronze', '90.641']}
count = 0
for p_id, rank in p_ids.iteritems():
    cur.execute(q1 % (game_id, p_id, rank[0]))
    #cur.execute(q1 % (game_id, p_id, 0))
 
    cur.execute(q2 % (game_id, p_id, 'medal', rank[1]))
    cur.execute(q2 % (game_id, p_id, 'rank', rank[0] ))
    cur.execute(q2 % (game_id, p_id, 'final_time', rank[2] ))
    cur.execute(q3 % (game_id))

    if rank[0] == '1':
        _type = 'winner'
        if count >= 1:
            _type = 'winner' + str(count + 1)

        count  = count + 1
        cur.execute(q2 % (game_id, 0, _type, p_id ))
