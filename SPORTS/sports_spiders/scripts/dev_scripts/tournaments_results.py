import MySQLdb
import datetime
import time


#conn = MySQLdb.connect(host = '10.4.15.132', db = 'SPORTSDB_BKP', user = 'root')
conn = MySQLdb.connect(host = '10.4.18.183', db = 'SPORTSDB', user = 'root')
cur = conn.cursor()

TOU_WINNER = 'insert into sports_tournaments_results (tournament_id, participant_id, season, id_type, result_type, result_sub_type, result_value, modified_at) values (%s, %s, %s, %s, %s, %s, %s, now()) on duplicate key update result_value=%s'

def get_participant_id(tou_id):
    tou_res = 'select participant_id from sports_tournaments_results where tournament_id = %s and result_sub_type = "rank" and result_type = "standings" and result_value = "1"' % tou_id
    cur.execute(tou_res)
    pid = cur.fetchone()
    if pid:
        pid = pid[0]

    return pid

def get_game_status(_type, tou_id, max_date):
    status_list = []
    gm_query = 'select id, status from sports_games where game = "soccer" and %s = %s and game_datetime = "%s" and status != "Hole"' %(_type, tou_id, max_date)
    cur.execute(gm_query)
    status  = cur.fetchall()
    gid = ''
    if status:
        for st in status:
            gid, status = st
            status_list.append(status)

    return gid, status_list

def get_max_date(_type, _id):
    max_query = 'select max(game_datetime) from sports_games where %s  = %s' % (_type, _id)
    cur.execute(max_query)
    max_date = cur.fetchone()
    if max_date:
        max_date = max_date[0]

    return max_date

def tournaments_results_check(tou_id):
    tou_res_check = 'select id from sports_tournaments_results where tournament_id = %s and result_type = "winner" and season = year(now())' % tou_id
    cur.execute(tou_res_check)
    tou_winner = cur.fetchone()

    return tou_winner

def get_game_winner(gid):
    query = 'select result_value from sports_games_results where result_type = "winner" and game_id = %s'
    values = (gid)
    cur.execute(query, values)
    pid = cur.fetchone()
    if pid:
        pid = pid[0]

    return pid

def get_tournament_id(title):
    query = 'select id from sports_tournaments where title = %s'
    values = (title)
    cur.execute(query, values)
    tid = cur.fetchone()
    if tid:
        tid = tid[0]

    return tid

def main():
    query = 'select id, title, season_end from sports_tournaments where game = "soccer" and type = "tournament" and date(season_end) <= date(now()) order by title'
    query = "select id, title, season_end from sports_tournaments where id not in (select tournament_id from sports_tournaments_results where season = '2014' and result_type = 'winner') and type = 'tournament' and season_end < curdate() and date(season_start) > 2013-01-01 and affiliation != 'obsolete' and game = 'soccer' and id not in (983, 984, 985, 986, 987, 988, 990, 991, 992, 213, 454, 455, 456, 577, 891, 928, 931, 981, 989, 589, 946, 948, 989, 993, 994, 949, 67, 585, 881, 882, 980, 982, 1244, 525) order by season_end"
    cur.execute(query)
    tou_data = cur.fetchall()
    if tou_data:
        for details in tou_data:
            tou_id, title, season_end = details
            max_date = get_max_date('tournament_id', tou_id)
            gid, status = get_game_status('tournament_id', tou_id, max_date)
            if ('scheduled') not in status:
                pid = get_participant_id(tou_id) 
                tou_winner = tournaments_results_check(tou_id)
                if not tou_winner and pid:
                    values  = (tou_id, '', '2014', '', 'winner', '', pid, pid)
                    cur.execute(TOU_WINNER, (values))
                elif 'Cup' in title:
                    tid = get_tournament_id(title)
                    pid = get_game_winner(gid)
                    season = season_end.year
                    if pid and tid:
                        if str(season) == '2015':
                            season = '2014'
                        values  = (tou_id, '', season, '', 'winner', '', pid, pid)
                        cur.execute(TOU_WINNER, (values))
            else:
                print "games are in scheduled status for %s" % title

    query = 'select id, title, season_end from sports_tournaments where  type = "event" and title like "% Final"'
    cur.execute(query)
    eve_data = cur.fetchall()
    if eve_data:
        for details in eve_data:
            eve_id, etitle, season_end = details
            max_date = get_max_date('event_id', eve_id)
            gid, status = get_game_status('event_id',eve_id, max_date)
            if ('completed') in status:
                title = etitle.split('Final')[0].strip()
                pid = get_game_winner(gid)
                tid = get_tournament_id(title)
                if pid and tid:
                    season = season_end.year
                    if str(season) == '2015':
                        season = '2014'
                    values  = (tid, '', season, '', 'winner', '', pid, pid)
                    cur.execute(TOU_WINNER, (values))

if __name__=='__main__':
    main()

