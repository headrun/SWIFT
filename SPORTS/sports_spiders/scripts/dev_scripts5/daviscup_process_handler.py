import os
import os.path
import sys
import optparse
import logging
import MySQLdb
import glob
import time
import datetime

SCHEMA_DIR = '/home/veveo/release/server/dbuniverse/schemas'

SK_CHECK_QUERY = "select entity_id from sports_source_keys where entity_type=%s and source_key=%s and source=%s"

GAME_INSERTION = "INSERT IGNORE INTO sports_games (id, gid, game_datetime, game, game_note, tournament_id, status, channels, radio, online, reference_url, event_id, location_id, stadium_id, created_at, modified_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"
INSERT_SKS = "INSERT INTO sports_source_keys (entity_id, entity_type, source, source_key, created_at) VALUES (%s, %s, %s, %s, now())"

GAME_PTS = "INSERT INTO sports_games_participants (game_id, participant_id, is_home, group_number, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE modified_at=now() , participant_id = %s"

PARTICIPANT_ID = 'select entity_id from sports_source_keys where entity_type="participant" and source_key="%s" and source="daviscup_tennis"'

UPDATE_STATUS = "update sports_games set status=%s, game_datetime=%s, tournament_id=%s,  reference_url=%s, location_id=%s, stadium_id=%s  where id=%s"

GAME_RTS = "INSERT INTO sports_games_results (game_id, participant_id, result_type, result_value, created_at, modified_at) VALUES(%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE participant_id=%s, result_value=%s, modified_at=now()"

TOU_EVT = "INSERT INTO sports_tournaments_events (tournament_id, event_id, sequence_num, status, created_at, modified_at) VALUES(%s, %s, '', '', now(), now()) ON DUPLICATE KEY UPDATE tournament_id=%s, event_id=%s, modified_at=now()"
TOU_PTS = "INSERT IGNORE INTO sports_tournaments_participants(participant_id, tournament_id, status, status_remarks, standing, seed, season, created_at, modified_at) values (%s, %s, '', '', '', 0, '2014', now(), now())"

LOC_INSERTION = "INSERT IGNORE INTO sports_locations (continent, country, state, city, street, zipcode, latlong, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, now())"

STD_INSERTION = "INSERT INTO sports_stadiums (gid, title, location_id, created_at) VALUES (%s, %s, %s, now()) ON DUPLICATE KEY UPDATE location_id=%s"

log = logging.getLogger()


def init_logger(log_fname, debug_mode = False):
    handler = logging.FileHandler(log_fname)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    if not debug_mode: log.setLevel(logging.INFO)
    else: log.setLevel(logging.DEBUG)

def load_file(fname, schema_dir):
    db = svdb.SuperVdb(log=log, dbfile=fname,
                       schema_directory=schema_dir,
                       dbformat='SVDB1.0')
    nfailed = db.records_failed_during_load

    return (db, nfailed)

'''def create_game_id(cur, db='SPORTSDB_DEV_DEV'):
    cur.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_games' and TABLE_SCHEMA='%s'" %db)
    count = cur.fetchone()
    game_id =  count[0]+1
    gid = "EV"+str(id)
    return game_id, gid'''


def get_tournament_id(tou_name, game, cur, outfile):
    cur.execute('SELECT id FROM sports_tournaments WHERE game = \"%s\" and title=\"%s\"' %(game, tou_name))
    data = cur.fetchone()
    if data:
        tid = data[0]
    else:
        print "New Tournament Found: %s" %tou_name
        outfile.write("New Tournament Found: %s" %tou_name)
        tid = data
    return tid

def get_participant_id(id, cur, outfile):
    print "id", id
    PARTICIPANT_ID = 'select entity_id from sports_source_keys where entity_type="participant" and source_key="%s" and source="daviscup_tennis"'
    pid = cur.execute(PARTICIPANT_ID% (id))
    pid = cur.fetchone()
    try:
        p_id = pid[0]
    except:
        import pdb;pdb.set_trace()
        outfile.write("New participant found:%s\n" % id)
        print "New  participant found: %s" % id
    return p_id

def update_game_locations(cur, loc):

    std = city = state = country = continent = street = latlong = zipcode = loc_id = ''
    if isinstance(loc, dict):
        std = loc.get('stadium', '')
        city = loc.get('city', '')
        state = loc.get('state', '')
        country = loc.get('country', '')
        continent = loc.get('continent', '')
        street = loc.get('street', '')
        latlong = loc.get('latlong', '')
        zipcode = loc.get('zipcode', '')
    if state or city or country:
        query = 'select id from sports_locations where state=%s and city=%s and country=%s'
        values = (state, city, country)
        cur.execute(query, values)
        loc_id = cur.fetchone()
        if not loc_id:
            cur.execute(LOC_INSERTION, (continent, country, state, city, street, zipcode, latlong))
            query = 'select id from sports_locations where continent=%s and country=%s and state=%s and city=%s and street=%s and zipcode=%s and latlong=%s'
            values = (continent, country, state, city, street, zipcode, latlong)
            cur.execute(query, values)
            loc_id = cur.fetchone()[0]
        else:
            loc_id = loc_id[0]
    std_id = ''
    if std:
        cur.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_stadiums' and TABLE_SCHEMA='SPORTSDB_DEV'")
        count = cur.fetchone()
        stadium_gid = 'STAD%s' %str(count[0])

    if std:
        query = "select id, location_id from sports_stadiums where title=%s"
        cur.execute(query, (std))
        data = cur.fetchone()
        if not data:
            cur.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_stadiums' and TABLE_SCHEMA='SPORTSDB_DEV'")
            count = cur.fetchone()
            stadium_gid = 'STAD%s' %str(count[0])
            cur.execute(STD_INSERTION, (stadium_gid, std, loc_id, loc_id))
            query = "select id from sports_stadiums where title=%s and location_id=%s"
            values = (std, loc_id)
            cur.execute(query, values)
            std_id = cur.fetchone()
            if std_id:
                std_id = std_id[0]
        else:
            std_id, loc_id = data

    return loc_id, std_id

 
def get_event_id(event_name, cur, outfile):

    cur.execute('SELECT id FROM sports_tournaments WHERE title=\"%s\"' %(event_name))
    data = cur.fetchone()
    if data:
        event_id = data[0]
    else:
        print "New Tournament Found: %s" %event_name
        outfile.write("New Tournament Found: %s" %event_name)
        event_id = data
    return event_id


def create_new_record(cur, record, id, game_id, outfile):
    
    sk = record.get('game_sk', '')
    status = record.get('status', '')
    game_dt = record.get('game_datetime', '')
    location_info = record.get('loc', {})
    tou_name = record.get('tou_name', '')
    game = 'tennis'
    ref = record.get('reference_url', '')
    event = record.get('event_name', '')
    home_scores = record.get('home_final', '')
    away_scores = record.get('away_final', '')
    final_score = record.get('final_score', '')
    winner = record.get('winner', '')
    hm_tm = record.get('home_team_id', '')
    aw_tm = record.get('away_team_id', '')
    source = record.get('source', '')

    if hm_tm:
        hm_pid = get_participant_id(hm_tm, cur, outfile)
    if aw_tm:
        aw_pid = get_participant_id(aw_tm, cur, outfile)

    if hm_pid and aw_pid:
        cur.execute(GAME_PTS, (id, hm_pid, 0, 0, hm_pid))
        cur.execute(GAME_PTS, (id, aw_pid, 1, 0, aw_pid))

    tid = get_tournament_id(tou_name, game, cur, outfile)
    if hm_pid and aw_pid and tid:
        cur.execute(TOU_PTS, (hm_pid, tid))
        cur.execute(TOU_PTS, (aw_pid, tid))
    event_id = get_event_id(event, cur, outfile)
    loc_id, std_id = update_game_locations(cur, location_info)

    game_values = (id, game_id, game_dt, game, '', tid, status, '', '', '', ref, event_id, loc_id, std_id)
    
    
    cur.execute(INSERT_SKS, (id, 'game', source, sk))
    cur.execute(GAME_INSERTION, game_values)
    return id

def create_new_player_record(cur, record, pl_gameid, pl_gid, outfile):

    sk = record.get('game_sk', '')
    status = record.get('status', '')
    game_dt = record.get('pl_game_datetime', '')
    location_info = record.get('loc', {})
    tou_name = record.get('tou_name', '')
    game = 'tennis'
    ref = record.get('reference_url', '')
    event = record.get('event_name', '')
    home_scores = record.get('home_final', '')
    away_scores = record.get('away_final', '')
    final_score = record.get('final_score', '')
    winner = record.get('winner', '')
    hm_tm = record.get('home_team_id', '')
    aw_tm = record.get('away_team_id', '')
    source = record.get('source', '')
    pl_game_sk = record.get('pl_game_sk', '')
    pl_game_status = record.get('pl_status', '')
    
    pl_winner = record.get('pl_winner', [])
    pl_ref = record.get('reference_url', '')
    aw_pl_score  = record.get('away_pl_score',[])
    aw_pl_id    = record.get('away_player_id', [])
    hm_pl_score = record.get('home_pl_score',[])
    hm_pl_id  = record.get('home_player_id', [])
    source = record.get('source', '') 
    game_id = cur.execute(SK_CHECK_QUERY, ('game', sk, source))
    game_id =cur.fetchone()
    if game_id:
        game_id = game_id[0]
        query = 'insert into sports_games_results (game_id, participant_id, result_type, result_value, created_at, modified_at) values (%s, %s, "%s", %s, now(), now()) ON DUPLICATE KEY UPDATE result_value=%s' % (pl_gameid, 0, 'super_game', game_id, game_id)
        cur.execute(query)
    if winner: 
        winner  = get_participant_id(winner, cur, outfile)
    
    if len(hm_pl_id) == 2 or len(aw_pl_id) == 2:
        game_note = "Men's Doubles"
        d_player_id1 = hm_pl_id[0]
        d_player_id3 = hm_pl_id[1]

        d_player_id2 = aw_pl_id[0]
        d_player_id4 = aw_pl_id[1]
        cond = d_player_id1 and d_player_id2 and d_player_id3 and d_player_id4
        is_double = True
        is_single = False
        _player_id1 = get_participant_id(d_player_id1, cur, outfile)
        _player_id2 = get_participant_id(d_player_id2, cur, outfile)
        _player_id3 = get_participant_id(d_player_id3, cur, outfile)
        _player_id4 = get_participant_id(d_player_id4, cur, outfile)
        cur.execute(GAME_PTS, (pl_gameid, _player_id1, 0, 1, _player_id1))
        cur.execute(GAME_PTS, (pl_gameid, _player_id2, 0, 1, _player_id2))
        cur.execute(GAME_PTS, (pl_gameid, _player_id3, 0, 2, _player_id3))
        cur.execute(GAME_PTS, (pl_gameid, _player_id4, 0, 2, _player_id4))


    else:
        game_note = "Men's Singles"
        single_player_id1 = hm_pl_id[0]
        single_player_id2 = aw_pl_id[0]
        cond = single_player_id1 and single_player_id2
        is_single = True
        is_double = False
        s_player_id1 = get_participant_id(single_player_id1, cur, outfile)
        s_player_id2 = get_participant_id(single_player_id2, cur, outfile)
        cur.execute(GAME_PTS, (pl_gameid, s_player_id1, 0, 0, s_player_id1))
        cur.execute(GAME_PTS, (pl_gameid, s_player_id2, 0, 0, s_player_id2))

    tid = get_tournament_id(tou_name, game, cur, outfile)
    event_id = get_event_id(event, cur, outfile)
    loc_id, std_id = update_game_locations(cur, location_info)

    game_values = (pl_gameid, pl_gid, game_dt, game, '', tid, status, '', '', '', ref, event_id, loc_id, std_id)

    cur.execute(INSERT_SKS, (pl_gameid, 'game', source, pl_game_sk))
    cur.execute(GAME_INSERTION, game_values)

def get_result_type(k):
    rt = k.replace('home_', '').replace('away_', '').strip()
    return rt

def update_scores(home_scores, away_scores,  final_score, winner, hm_pid, aw_pid, game_id, status, outfile, cur):
    query = "INSERT INTO sports_games_results (game_id, participant_id, result_type, result_value, created_at)\
    VALUES (%s, %s, %s, %s, now()) ON DUPLICATE KEY UPDATE result_value=%s"
    if home_scores:
        values = (game_id, hm_pid, 'final', home_scores ,home_scores)
        cur.execute(query, values)
    if away_scores:
        values = (game_id, aw_pid, 'final', away_scores , away_scores)
        cur.execute(query, values)
    if final_score:
        values = (game_id, '0', 'score', final_score , final_score)
        cur.execute(query, values)
    if winner:
        values = (game_id, winner, 'winner', winner, winner)
        cur.execute(query, values)

def update_player_scores(game_id, pl_game_id, s_player_id1, s_player_id2, _player_id1, _player_id2, _player_id3, _player_id4, game_note, pl_game_status, hm_pl_score, aw_pl_score, s_pl_winner, pl_winner_1, pl_winner_2, outfile, cur):
    
    query = "INSERT INTO sports_games_results (game_id, participant_id, result_type, result_value, created_at)\
    VALUES (%s, %s, %s, %s, now()) ON DUPLICATE KEY UPDATE result_value=%s"
    
    final_away_score = []
    final_home_score = []
    full_scores = [] 
    aw_pl_score1 = [tie_brk_pt.split('(')[0].replace(')', '') for tie_brk_pt in aw_pl_score if "("  in tie_brk_pt]
    aw_pl_score2 = [tie_brk_pt for tie_brk_pt in aw_pl_score if "("  not in tie_brk_pt]
    for i in aw_pl_score2:
        aw_pl_score1.append(i)

    hm_pl_score1 = [tie_brk_pt.split('(')[0].replace(')', '') for tie_brk_pt in hm_pl_score if "("  in tie_brk_pt]
    hm_pl_score2 = [tie_brk_pt for tie_brk_pt in hm_pl_score if "("  not in tie_brk_pt]
    for i in hm_pl_score2:
        hm_pl_score1.append(i)
    final_home_score.append(hm_pl_score1)
    final_away_score.append(aw_pl_score1)

    full_scores.append(final_home_score)
    full_scores.append(final_away_score)

    zip_data = zip(full_scores[0], full_scores[1])
    
    ps1 = ps2 = 0
    
    for i in zip_data:
        if isinstance(i[0], list):
            if len(i[0])  == len(i[1]) == 5:
                _count  = [0, 1, 2, 3, 4]

            elif len(i[0])  == len(i[1]) == 4:
                _count  = [0, 1, 2, 3]
            elif len(i[0])  == len(i[1]) == 3:
                _count = [0,1,2]
            elif len(i[0])  == len(i[1]) == 2:
                _count = [0,1]
            elif len(i[0])  == len(i[1]) == 1:
                _count = [0]
            if i[0] and i[1]:
                    for _ct in _count:
                        #if  pl_game_status in ["completed", "retired", "walkover"]:
                        if  pl_game_status in ["ongoing"]:
                            if int(i[0][_ct]) > 5 or int(i[1][_ct]) > 5:
                                if int(i[0][_ct]) > int(i[1][_ct]): ps1 = ps1+1
                            else: ps2 = ps2+1
                        else:
                            if int(i[0][_ct]) > int(i[1][_ct]): ps1 = ps1+1
                            else: ps2 = ps2+1
        else:
            if len(i[0])  == len(i[1]) == 5:
                _count  = [0, 1, 2, 3, 5]

            elif len(i[0])  == len(i[1]) == 4:
                _count  = [0, 1, 2, 3]
            elif len(i[0])  == len(i[1]) == 3:
                _count = [0,1,2]
            elif len(i[0])  == len(i[1]) == 2:
                _count = [0,1]
            elif len(i[0])  == len(i[1]) == 1:
                _count = [0]

            if i[0] and i[1]:
                if _count:
                    for _ct in _count:
                        #if  pl_game_status in ["completed", "retired", "walkover"]:
                        if  pl_game_status in ["ongoing"]:
                            if int(i[0][_ct]) > 5 or int(i[1][_ct]) > 5:
                                if int(i[0][_ct]) > int(i[1][_ct]): ps1 = ps1+1
                            else: ps2 = ps2+1
                        else:
                            if int(i[0][_ct]) > int(i[1][_ct]): ps1 = ps1+1
                            elif int(i[0][_ct]) < int(i[1][_ct]): ps2 = ps2+1

        final = str(ps1)+' - '+str(ps2)
        print "final", final
        if final:
            values = (pl_game_id, 0, 'score', final, final)
            cur.execute(query, values)
        if game_note == "Men's Singles" and final:
            values = (pl_game_id, s_player_id1, 'final', str(ps1), str(ps1))
            cur.execute(query, values)
            values = (pl_game_id, s_player_id2, 'final', str(ps2), str(ps2))
            cur.execute(query, values)
        elif game_note == "Men's Doubles" and final:
            values = (pl_game_id, _player_id1, 'final', str(ps1), str(ps1))
            cur.execute(query, values)
            values = (pl_game_id, _player_id2, 'final', str(ps2), str(ps2))
            cur.execute(query, values)
            values = (pl_game_id, _player_id3, 'final', str(ps1), str(ps1))
            cur.execute(query, values)
            values = (pl_game_id, _player_id4, 'final', str(ps2), str(ps2))
            cur.execute(query, values)



    if game_note == "Men's Singles":
        if hm_pl_score :
            count = 0
            tie = 0 
            tie_brk_hm_pl_score = [tie_brk_pt.split('(')[1].replace(')', '') for tie_brk_pt in hm_pl_score if "(" in tie_brk_pt]
            print "len", len(tie_brk_hm_pl_score)
            for tie_brk_pt in tie_brk_hm_pl_score:
                tie += 1
                values = (pl_game_id, s_player_id1, 'T%s'%tie, tie_brk_pt, tie_brk_pt)
                cur.execute(query, values)
            for i in hm_pl_score:
                count += 1
                if "(" in i:
                    i = i.split('(')
                    set_pt = i[0]
                    values = (pl_game_id, s_player_id1, 'S%s'%count, set_pt, set_pt)
                    cur.execute(query, values)
                else:
                    values = (pl_game_id, s_player_id1, 'S%s'%count, i, i)
                    cur.execute(query, values)

        if aw_pl_score:
            count = 0
            tie = 0
            tie_brk_aw_pl_score = [tie_brk_pt.split('(')[1].replace(')', '') for tie_brk_pt in aw_pl_score if "(" in tie_brk_pt]
            print "len", len(tie_brk_aw_pl_score)
            for tie_brk_pt in tie_brk_aw_pl_score:
                tie += 1
                values = (pl_game_id, s_player_id2, 'T%s'%tie, tie_brk_pt, tie_brk_pt)
                cur.execute(query, values)
            for i in aw_pl_score:
                count += 1
                if "(" in i:
                    i = i.split('(')
                    set_pt = i[0]
                    values = (pl_game_id, s_player_id2, 'S%s'%count, set_pt, set_pt)
                    cur.execute(query, values)
                else:
                    values = (pl_game_id, s_player_id2, 'S%s'%count, i, i)
                    cur.execute(query, values)
            
        if s_pl_winner:
            values = (pl_game_id, s_pl_winner, 'winner', s_pl_winner, s_pl_winner)
            cur.execute(query, values)

    else:
        if hm_pl_score:
            count = 0
            tie = 0 
            tie_brk_hm_pl_score = [tie_brk_pt.split('(')[1].replace(')', '') for tie_brk_pt in hm_pl_score if "(" in tie_brk_pt]
            print "len", len(tie_brk_hm_pl_score)
            for tie_brk_pt in tie_brk_hm_pl_score:
                tie += 1
                if _player_id1:
                    values = (pl_game_id, _player_id1, 'T%s'%tie, tie_brk_pt, tie_brk_pt)
                    cur.execute(query, values)
                if _player_id1:
                    values = (pl_game_id, _player_id2, 'T%s'%tie, tie_brk_pt, tie_brk_pt)
                    cur.execute(query, values)
            for i in hm_pl_score:
                count += 1
                if "(" in i:
                    i = i.split('(')
                    set_pt = i[0]
                    if _player_id1:
                        values = (pl_game_id, _player_id1, 'S%s'%count, set_pt, set_pt)
                        cur.execute(query, values)
                    if _player_id2:
                        values = (pl_game_id, _player_id2, 'S%s'%count, set_pt, set_pt)
                        cur.execute(query, values)

                else:
                    if _player_id1:
                        values = (pl_game_id, _player_id1, 'S%s'%count, i, i)
                        cur.execute(query, values)
                    if _player_id2:
                        values = (pl_game_id, _player_id2, 'S%s'%count, i, i)
                        cur.execute(query, values)

        if aw_pl_score:
            count = 0
            tie = 0 
            tie_brk_aw_pl_score = [tie_brk_pt.split('(')[1].replace(')', '') for tie_brk_pt in aw_pl_score if "(" in tie_brk_pt]
            print "len", len(tie_brk_aw_pl_score)
            for tie_brk_pt in tie_brk_aw_pl_score:
                tie += 1
                if _player_id3:
                    values = (pl_game_id, _player_id3, 'T%s'%tie, tie_brk_pt, tie_brk_pt)
                    cur.execute(query, values)
                if _player_id4:
                    values = (pl_game_id, _player_id4, 'T%s'%tie, tie_brk_pt, tie_brk_pt)
                    cur.execute(query, values)
            for i in aw_pl_score:
                count += 1
                if "(" in i:
                    i = i.split('(')
                    set_pt = i[0]
                    if _player_id3:
                        values = (pl_game_id, _player_id3, 'S%s'%count, set_pt, set_pt)
                        cur.execute(query, values)
                    if _player_id3:
                        values = (pl_game_id, _player_id3, 'S%s'%count, set_pt, set_pt)
                        cur.execute(query, values)

                else:
                    if _player_id3:
                        values = (pl_game_id, _player_id3, 'S%s'%count, i, i)
                        cur.execute(query, values)
                    if _player_id4:
                        values = (pl_game_id, _player_id4,'S%s'%count, i, i)
                        cur.execute(query, values)
        if pl_winner_1:
            values = (pl_game_id, pl_winner_1, 'winner', pl_winner_1, pl_winner_1)
            cur.execute(query, values)
        if pl_winner_2:
            values = (pl_game_id, pl_winner_2, 'winner', pl_winner_2, pl_winner_2)
            cur.execute(query, values)

def update_db(cur, record, game_id, outfile):
    
    sk = record.get('game_sk', '')
    status = record.get('status', '')
    game_dt = record.get('game_datetime', '')
    location_info = record.get('loc', {})
    tou_name = record.get('tou_name', '')
    game = 'tennis'
    event = record.get('event_name', '')
    home_scores = record.get('home_final', '')
    ref = record.get('reference_url', '')
    away_scores = record.get('away_final', '')
    final_score = record.get('final_score', '')
    winner = record.get('winner', '')
    source = record.get('source', '')
    hm_tm = record.get('home_team_id', '')
    aw_tm = record.get('away_team_id', '')

    if hm_tm:
        hm_pid = get_participant_id(hm_tm, cur, outfile)
    if aw_tm:
        aw_pid = get_participant_id(aw_tm, cur, outfile)

    if hm_pid and aw_pid:
        cur.execute(GAME_PTS, (game_id, hm_pid, 0, 0, hm_pid))
        cur.execute(GAME_PTS, (game_id, aw_pid, 1, 0, aw_pid))
    tid = get_tournament_id(tou_name, game, cur, outfile)
    event_id = get_event_id(event, cur, outfile)
    loc_id, std_id = update_game_locations(cur, location_info)
    values = (status, game_dt, tid,  ref, loc_id, std_id, game_id)
    cur.execute(UPDATE_STATUS, values)
    print "updated exist game record: %s" %game_id
    
    if status in ["completed", "retired", "walkover"]:
        if home_scores and away_scores and winner:
            winner  = get_participant_id(winner, cur, outfile)
            update_scores(home_scores, away_scores,  final_score, winner, hm_pid, aw_pid, game_id, status, outfile, cur)
        else:
            outfile.write("scores missed for one of the team or for two teams\n")

def update_player_record(cur, record, pl_game_id, outfile):
    
    location_info = record.get('loc', {})
    tou_name = record.get('tou_name', '')
    game = 'tennis'
    game_dt = record.get('pl_game_datetime', '')
    event = record.get('event_name', '')
    pl_game_sk = record.get('game_sk', '')
    pl_game_status = record.get('pl_status', '')
    pl_winner = record.get('pl_winner', [])
    pl_ref = record.get('reference_url', '')
    aw_pl_score  = record.get('away_pl_score',[])
    aw_pl_id    = record.get('away_player_id', [])
    hm_pl_score = record.get('home_pl_score',[])
    hm_pl_id  = record.get('home_player_id', [])
    ref = record.get('reference_url', '')
    sk = record.get('game_sk', '')
    print "sk>>", sk
    source = record.get('source', '') 
    
    if sk: 
        game_id = cur.execute(SK_CHECK_QUERY, ('game', sk, source))
        game_id =cur.fetchone()
        if game_id:
            game_id = game_id[0]
            query = 'insert into sports_games_results (game_id, participant_id, result_type, result_value, created_at, modified_at) values (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE result_value=%s'
            values = (pl_game_id, '0', 'super_game', game_id, game_id)
            cur.execute(query, values)
 
    else:
        outfile.write('no sk found for the record')
        print "no sk found for the record"
    tid = get_tournament_id(tou_name, game, cur, outfile)
    event_id = get_event_id(event, cur, outfile)
    loc_id, std_id = update_game_locations(cur, location_info)
    values = (pl_game_status, game_dt, tid,  ref, loc_id, std_id, pl_game_id)
    cur.execute(UPDATE_STATUS, values)
    print "updated exist game record: %s" %game_id
 
    if len(hm_pl_id) == 2 and len(aw_pl_id) == 2 and len(pl_winner) == 2 and pl_game_status != "scheduled":
        
        game_note = "Men's Doubles"
        d_player_id1 = hm_pl_id[0]
        d_player_id3 = hm_pl_id[1]

        d_player_id2 = aw_pl_id[0]
        d_player_id4 = aw_pl_id[1]
        cond = d_player_id1 and d_player_id2 and d_player_id3 and d_player_id4
        is_double = True
        is_single = False
        _player_id1 = get_participant_id(d_player_id1, cur, outfile)
        _player_id2 = get_participant_id(d_player_id2, cur, outfile)
        _player_id3 = get_participant_id(d_player_id3, cur, outfile)
        _player_id4 = get_participant_id(d_player_id4, cur, outfile)
        s_player_id1= 0
        s_player_id2= 0
        cur.execute(GAME_PTS, (pl_game_id, _player_id1, 0, 1, _player_id1))
        cur.execute(GAME_PTS, (pl_game_id, _player_id2, 0, 1, _player_id2))
        cur.execute(GAME_PTS, (pl_game_id, _player_id3, 0, 2, _player_id3))
        cur.execute(GAME_PTS, (pl_game_id, _player_id4, 0, 2, _player_id4))
        cur.execute(TOU_PTS, (_player_id1, 290))
        cur.execute(TOU_PTS, (_player_id2, 290))
        cur.execute(TOU_PTS, (_player_id3, 290))
        cur.execute(TOU_PTS, (_player_id4, 290)) 
        pl_winner_1 = pl_winner[0]
        pl_winner_2 = pl_winner[1]
        if pl_winner_1 and pl_winner_2:
            pl_winner_1 = get_participant_id(pl_winner_1, cur, outfile)
            pl_winner_2 = get_participant_id(pl_winner_2, cur, outfile)
        s_pl_winner = 0

    elif len(hm_pl_id) == 1 and len(aw_pl_id) == 1 and len(pl_winner) == 1 and pl_game_status != "scheduled":
        game_note = "Men's Singles"
        single_player_id1 = hm_pl_id[0]
        single_player_id2 = aw_pl_id[0]
        cond = single_player_id1 and single_player_id2
        is_single = True
        is_double = False
        s_player_id1 = get_participant_id(single_player_id1, cur, outfile)
        s_player_id2 = get_participant_id(single_player_id2, cur, outfile)
        _player_id1 = 0
        _player_id2 = 0
        _player_id3 = 0
        _player_id4 = 0
        pl_winner_1 = 0
        pl_winner_2 = 0
        cur.execute(GAME_PTS, (pl_game_id, s_player_id1, 0, 0, s_player_id1))
        cur.execute(GAME_PTS, (pl_game_id, s_player_id2, 0, 0, s_player_id2))
        cur.execute(TOU_PTS, (s_player_id1, 290))
        cur.execute(TOU_PTS, (s_player_id2, 290))
        if pl_winner:
            s_pl_winner = pl_winner[0]
            s_pl_winner = get_participant_id(s_pl_winner, cur, outfile)


    tid = get_tournament_id(tou_name, game, cur, outfile)
    event_id = get_event_id(event, cur, outfile)
    loc_id, std_id = update_game_locations(cur, location_info)
    values = (pl_game_status, game_dt, tid,  ref, loc_id, std_id, pl_game_id)
    cur.execute(UPDATE_STATUS, values)
    print "updated exist game record: %s" %game_id
    
    if pl_game_status in ["completed", "retired", "walkover"]:
        if hm_pl_score and aw_pl_score:
            update_player_scores(game_id, pl_game_id, s_player_id1, s_player_id2, _player_id1, _player_id2, _player_id3, _player_id4, game_note, pl_game_status, hm_pl_score, aw_pl_score, s_pl_winner, pl_winner_1, pl_winner_2, outfile, cur)
        else:
            outfile.write("scores missed for one of the team or for two teams\n")

def process(options):
    cur = MySQLdb.connect(host = '10.4.15.132', user = 'root', db= 'SPORTSDB_DEV').cursor()
    #cur = MySQLdb.connect(host = '10.4.18.183', user = 'root', db= 'SPORTSDB').cursor()
    global outfile
    outfile = open('output_file', 'ab+')
    if not options.infile :
        print "Need --infile, --outfile options!!"
        sys.exit(1)

    records = open(options.infile, 'r').readlines()

    for rec in records:
        record = eval(rec)
        sk = record.get('game_sk', '')
        print "sk>>", sk
        source = record.get('source', '')
        if not sk:
            outfile.write('no sk found for the record')
            print "no sk found for the record"
            continue

        game_id = cur.execute(SK_CHECK_QUERY, ('game', sk, source))
        game_id =cur.fetchone()
        if game_id:
            game_id = game_id[0]

        if game_id:
            print "games exist: %s" %game_id
            update_db(cur, record, game_id, outfile)
        else:
            cur.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_games' and TABLE_SCHEMA='SPORTSDB_DEV'")
            count = cur.fetchone()
            print "count", count
            id =  str(count[0]+1)
            game_id = "EV"+str(id)
            create_new_record(cur, record, id, game_id, outfile)
    for rec in records:
        record = eval(rec)
        source = record.get('source', '')
        pl_game_sk = record.get('pl_game_sk', '')
        print "pl_game_sk",pl_game_sk
        if not pl_game_sk:
            outfile.write('no sk found for the record')
            print "no sk found for the record"
            continue
        pl_game_id = cur.execute(SK_CHECK_QUERY, ('game', pl_game_sk, source))

        pl_game_id =cur.fetchone()
        if pl_game_id:
            pl_game_id = pl_game_id[0]
            print "player games exist: %s" %pl_game_id
            update_player_record(cur, record, pl_game_id, outfile)
        else:
            cur.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_games' and TABLE_SCHEMA='SPORTSDB_DEV'")
            count = cur.fetchone()
            print "count", count
            pl_gameid =  str(count[0]+1)
            pl_gid = "EV"+str(pl_gameid)
            create_new_player_record(cur, record, pl_gameid, pl_gid, outfile)

       
if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-i', '--input-dir', default=None, help='input directory which have svdb data files')
    parser.add_option('-a', '--address', default='', metavar='IP:PORT', help='address of dbserver')
    parser.add_option('-d', '--debug', metavar='BOOL', help='enable debugging', action='store_true')
    parser.add_option('', '--logfile', metavar='FILE', default=None)
    parser.add_option('-c', '--infile', metavar='FILE', default=None)
    (options, args) = parser.parse_args()
    process(options)

