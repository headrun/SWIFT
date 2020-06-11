#!usr/bin/env python
import sys
import time
from urlparse import urljoin
import urllib2, httplib
import MySQLdb


def main():
    #con = MySQLdb.connect(host='10.4.15.132', user='root', db='SPORTSDB_DEV')
    con = MySQLdb.connect(host='10.4.18.183', user='root', db='SPORTSDB')
    con.set_character_set('utf8')
    cursor = con.cursor()
     
    my_file = open('PLAYERS_MEN_may_29', "r+").readlines()
    for line in my_file:
        record = eval(line)
        
        gender  = record.get('gender', '')
        name = record.get('player_name', '')
        src = "frenchopen_tennis"
        game = "tennis"
        sk = record.get('player_sk', '')
        
        sk_query = 'select entity_id from sports_source_keys where source = "frenchopen_tennis" and source_key="%s" and entity_type="participant"' % (sk)
        sk_ = cursor.execute(sk_query)
        sk_ = cursor.fetchall()
        if sk_:
            print "sk already exist"
        else:
            _query = 'select id from sports_participants where title like \"%%%s%%\" and game = "tennis" and participant_type = "player"' % name
            _data = cursor.execute(_query)
            _data = cursor.fetchall()
            if _data:
                pid = _data[0][0]
                _file = open('existing_players', 'ab+')
                _file.write("%s###%s###%s\n" %(sk, name, pid))
                query = "insert into sports_source_keys (entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, %s, now(), now())"
                values = (pid, 'participant', src, sk)
                cursor.execute(query, values)
            else:
                _file = open('new_players', 'ab+')
                _file.write("%s###%s###%s\n" %(sk, name, sk))
                '''cursor.execute('select id, gid from sports_participants where id in (select max(id) from sports_participants)')
                tou_data = cursor.fetchall()
                if tou_data:
                    max_id, max_gid = tou_data[0]
                    next_id = max_id+1
                    next_gid = 'PL'+str(int(max_gid.replace('TEAM', '').replace('PL', ''))+1)
                    print next_gid
                    print next_id

                    query = "insert into sports_participants (id, gid, title, aka, game, participant_type, image_link, base_popularity, reference_url, location_id, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"
                    values = (next_id, next_gid, name, '', 'tennis', 'player', image, '200', ref, '')
                    cursor.execute(query, values)
                    con.commit()

                    query = "insert into sports_players (participant_id, debut, main_role, roles, gender, age, height, weight, birth_date, birth_place, salary_pop, rating_pop, weight_class, marital_status, participant_since, competitor_since, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now());"
                    values = (next_id, '', role, '', gender, '', '', '', birth_date, '', '', '', '', '', '', '')
                    cursor.execute(query, values)
                    con.commit()

                    query = "insert into sports_source_keys (entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, %s, now(), now())"
                    values = (next_id, 'participant', src, sk)
                    cursor.execute(query, values)
                    con.commit()
                    query = "insert into sports_roster (team_id, player_id, player_number, player_role, status, season, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now())"
                    values = (team_id, next_id, '', role, 'active', '2014')
                    cursor.execute(query, values)
                    con.commit()'''

if __name__ == '__main__':
    main()

