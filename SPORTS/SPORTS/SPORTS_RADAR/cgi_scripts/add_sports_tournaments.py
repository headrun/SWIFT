#!/usr/bin/python

import cgitb; cgitb.enable()
import cgi
import MySQLdb


def handle_events(tou_name, event_name, sport_id, status, event, val, cursor, tou_fields):
    tou_id = get_id(tou_name, sport_id, cursor)
    event_id = get_id(event_name, sport_id, cursor)

    if tou_id and event_id:
        query = "insert into sports_tournaments_events (tournament_id, event_id, sequence_num, status, created_at, modified_at) values (%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()"
        values = (tou_id, event_id, '0',status)
        cursor.execute(query, values)


        query = "select * from sports_tournaments_events where tournament_id=%s and event_id=%s"
        values = (tou_id, event_id)
        cursor.execute(query, values)
        tou_event_data = cursor.fetchall()
        tou_res = prepare_result_table(tou_event_data, tou_fields)
        return tou_res
    else:
        return "One of the tournament/event missed"


def prepare_result_table(data, table_fields):
    if len(data)==1:
        res = '<table border="1">'
        for i in range(len(table_fields)):
            if data[0][i]: v = data[0][i]
            else: v = '-'
            res += '<tr><td>%s</td><td>%s</td></tr>' %(table_fields[i], v)
        res += '</table>'
    elif len(data)>1:
        res = '<table border="1"><thead>'
        for f in table_fields:
            res += '<th>%s</th>'
        print '</thead>'
        for dt in data:
            res += '<tr>'
            for d in dt:
                res += '<td>%s</td>' %d
            res += '</tr>'
        res += '</table>'
    return res


def get_id(tou_name, sport_id, cursor):
    query = "select id from sports_tournaments where (sport_id=%s or sport_id = 'multi sport') and title=%s"
    values = (sport_id, tou_name,)
    cursor.execute(query, values)
    tou_id = cursor.fetchall()
    tou = ''
    if len(tou_id)>1:
        print "</br>Multiple records exist with title: %s</br>" %tou_name
        print
        return
    elif len(tou_id)==1:
        if tou_id: tou = tou_id[0][0]
    else:
        tou = ''
        print "</br>No tournament exist or record exist with other name in DB: %s</br>" %tou_name
        print
    if tou: return tou
    else: return None


def main(field_storage):

    fields = ['id', 'gid', 'title', 'type', 'aka', 'sport_id', 'affiliation', 'season_start', 'season_end', 'base_popularity', 'keywords', 'reference', 'scheduleurl', 'scores url', 'standings url', 'location_ids', 'stadium_ids', 'created_at', 'modified_at', 'image_link']
    tou_fields = ['id', 'tournament_id', 'event_id', 'sequence_num', 'status', 'created_at', 'modified_at']


    val = field_storage.getfirst('action', '')
    name = field_storage.getfirst('name', '')
    type = field_storage.getfirst('type', '')
    aka = field_storage.getfirst('aka', '')
    sport_id = field_storage.getfirst('sport_id', '')
    aff = field_storage.getfirst('aff', '')
    gender = field_storage.getfirst('gender', '')
    genre = field_storage.getfirst('genre', '')
    start = field_storage.getfirst('start_dt', '')
    end = field_storage.getfirst('end_dt', '')
    ref = field_storage.getfirst('ref', '')
    sched = field_storage.getfirst('sched', '')
    score = field_storage.getfirst('score', '')
    std = field_storage.getfirst('std', '')
    loc = field_storage.getfirst('loc', '')
    stadium = field_storage.getfirst('stadium', '')
    event = field_storage.getfirst('event', '')
    tou_name = field_storage.getfirst('tou_name', '')
    event_name = field_storage.getfirst('event_name', '')
    status = field_storage.getfirst('status', '')
    image = field_storage.getfirst('image', '')

    print "Content-Type: text/html"
    print

    print '<html>'
    print '<head><link rel="stylesheet" type="text/css" href="tou.css"></head>'
    print '<body>'
    print '<form><table>'
    print '<tr><td>Tournament:</td><td><input type="text", size=50, name="name", value="%s"></input><br/></td></tr>' %name
    print '<tr><td>Type:</td><td><input type="text", size=50, name="type", value="%s"></input><br/></td></tr>' %type
    print '<tr><td>Aka:</td><td><input type="text", size=50, name="aka", value="%s"></input><br/></td></tr>' %aka
    print '<tr><td>Aff:</td><td><input type="text", size=50, name="aff", value="%s"></input><br/></td></tr>' %aff
    print '<tr><td>Gender(male/female):</td><td><input type="text", size=50, name="gender", value="%s"></input><br/></td></tr>' %gender
    print '<tr><td>Genre:</td><td><input type="text", size=50, name="genre", value="%s"></input><br/></td></tr>' %genre
    print '<tr><td>Sport_id:</td><td><input type="text", size=50, name="sport_id", value="%s"></input><br/></td></tr>' %sport_id
    print '<tr><td>Start:</td><td><input type="text", size=50, name="start_dt", value="%s"></input><br/></td></tr>' %start
    print '<tr><td>End:</td><td><input type="text", size=50, name="end_dt", value="%s"></input><br/></td></tr>' %end
    print '<tr><td>Ref:</td><td><input type="text", size=50, name="ref", value="%s"></input><br/></td></tr>' %ref
    print '<tr><td>Schd.url:</td><td><input type="text", size=50, name="sched", value="%s"></input><br/></td></tr>' %sched
    print '<tr><td>Score.url:</td><td><input type="text", size=50, name="score", value="%s"></input><br/></td></tr>' %score
    print '<tr><td>Stdngs.url:</td><td><input type="text", size=50, name="std", value="%s"></input><br/></td></tr>' %std
    print '<tr><td>Loc:</td><td><input type="text", name="loc", size=50, value="%s"></input><br/></td></tr>' %loc
    print '<tr><td>Stadium:</td><td><input type="text", name="stadium", size=50, value="%s"></input><br/></td></tr>' %stadium
    print '<tr><td>Tournament Name(events table):</td><td><input type="text", size=50, name="tou_name", value="%s"></input><br/></td></tr>' %tou_name
    print '<tr><td>Event:</td><td><input type="text", name="event_name", size=50, value="%s"></input><br/></td></tr>' %event_name
    print '<tr><td>Status:</td><td><input type="text", name="status", size=50, value="%s"></input><br/></td></tr>' %status
    print '<tr><td>Image:</td><td><input type="text", name="image", size=50, value="%s"></input><br/></td></tr>' %image
    print '<tr><td></br><input type="submit", name="action" value="submit"><br/></td></tr>'
    print '</table></form>'

    if not name and val=='submit':
        print "<b>Please enter tournament details</b>"
        print '</body></html>'
        return

    con = MySQLdb.connect(host='10.28.218.81', user='root', db='SPORTSDB', charset='utf8', use_unicode=True)
    con.set_character_set('utf8')
    cursor = con.cursor()

    print name, type, sport_id, start, end
    print val
    if val=="submit":
        cond = name and type and sport_id and start and end and image
        print cond
        if cond:

            query = "select id from sports_tournaments where sport_id=%s and title=%s"
            values = (sport_id, name,)
            cursor.execute(query, values)
            db_name = cursor.fetchall()
            print db_name
            if not db_name:
                cursor.execute('select id, gid from sports_tournaments where id in (select max(id) from sports_tournaments)')
                tou_data = cursor.fetchall()
                max_id, max_gid = tou_data[0]

                next_id = max_id+1
                next_gid = 'TOU'+str(int(max_gid.replace('TOU', ''))+1)

                print next_id
                print next_gid

                keywords = ''
                base_popularity = 200
                aka = "###".join(aka.split('<>'))
                print gender
                query = "insert into sports_tournaments (id, gid, title, type, aka, sport_id, affiliation, gender, genre,season_start, season_end, base_popularity, keywords, reference_url, schedule_url, scores_url, standings_url, location_ids, stadium_ids, created_at, modified_at, image_link) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now(), %s)"

                values = (next_id, next_gid, name, type, aka, sport_id, aff, gender, genre,start, end, base_popularity, keywords, ref, sched, score, std, loc, stadium, image)
                cursor.execute(query, values)
            else:
                next_id = db_name[0][0]

            if tou_name and event_name:
                tou_event_res = handle_events(tou_name, event_name, sport_id, status, event, val, cursor, tou_fields)
            else:
                tou_event_res = "No event mappping"

            query = "select * from sports_tournaments where id = %s"
            values = (next_id,)
            cursor.execute(query, values)

            data = cursor.fetchall()

            res = prepare_result_table(data, fields)
            print res

            print tou_event_res

            print '</body></html>'
        else:
            print '</body></html>'
    else:
        print '</body></html>'

    cursor.close()

if __name__ == '__main__':
    field_storage = cgi.FieldStorage()
    main(field_storage)
