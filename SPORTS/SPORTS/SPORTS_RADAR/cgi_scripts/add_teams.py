#!/usr/bin/python

import cgitb; cgitb.enable()
import cgi
import MySQLdb


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
            res += '<th>%s</th>' %f
        res += '</thead>'
        for dt in data:
            res += '<tr>'
            for d in dt:
                res += '<td>%s</td>' %d
            res += '</tr>'
        res += '</table>'
    return res


def get_id(tou_name, sport_id, cursor):
    query = "select id from sports_tournaments where sport_id=%s and title=%s"
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

    teams_fields = ['id', 'participant_id', 'short_title', 'callsign', 'category', 'keywords', 'tournament_id', 'division', 'gender', 'formed', 'timezone', 'logo_url', 'vtap_logo_url', 'youtube_channel', 'stadium_id', 'created_at', 'modified_at', 'display_title']

    player_fields = ['id','participant_id','debut','main_role','roles','gender','age','height','weight','birth_date','birth_place','salary_pop','rating_pop','weight_class','marital_status','participant_since','competitor_since','created_at','modified_at']

    participants_fields = ['id','gid','title','aka','sport_id','participant_type','image_link','base_popularity','reference_url','location_id','created_at','modified_at']

    sk_fields = ['id', 'entity_id', 'entity_type', 'source', 'source_key', 'created_at', 'modified_at']
    pt_fields = ['participant_id', 'title', 'sport_id', 'participant_type', 'source', 'source_key']


    val = field_storage.getfirst('action', '')

    #Participant table details
    name = field_storage.getfirst('name', '')
    aka = field_storage.getfirst('aka', '')
    sport_id = field_storage.getfirst('sport_id', '')
    participant_type = field_storage.getfirst('participant_type', '')
    image = field_storage.getfirst('img', '')
    bp = field_storage.getfirst('bp', '0')
    ref = field_storage.getfirst('ref', '')
    loc = field_storage.getfirst('loc', '0')

    #Teams table details
    short_title = field_storage.getfirst('short', '')
    callsign = field_storage.getfirst('callsign', '')
    display_title = field_storage.getfirst('display', '')
    category = field_storage.getfirst('category', '')
    keywords = field_storage.getfirst('kws', '')
    tou_name = field_storage.getfirst('tou_name', '')
    division = field_storage.getfirst('division', '')
    gender = field_storage.getfirst('gender', '')
    formed = field_storage.getfirst('formed', '')
    tz = field_storage.getfirst('tz', '')
    logo_url = field_storage.getfirst('logo', '')
    vtap = field_storage.getfirst('vtap', '')
    yt = field_storage.getfirst('yt', '')
    std = field_storage.getfirst('std', '')
    src = field_storage.getfirst('src', '')
    sk = field_storage.getfirst('sk', '')

    print "Content-Type: text/html"
    print

    print '<html>'
    print '<head></head>'
    print '<center><h2><b>TEAMS ADDITION</b></h2></center><br/>'
    print '<body>'
    print '<p><b>PARTICIPANTS TABLE</b></p>'
    print '<form><table>'
    print '<tr><td>Participant Title:</td><td><input type="text", size=50, name="name", value="%s"></input><br/></td></tr>' %name
    print '<tr><td>Aka:</td><td><input type="text", size=50, name="aka", value="%s"></input><br/></td></tr>' %aka
    print '<tr><td>Sport_id:</td><td><input type="text", size=50, name="sport_id", value="%s"></input><br/></td></tr>' %sport_id
    print '<tr><td>Participant Type:</td><td><input type="text", size=50, name="participant_type", value="%s"></input><br/></td></tr>' %participant_type
    print '<tr><td>Image:</td><td><input type="text", size=50, name="img", value="%s"></input><br/></td></tr>' %image
    print '<tr><td>Base popularity:</td><td><input type="text", size=50, name="bp", value="%s"></input><br/></td></tr>' %bp
    print '<tr><td>Reference:</td><td><input type="text", size=50, name="ref", value="%s"></input><br/></td></tr>' %ref
    print '<tr><td>Loc:</td><td><input type="text", name="loc", size=50, value="%s"></input><br/></td></tr>' %loc

    print '<tr><td><br/></td></tr>'
    print '<tr><td><b>TEAMS TABLE</b></td></tr>'

    print '<tr><td>Short Title:</td><td><input type="text", size=50, name="short", value="%s"></input><br/></td></tr>' %short_title
    print '<tr><td>Callsign:</td><td><input type="text", size=50, name="callsign", value="%s"></input><br/></td></tr>' %callsign
    print '<tr><td>Display Title:</td><td><input type="text", size=50, name="display", value="%s"></input><br/></td></tr>' %display_title
    print '<tr><td>Category:</td><td><input type="text", name="category", size=50, value="%s"></input><br/></td></tr>' %category
    print '<tr><td>Keywords:</td><td><input type="text", size=50, name="kws", value="%s"></input><br/></td></tr>' %keywords
    print '<tr><td>Tournament_name:</td><td><input type="text", name="tou_name", size=50, value="%s"></input><br/></td></tr>' %tou_name
    print '<tr><td>Division:</td><td><input type="text", name="division", size=50, value="%s"></input><br/></td></tr>' %division
    print '<tr><td>Gender(male/female):</td><td><input type="text", name="gender", size=50, value="%s"></input><br/></td></tr>' %gender
    print '<tr><td>Formed:</td><td><input type="text", name="formed", size=50, value="%s"></input><br/></td></tr>' %formed
    print '<tr><td>TimeZone:</td><td><input type="text", name="tz", size=50, value="%s"></input><br/></td></tr>' %tz
    print '<tr><td>LogoUrl:</td><td><input type="text", name="logo", size=50, value="%s"></input><br/></td></tr>' %logo_url
    print '<tr><td>Vtap Logo Url:</td><td><input type="text", name="vtap", size=50, value="%s"></input><br/></td></tr>' %vtap
    print '<tr><td>Youtube Channel:</td><td><input type="text", name="yt", size=50, value="%s"></input><br/></td></tr>' %yt
    print '<tr><td>Stadium:</td><td><input type="text", name="std", size=50, value="%s"></input><br/></td></tr>' %std

    print '<tr><td><br/></td></tr>'
    print '<tr><td><b>SOURCEKEYS TABLE</b></td></tr>'
    print '<tr><td>SourceName:</td><td><input type="text", name="src", size=50, value="%s"></input><br/></td></tr>' %src
    print '<tr><td>SourceKey:</td><td><input type="text", name="sk", size=50, value="%s"></input><br/></td></tr>' %sk

    print '<tr><td><br/></td></tr>'
    #print '<button type="submit" name="action" value="confirm" >CONFIRM</button><br/><br/>'
    print '<tr><td></br><input type="submit", name="action" value="confirm"><br/></td></tr>'
    print '</table>'

    pt_cond = (not name or not sport_id or not participant_type)
    team_cond = (not category or not gender)
    sk_cond = (not src or not sk)

    if "player" in participant_type and val=='confirm':
        print '<script>alert("You have entered player type in teams details")</script>'
        print '</body></html>'
        return

    if pt_cond  and  val=='confirm':
        print "<br/></br/><b>Please enter full team details</b>"
        print "<b><br/>One of the team name or sport_id or participant_type missed</b>"
        print '<script>alert("Please check participant table data once.")</script>'
        print '</body></html>'
        return

    if team_cond and val=='confirm':
        print "<br/></br/><b>Please enter full team details</b>"
        print "<b><br/>One of the teamcategory  or gender missed</b>"
        print '<script>alert("Please check teams table data once.")</script>'
        print '</body></html>'
        return

    if sk_cond and val=='confirm':
        print "<br/></br/><b>Please enter full source key details</b>"
        print '<script>alert("Please check source_keys table data once.")</script>'
        print '</body></html>'
        return

#    con = MySQLdb.connect(host='10.28.218.81', user='root', db='SPORTSDB')
    con = MySQLdb.connect(host='10.28.218.81', user='root', db='SPORTSDB', charset='utf8', use_unicode=True)
    cursor = con.cursor()

    #Get tournament details
    query = "select id from sports_tournaments where sport_id=%s and title=%s"
    values = (sport_id, tou_name,)
    cursor.execute(query, values)
    tournament_name = cursor.fetchall()
    if tournament_name:
        tou_id = tournament_name[0][0]
    else:
        tou_id = 0

    if val=="confirm":

        query = "select A.id, A.title, A.sport_id, A.participant_type, B.source, B.source_key from sports_participants A,\
        sports_source_keys B where A.id=B.entity_id and B.source=%s and B.source_key=%s and B.entity_type='participant'"
        values= (src, sk)
        cursor.execute(query, values)
        db_data = cursor.fetchall()
        if not db_data:
            query = '''select A.id, A.title, A.sport_id, A.participant_type, B.source, B.source_key from sports_participants A,\
            sports_source_keys B where A.id=B.entity_id and B.entity_type="participant" and (A.title like "%%%s%%" or A.aka like "%%%s%%") and A.sport_id="%s" and B.source="%s"'''
            values = (name, name, sport_id, src)
            cursor.execute(query%values)
            db_data = cursor.fetchall()

        if db_data:
            print '<script>alert("DUPLICATE RECORDS FOUND. SEE BELOWE TABLE DATA")</script>'
            res = prepare_result_table(db_data, pt_fields)
            print '<br/>'
            print '<br/>'
            print res
            print '<br/>'
            print '<br/>'
        else:
            print '<script>alert("Please verify once before you submit data")</script>'
            print 'TEAM Name: %s\n' %name
            print '<br/>'
            print 'GAME: %s\n' %sport_id
            print '<br/>'
            print 'Participant type: %s\n' %participant_type
            print '<br/>'
            print 'tournament name: %s (%s)\n' %(tou_name, tou_id)
            print '<br/>'
            print '<br/>'

        print '<input type="submit" name="action" value="submit"><br/><br/>'

    if val == 'submit':
        cursor.execute('select id, gid from sports_participants where id in (select max(id) from sports_participants)')
        tou_data = cursor.fetchall()
        max_id, max_gid = tou_data[0]

        next_id = max_id+1
        next_gid = 'TEAM'+str(int(max_gid.replace('TEAM', '').replace('PL', ''))+1)

        print next_id
        print next_gid

        aka = "###".join(aka.split('<>'))



        query = "insert into sports_participants (id, gid, title, aka, sport_id, participant_type, image_link, base_popularity, reference_url, location_id, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"
        values = (next_id, next_gid, name, aka, sport_id, participant_type, image, bp, ref, loc)
        cursor.execute(query, values)


        query = "insert into sports_teams (participant_id, short_title, callsign, category, keywords, tournament_id, division, gender, formed, timezone, logo_url,vtap_logo_url,youtube_channel, stadium_id, created_at, modified_at, display_title) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now(), %s)"
        values = (next_id, short_title, callsign, category, keywords, tou_id, division, gender, formed, tz,vtap, yt, logo_url, std, display_title)
        cursor.execute(query, values)


        query = "insert into sports_source_keys (entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, %s, now(), now())"
        values = (next_id, 'participant', src, sk)
        cursor.execute(query, values)

        query = "select * from sports_participants where id = %s"
        values= (next_id,)
        cursor.execute(query, values)
        pt_data = cursor.fetchall()

        query = "select * from sports_teams where participant_id = %s"
        values = (next_id,)
        cursor.execute(query, values)
        team_data = cursor.fetchall()

        query = "select * from sports_source_keys where entity_id=%s and source=%s and source_key=%s and entity_type = %s"
        values = (next_id, src, sk, 'participant')
        cursor.execute(query, values)
        sk_data = cursor.fetchall()


        PT = prepare_result_table(pt_data, participants_fields)
        TM = prepare_result_table(team_data, teams_fields)
        SK = prepare_result_table(sk_data, sk_fields)

        print PT  #Display participants table data
        print TM  #Display teams table data
        print SK  #Dsiplay source keys table data

        print '</form></body></html>'

    cursor.close()

if __name__ == '__main__':
    field_storage = cgi.FieldStorage()
    main(field_storage)
