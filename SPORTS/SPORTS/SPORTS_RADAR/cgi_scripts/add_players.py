#!/usr/bin/python

import cgitb; cgitb.enable()
import cgi
import MySQLdb


def prepare_result_table(data, table_fields):
    res = ''
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

    player_fields = ['id','participant_id','debut','main_role','roles','gender','age','height','weight','birth_date','birth_place','salary_pop','rating_pop','weight_class','marital_status','participant_since','competitor_since','created_at','modified_at', 'display_title', 'short_title']
    
    participants_fields = ['id','gid','title','aka','sport_id','participant_type','image_link','base_popularity','reference_url','location_id','created_at','modified_at']

    sk_fields = ['id', 'entity_id', 'entity_type', 'source', 'source_key', 'created_at', 'modified_at']
    pt_fields = ['participant_id', 'title', 'sport_id', 'participant_type', 'source', 'source_key']
    tou_pt_fields = ['tournament_id', 'participant_id', 'status', 'status_remarks', 'standing', 'seed', 'season', 'created_at', 'modified_at']


    
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

    #Player Table detail
    debut = field_storage.getfirst('debut', '')
    main_role = field_storage.getfirst('main_role', '')
    roles = field_storage.getfirst('roles', '')
    gender = field_storage.getfirst('gender', '')
    age = field_storage.getfirst('age', '')
    height = field_storage.getfirst('height', '')
    weight = field_storage.getfirst('weight', '')
    birth_date = field_storage.getfirst('birth_date', '')
    birth_place = field_storage.getfirst('birth_place', '')
    salary_pop = field_storage.getfirst('salary_pop', '')
    rating_pop = field_storage.getfirst('rating_pop', '')
    weight_class = field_storage.getfirst('weight_class', '')
    marital_status = field_storage.getfirst('marital_status', '')
    participant_since = field_storage.getfirst('participant_since', '')
    competitor_since = field_storage.getfirst('competitor_since', '')
    display_title = field_storage.getfirst('display_title','')
    short_title = field_storage.getfirst('short_title','')
 
    #Source key table details
    src = field_storage.getfirst('src', '')
    sk = field_storage.getfirst('sk', '')
    
    #Tournaments participants table details
    tournament = field_storage.getfirst('tou', '')
    season = field_storage.getfirst('season', '')
    status = field_storage.getfirst('status', '')
    status_remarks = field_storage.getfirst('st_emarks', '')
    standing = field_storage.getfirst('standing', '')
    seed = field_storage.getfirst('seed', '')


    print "Content-Type: text/html"
    print

    print '<html>'
    print '<head></head>'
    print '<center><h2><b>PLAYERS ADDITION</b></h2></center><br/>'
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
    print '<tr><td><b>PLAYERS TABLE</b></td></tr>'
    
    print '<tr><td>Debut:</td><td><input type="text", size=50, name="debut", value="%s"></input><br/></td></tr>' %debut
    print '<tr><td>Main Role:</td><td><input type="text", size=50, name="main_role", value="%s"></input><br/></td></tr>' %main_role
    print '<tr><td>Roles:</td><td><input type="text", name="roles", size=50, value="%s"></input><br/></td></tr>' %roles
    print '<tr><td>Gender(male/female):</td><td><input type="text", size=50, name="gender", value="%s"></input><br/></td></tr>' %gender
    print '<tr><td>Age:</td><td><input type="text", name="age", size=50, value="%s"></input><br/></td></tr>' %age
    print '<tr><td>Height:</td><td><input type="text", name="height", size=50, value="%s"></input><br/></td></tr>' %height
    print '<tr><td>Weight:</td><td><input type="text", name="weight", size=50, value="%s"></input><br/></td></tr>' %weight
    print '<tr><td>Birth Date:</td><td><input type="text", name="birth_date", size=50, value="%s"></input><br/></td></tr>' %birth_date
    print '<tr><td>Birth Place:</td><td><input type="text", name="birth_place", size=50, value="%s"></input><br/></td></tr>' %birth_place
    print '<tr><td>Salary Pop:</td><td><input type="text", name="salary_pop", size=50, value="%s"></input><br/></td></tr>' %salary_pop
    print '<tr><td>Rating Pop:</td><td><input type="text", name="rating_pop", size=50, value="%s"></input><br/></td></tr>' %rating_pop
    print '<tr><td>Weight Class:</td><td><input type="text", name="weight_class", size=50, value="%s"></input><br/></td></tr>' %weight_class
    print '<tr><td>Marital Status:</td><td><input type="text", name="marital_status", size=50, value="%s"></input><br/></td></tr>' %marital_status
    print '<tr><td>Participant Since</td><td><input type="text", name="participant_since", size=50, value="%s"></input><br/></td></tr>' %participant_since
    print '<tr><td>Competitor Since:</td><td><input type="text", name="competitor_since", size=50, value="%s"></input><br/></td></tr>' %competitor_since
    
    print '<tr><td>Display Title:</td><td><input type="text", name="rating_pop", size=50, value="%s"></input><br/></td></tr>' %display_title
    print '<tr><td>Short Title:</td><td><input type="text", name="rating_pop", size=50, value="%s"></input><br/></td></tr>' %short_title
    print '<tr><td><br/></td></tr>'
    print '<tr><td><b>SOURCEKEYS TABLE</b></td></tr>'
    print '<tr><td>SourceName:</td><td><input type="text", name="src", size=50, value="%s"></input><br/></td></tr>' %src
    print '<tr><td>SourceKey:</td><td><input type="text", name="sk", size=50, value="%s"></input><br/></td></tr>' %sk

    print '<tr><td><br/></td></tr>'
    print '<tr><td><b>TOURNAMENTS PARTICIPANTS TABLE</b></td></tr>'
    print '<tr><td>Tournament:</td><td><input type="text", name="tou", size=50, value="%s"></input><br/></td></tr>' %tournament
    print '<tr><td>Season:</td><td><input type="text", name="season", size=50, value="%s"></input><br/></td></tr>' %season
    print '<tr><td>Status:</td><td><input type="text", name="status", size=50, value="%s"></input><br/></td></tr>' %status
    print '<tr><td>Status Remarks:</td><td><input type="text", name="st_remarks", size=50, value="%s"></input><br/></td></tr>' %status_remarks
    print '<tr><td>Standing:</td><td><input type="text", name="standing", size=50, value="%s"></input><br/></td></tr>' %standing
    print '<tr><td>Seed::</td><td><input type="text", name="seed", size=50, value="%s"></input><br/></td></tr>' %seed
    print '<tr><td><br/></td></tr>'
    #print '<button type="submit" name="action" value="confirm" >CONFIRM</button><br/><br/>'
    print '<tr><td></br><input type="submit", name="action" value="confirm"><br/></td></tr>'
    print '</table>'

    pt_cond = (not name or not sport_id or not participant_type)
    player_cond = (not gender)
    sk_cond = (not src or not sk)
    
    if "team" in participant_type and val=='confirm':
        print '<script>alert("You have entered team type in Player details")</script>'
        print '</body></html>'
        return
        
    if pt_cond  and  val=='confirm':
        print "<br/></br/><b>Please enter full player details</b>"
        print "<b><br/>One of the player name or sport_id or participant_type missed</b>"
        print '<script>alert("Please check participant table data once.")</script>'
        print '</body></html>'
        return
    
    if player_cond and val=='confirm':
        print "<br/></br/><b>Please enter full Player details</b>"
        print "<b><br/>Gender is missed</b>"
        print '<script>alert("Please check players table data once.")</script>'
        print '</body></html>'
        return

    if sk_cond and val=='confirm':
        print "<br/></br/><b>Please enter full source key details</b>"
        print '<script>alert("Please check source_keys table data once.")</script>'
        print '</body></html>'
        return
        
    con = MySQLdb.connect(host='10.28.218.81', user='root', db='SPORTSDB', charset='utf8', use_unicode=True)
    cursor = con.cursor()
    
    if val=="confirm":
        query = """select A.id, A.title, A.sport_id, A.participant_type, B.source, B.source_key from sports_participants A,\
        sports_source_keys B where A.id=B.entity_id and B.source="%s" and B.source_key="%s" and\
        B.entity_type='participant' and A.participant_type='player'"""
        values= (src, sk)
        cursor.execute(query, values)
        db_data = cursor.fetchall()
        if not db_data:
            query = """select A.id, A.title, A.sport_id, A.participant_type, B.source, B.source_key from sports_participants A,\
            sports_source_keys B where A.id=B.entity_id and B.entity_type='participant' and\
            (A.title like "%%%s%%" or A.aka like "%%%s%%") and A.sport_id="%s" and B.source="%s" and A.participant_type='player'"""
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
            print '<br/>'
            
        print '<input type="submit" name="action" value="submit"><br/><br/>'
        
    if val == 'submit':
        cursor.execute('select id, gid from sports_participants where id in (select max(id) from sports_participants)')
        tou_data = cursor.fetchall()
        max_id, max_gid = tou_data[0]

        next_id = max_id+1
        next_gid = 'PL'+str(int(max_gid.replace('TEAM', '').replace('PL', ''))+1)


        aka = "###".join(aka.split('<>'))
                
        query = "insert into sports_participants (id, gid, title, aka, sport_id, participant_type, image_link, base_popularity, reference_url, location_id, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"
        values = (next_id, next_gid, name, aka, sport_id, participant_type, image, bp, ref, loc)
        cursor.execute(query, values)
       
            
        query = "insert into sports_players (participant_id, debut, main_role, roles, gender, age, height, weight, birth_date, birth_place, birth_place_id,salary_pop, rating_pop, weight_class, marital_status, participant_since, competitor_since, created_at, modified_at, display_title, short_title) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now(), %s, %s);"
        values = (next_id, debut, main_role, roles, gender, age, height, weight, birth_date, birth_place, '',salary_pop, rating_pop, weight_class, marital_status, participant_since, competitor_since, display_title, short_title)
        cursor.execute(query, values)
        

        query = "insert into sports_source_keys (entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, %s, now(), now())"
        values = (next_id, 'participant', src, sk)
        cursor.execute(query, values)
        
        query = "select id from sports_tournaments where sport_id = %s and title = %s"
        values = (sport_id, tournament)
        cursor.execute(query, values)
        tou_id = cursor.fetchone()
        if tou_id:
#        print '<script>alert("%s")</script>' 
            tou_id = tou_id[0]

            query = "insert into sports_tournaments_participants (tournament_id, participant_id, status, status_remarks, standing, seed, season, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now())"
            values = (tou_id, next_id, status, status_remarks, standing, seed, season)
            cursor.execute(query, values)

        query = "select * from sports_participants where id = %s"
        values= (next_id,)
        cursor.execute(query, values)
        pt_data = cursor.fetchall()

        query = "select * from sports_players where participant_id = %s"
        values = (next_id,)
        cursor.execute(query, values)
        team_data = cursor.fetchall() 

        query = "select * from sports_source_keys where entity_id=%s and source=%s and source_key=%s and entity_type = %s"
        values = (next_id, src, sk, 'participant')
        cursor.execute(query, values)
        sk_data = cursor.fetchall()
        if tou_id:
            query = "select * from sports_tournaments_participants where participant_id= %s and tournament_id = %s"
            values = (next_id, tou_id)
            cursor.execute(query, values)
            tou_pt_data = cursor.fetchall()
            TP = prepare_result_table(tou_pt_data, tou_pt_fields)

            print TP  #Diplay tournaments participants table data

        PT = prepare_result_table(pt_data, participants_fields)
        TM = prepare_result_table(team_data, player_fields)
        SK = prepare_result_table(sk_data, sk_fields)
        
        print PT  #Display participants table data
        print TM  #Display teams table data
        print SK  #Dsiplay source keys table data

        print '</form></body></html>'

    cursor.close()

if __name__ == '__main__':
    field_storage = cgi.FieldStorage()
    main(field_storage)
