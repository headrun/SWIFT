from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
import re
import datetime
import time
import MySQLdb
import urllib

TEAM_LIST = ('NE', 'BAL', 'CIN', 'CLE', 'CHI', 'DET',
             'GB', 'MIN', 'HOU', 'IND', 'JAC', 'TEN',
             'ATL', 'CAR', 'NO', 'TB', 'BUF', 'MIA',
             'DAL', 'NYG', 'PHI', 'NYJ', 'WAS', 'DEN',
             'KC', 'OAK', 'SD', 'ARI', 'SF', 'SEA', 'STL', 'PIT')

ROLES_DICT = {'C': 'Center', 'G': 'Guard', 'T': 'Tackle',
              'QB': 'Quarterback', 'RB': 'Running back',
              'WR': 'Wide receiver', 'TE': 'Tight end',
              'DT': 'Defensive tackle', 'DE': 'Defensive end',
              'MLB': 'Middle linebacker', 'OLB': 'Outside linebacker',
              'CB': 'Cornerback', 'S': 'Safety', 'K': 'Kicker',
              'H': 'Holder', 'LS': 'Long snapper',
              'P': 'Punter', 'PR': 'Punt returner', 'KR': 'Kick returner',
              'FB': 'Fullback', 'HB': 'Halfback', 'ILB': 'Inside Linebacker',
              'TB': 'Tailback', 'RG': 'Right Guard', 'LG': 'Left Guard',
              'RT': 'Right Tackle', 'LT': 'Left Tackle', 'NG': 'Nose Guard',
              'DL': 'Defensive line', 'FS': 'Free safety', 'LB': 'Linebacker',
              'NT': 'Nose Tackle', 'DB': 'Defensive back', 'PK': 'Placekicker',
              'SS': 'Strong safety', 'OG': 'Offensive guard',
              'OT': 'Offensive Tackle', 'OL': 'Offensive Line',
              'SAF': 'Safety'}


REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', \
                    'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
                    'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', \
                    'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
                    'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', \
                    'WV' : 'West Virgina', 'FL' : 'Florida', \
                    'KS' : 'Kansas', 'TN' : 'Tennessee', \
                    'LA' : 'Louisiana', 'MO' : 'Missouri', \
                    'AR' : 'Arkansas', 'SD' : 'South Dakota', \
                    'MS' : 'Mississippi', 'MI' : 'Michigan', \
                    'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', \
                    'ID' : 'Idaho', 'RI' : 'Rhode Island', \
                    'NM' : 'New Mexico', 'MN' : 'Minnesota', \
                    'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
                    'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', \
                    'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado' }

def update_reference_url(pl_id, pl_link, pl_image):
    conn, cursor = mysql_connection()
    values = (pl_image, pl_id)
    cursor.execute(REF_QUERY % values)
    conn.close()

def update_pl_location(pl_id, loc_id):
    conn, cursor = mysql_connection()
    values = (loc_id, pl_id)
    cursor.execute(LOC_QUERY % values)
    conn.close()

def mysql_connection():
    conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
    cursor = conn.cursor()
    return conn, cursor

REF_QUERY = 'update sports_participants set image_link = "%s" where id=%s and image_link like "%%http://static.nfl.com/static/content/public/static/img/getty/headshot/%%"'

LOC_QUERY = 'update sports_participants set location_id = %s where location_id = "" and id = %s and participant_type = "player" and game="football"'

PAR_QUERY = "insert into sports_participants (id, gid, title, aka, game, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at, short_title, display_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, now(), now(), %s, %s) on duplicate key update modified_at = now();"

MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'

def get_player_role(position):
    for key, value in ROLES_DICT.iteritems():
        if position == key:
            position = value
            return position

def check_player(pl_sk):
    conn, cursor = mysql_connection()
    cursor.execute(SK_QUERY % pl_sk)
    entity_id = cursor.fetchone()
    conn.close()
    if entity_id:
        pl_exists = True

    else:
        pl_exists = False
        entity_id = ''

    return pl_exists, entity_id

def check_title(name):
    conn, cursor = mysql_connection()
    name = "%" + name + "%"
    cursor.execute(PL_NAME_QUERY % (name, GAME))
    pl_id = cursor.fetchone()
    conn.close()
    return pl_id

def check_birth_date(pl_id):
    qury = 'select birth_date from sports_player where participant_id = %s'
    conn, cursor = mysql_connection()
    cursor.execute(qury %(pl_id))
    birt_date = cursor.fetchone()
    conn.close()
    return birt_date

def get_player_res(values):
    res = []
    for value in values:
        res_ = value.replace(': ', '').split(' ')[0]
        if ('\r' or '\n' or '\t') in res_:
            res_ = res_.replace(' ', '').split('\r')[0]
        res.append(res_)
    return res

def get_player_dob(values):
    result = []
    for value in values:
        res_ = value.replace(': ', '').replace('\r', '').\
                replace('\n', '').split(' ')
        if len(res_) >= 2:
            result.append(res_[0])
            result.append(res_[1])
        else:
            result.append(res_)
    return result

def add_source_key(entity_id, _id):
    if _id and entity_id:
        conn, cursor = mysql_connection()
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now())"
        values = (entity_id, 'participant', 'NFL', _id)

        cursor.execute(query, values)
        conn.close()

GAME = 'football'
PAR_TYPE = 'player'
BASE_POP = "200"
LOC = '0'
DEBUT = "0000-00-00"
ROLES = ''
SAL_POP = ''
RATING_POP = ''
GENDER = 'male'
MARITAL_STATUS = ''
PAR_SINCE = COMP_SINCE = ''
WEIGHT_CLASS = AKA = ''

SK_QUERY = 'select entity_id from sports_source_keys where \
            entity_type="participant" and source="NFL" and source_key= "%s"'

PL_NAME_QUERY = 'select id from sports_participants where \
title like "%s" and game="%s" and participant_type="player"';


#PL_SKS = ['2553764', '2552289', '2553723', '2552629', '2553913', '2552289']
PL_SKS = ['2553652']


GAME = 'football'
PAR_TYPE = 'player'
BASE_POP = "200"
LOC = '0'
DEBUT = "0000-00-00"
ROLES = ''
SAL_POP = ''
RATING_POP = ''
MARITAL_STATUS = ''
PAR_SINCE = COMP_SINCE = ''
WEIGHT_CLASS = AKA = ''

class NflPlay(VTVSpider):
    name = "nfl_playe"
    start_urls = []

    def start_requests(self):
        st_url = ['http://www.nfl.com/teams/roster?team=%s']
        st_url = open("nfl_list", "r+")
        for url in st_url:
            t_url = url
            yield Request(t_url, callback = self.parse, \
                              meta = {'team_sk': team})

    def populate_player(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, \
                '//div[@id="player-profile"]//div[@class="player-info"]')
        pl_exists = response.meta['pl_exists']
        pl_link = response.meta['pl_link']
        pl_sk = response.meta['pl_sk']
        for node in nodes:
            name = extract_data(node, './p/span[@class="player-name"]/text()')
            pl_id = check_title(name)
            '''if pl_id:
                if len(pl_id) >= 1:
                    #add_source_key(str(pl_id[0]), response.meta['pl_sk'][0])
                    print "Added source key" , name
                    continue'''
            team = extract_data(node, './p/a[contains(@href, "team")]/@href').\
                    split('=')[-1]
            pl_res = extract_list_data(node, './p[3]/text()')
            img = extract_data(node, \
                  './preceding-sibling::div[@class="player-photo"]//@src')
            if len(pl_res) >= 4:
                pl_height, pl_weight, age = get_player_res((pl_res[1], \
                                  pl_res[2], pl_res[3]))
            else:
                pl_height, pl_weight = get_player_res((pl_res[1], pl_res[2]))
                age = ''
            up_weight = ''
            up_height = ''

            if pl_weight:
                up_weight = float(pl_weight.strip()) * float(0.453592)
                up_weight = round(float(up_weight))
                up_weight = str(up_weight).replace('.0', '') + " kg"
            if pl_height:
                update_ft = float(pl_height.split("-")[0].strip()) * float(30.48)
                update_in = float(pl_height.split("-")[1].strip()) *float(2.54)
                up_height = round(float(update_ft) + float(update_in))
                up_height = str(up_height).replace('.0', '') + " cm" 
 

            pl_born = extract_list_data(node, './p[4]/text()')
            college = extract_list_data(node, './p[5]/text()')[0].replace(':', '').strip()
            if len(pl_born) == 2:
                pl_born = pl_born[1].split(',')
                if len(pl_born) == 2:
                    dob_list = get_player_dob((pl_born[0], pl_born[1]))
                    dob, b_place = dob_list[0] , dob_list[1]
                    state = REPLACE_STATE_DICT.get(pl_born[1].strip())
                    if not state:
                        pl_state = pl_born[1].strip()
                    else:
                        pl_state = state

                else:
                    dob = pl_born[0].replace(': ', '').split(' ')[0]
                    b_place = ''
                    pl_state = ''
                dob = (datetime.datetime(*time.strptime(dob.strip(), '%m/%d/%Y')[0:6])).date()
            else:
                dob = ''
                b_place = ''
                pl_state = ''
            status_remarks = standing = seed = status = ''
            player_role = response.meta['pos']

            if pl_exists == True:
                date_birth = get_birt_date(response.meta['pl_id'][0])
                import pdb;pdb.set_trace()
                cursor.execute(MAX_ID_QUERY)
                pl_data = cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                        replace('PL', '')) + 1)

                values = (next_id, next_gid, name, '', 'football', 'player', img, 200,  \
                      response.url, loc_id)
                cursor.execute(PAR_QUERY, values)
                values = (next_id, DEBUT, player_role, ROLES, 'male', \
                      age, pl_height, pl_weight, b_date, b_place, SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, '', '')

                cursor.execute(PL_QUERY, values)
                add_source_key(next_id, response.meta['pl_sk'][0])
                conn.close()
 
                print "Added player", name



    def parse(self, response):
        season = datetime.datetime.now().year
        hxs = Selector(response)
        participants = {}

        nodes = get_nodes(hxs, \
                '//div[@id="team-stats-wrapper"]//table//tbody//tr')
        for node in nodes:
            status = extract_data(node, './following::td[4]/text()')
            if 'ACT' in status:
                status = 'active'
            else:
                status = 'inactive'
            link = extract_data(node, './td/a/@href').strip()
            pl_sk = (re.findall(r'/(\d+)/profile', link))
            if pl_sk[0] not in PL_SKS:
                continue
            if "http" not in link:
                pl_link = "http://www.nfl.com" + link
            else:
                pl_link =  link
            position = extract_data(node, './td[3]/text()')
            position = get_player_role(position)
            pl_exists, p_id = check_player(pl_sk[0])

            if pl_exists == True:
                yield Request(pl_link, \
                             self.populate_player, \
                             meta = {'pos': position, 'pl_sk': pl_sk, 'pl_exists': pl_exists, 'pl_link': pl_linki, 'pl_id': p_id})
