import datetime
import time
import re
import MySQLdb
from vtvspider_new import VTVSpider, extract_data, get_nodes, extract_list_data, get_weight, get_height
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

def create_cursor():
    con = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
    cursor = con.cursor()
    return con, cursor

def check_player(pl_sk):
    con, cursor = create_cursor()
    cursor.execute(SK_QUERY % pl_sk)
    entity_id = cursor.fetchone()
    if entity_id:
        pl_exists = True
        pl_id = entity_id
    else:
        pl_exists = False
        pl_id = ''
    con.close()
    return pl_exists, pl_id

def add_source_key(entity_id, _id):
    if _id and entity_id:
        con, cursor = create_cursor()
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'espn_ncaa-ncf', _id)

        cursor.execute(query, values)
        con.close()

def check_title(name):
    con, cursor = create_cursor()
    cursor.execute(PL_NAME_QUERY % (name, GAME))
    pl_id = cursor.fetchone()
    con.close()
    return pl_id

PAR_QUERY = "insert into sports_participants (id, gid, title, aka, game, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at, display_title, short_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, now(), now(), %s, %s) on duplicate key update modified_at = now();"
MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'


PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"';

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="espn_ncaa-ncf" and source_key= "%s"'


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



PLAYER_POSITION_DICT = {'RB': 'Running back', 'S': 'Safety',
                        'WR': 'Wide receiver', 'DE': 'Defensive end',
                        'IL': 'Inside linebacker', 'OT': 'Offensive tackle',
                        'OL': 'Outside linebacker', 'LB': 'Linebacker',
                        'QB': 'Quarterback', 'DL': 'Defensive linemen',
                        'DB': 'Defensive back', 'LS': 'Long snapper',
                        'FB': 'Fullback', 'CB': 'Cornerback',
                        'TE': 'Tight end', 'PK': 'Placekicker',
                        'DT': 'Defensive tackle', 'P': 'Probable',
                        'NT': "Nose tackle", 'G': "Guard",
                        'C': "Center", '?': ''}

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', 'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', 'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', 'WV' : 'West Virgina', 'FL' : 'Florida', 'KS' : 'Kansas', 'TN' : 'Tennessee', \
'LA' : 'Louisiana', 'MO' : 'Missouri', 'AR' : 'Arkansas', 'SD' : 'South Dakota', 'MS' : 'Mississippi', 'MI' : 'Michigan', \
'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', 'ID' : 'Idaho', 'RI' : 'Rhode Island', \
'NM' : 'New Mexico', 'MN' : 'Minnesota', 'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', 'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado' }

def get_position(position):
    pos = ''
    for key, value in PLAYER_POSITION_DICT.iteritems():
        if position == key:
            pos = value
    return pos

class NCFplayers(VTVSpider):
    name = "ncf_players"
    start_urls = ['http://espn.go.com/college-football/teams']

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="mod-content"]//ul//li')

        for node in nodes[:30]:
            team_link = extract_data(node, './/a[contains(@href, "college-football")]/@href')
            if team_link:
                team_link = team_link.replace('_', 'roster/_')
                if "roster" not in team_link:
                    continue
                yield Request(team_link, callback=self.parse_teamdetails)

    def parse_teamdetails(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        participants = {}
        team_sk = response.url.split('/')[-2]
        pl_nodes = get_nodes(hxs, '//div[@class="mod-content"]//table//tr[contains(@class, "row")]')
        for pl_node in pl_nodes:
            pl_data = extract_list_data(pl_node, './/td/text()')
            pos = get_position(pl_data[1])
            pl_link = extract_data(pl_node, './/td/a/@href')
            if pl_link:
                yield Request(pl_link, callback=self.parse_players, meta = {'pos': pos})

    def parse_players(self, response):
        hxs = Selector(response)
        pl_sk = response.url.split('/')[-2]
        pos = response.meta['pos']
        nodes = get_nodes(hxs, '//div[@class="mod-content"]//div[@class="player-bio"]')
        for node in nodes:
            player_title = extract_data(node, './/preceding-sibling::h1/text()').strip()
            dobth = extract_data(node, './/ul//li[span[contains(text(), "Birth")]]/text()')
            if dobth:
                if "(Age" in dobth:
                    dob = dobth.split('(')[0].strip()
                    dob = (datetime.datetime(*time.strptime(dob.strip(), '%B %d, %Y')[0:6])).date()
                    age = dobth.split('(Age:')[1].strip().replace(')', '')
                else:
                    dob = (datetime.datetime(*time.strptime(dobth.strip(), '%B %d, %Y')[0:6])).date()
                    age = ''
            else:
                dob = ''
                age = ''

            city = ''
            state = ''
            birth_place = extract_data(node, './/ul//li[span[contains(text(), "Hometown")]]/text()')
            if "," in birth_place:
                city = birth_place.split(',')[0].strip()
                state = birth_place.split(',')[1].strip()
                state = REPLACE_STATE_DICT.get(state)

            loc_id = ''
            con, cursor = create_cursor()
            if city:
                loc_id = 'select id from sports_locations where city ="%s" and state = "%s" limit 1' %(city, state)
                cursor.execute(loc_id)
                loc_id = cursor.fetchall()
                if loc_id:
                    loc_id = str(loc_id[0][0])
                else:
                    loc_id = ''

            height = extract_data(node, './/ul//li[span[contains(text(), "Height")]]/text()')
            if "-" in height:
                feets = height.split('-')[0]
                inches = height.split('-')[1]
            if height and "-" not in height:
                feets = height
                inches = ''
            if height:
                height = get_height(feets = feets, inches = inches)
            weight = extract_data(node, './/ul//li[span[contains(text(), "Weight")]]/text()')
            if "lbs" in weight:
                weight = weight.replace('.', '').replace('lbs', '').strip()
                weight = get_weight(lbs = weight)
            pl_exists, pl_id = check_player(pl_sk)
            if pl_exists == False:
                pts_id = check_title(player_title)
                if pts_id:
                    f = open('ncf_existing_players', 'w')
                    f.write('%s\t\%s\t%s\n'%(pl_sk, player_title, response.url))

                    print player_title
                    #add_source_key(str(pts_id[0]), pl_sk)
                else:
                    img = ''
                    cursor.execute(MAX_ID_QUERY)
                    pl_data = cursor.fetchall()
                    max_id, max_gid = pl_data[0]
                    next_id = max_id + 1
                    next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                            replace('PL', '')) + 1)

                    values = (next_id, next_gid, player_title, AKA, GAME, PAR_TYPE, img, \
                          BASE_POP, response.url, loc_id)
                    cursor.execute(PAR_QUERY, values)
                    values = (next_id, DEBUT, pos, ROLES, GENDER, \
                          age, height, weight, dob, birth_place, SAL_POP, RATING_POP, \
                          WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, '', '')

                    cursor.execute(PL_QUERY, values)
                    add_source_key(next_id, pl_sk)
                    con.close()
