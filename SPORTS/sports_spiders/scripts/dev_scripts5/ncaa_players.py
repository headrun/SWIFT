import datetime
import time
import re
import MySQLdb
from vtvspider_new import VTVSpider, extract_data, get_nodes,  \
extract_list_data, get_weight, get_height, get_age
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


def check_player(self, pl_sk):
    self.cursor.execute(SK_QUERY % pl_sk)
    entity_id = self.cursor.fetchone()
    if entity_id:
        pl_exists = True
        pl_id = entity_id
    else:
        pl_exists = False
        pl_id = ''
    return pl_exists, pl_id

def add_source_key(self, entity_id, _id):
    if _id and entity_id:
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'espn_ncaa-ncb', _id)

        self.cursor.execute(query, values)

def check_title(self, name, dob_):
    self.cursor.execute(PL_NAME_QUERY % (name, GAME, dob_))
    pl_id = self.cursor.fetchone()
    con.close()
    return pl_id

PAR_QUERY = "insert into sports_participants (id, gid, title, aka, game, sport_id, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at, display_title, short_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, now(), now(), %s, %s) on duplicate key update modified_at = now();"
MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'


'''PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"';'''


PL_NAME_QUERY = 'select P.id from sports_participants P, sports_players PL where P.title="%s" and P.game="%s" and P.id=PL.participant_id and PL.birth_date="%s"'

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="espn_ncaa-ncb" and source_key= "%s"'


GAME = 'basketball'
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

def update_pl_location(self, pl_id, loc_id):
    values = (loc_id, pl_id)
    self.cursor.execute(LOC_QUERY % values)

LOC_QUERY = 'update sports_participants set location_id = %s where location_id = "" and id = %s and participant_type = "player" and game="basketball"'



PLAYER_POSITION_DICT = {'RB': 'Running back', 'S': 'Safety',
                        'WR': 'Wide receiver', 'DE': 'Defensive end',
                        'IL': 'Inside linebacker', 'OT': 'Offensive tackle',
                        'OL': 'Outside linebacker', 'LB': 'Linebacker',
                        'QB': 'Quarterback', 'DL': 'Defensive linemen',
                        'DB': 'Defensive back', 'LS': 'Long snapper',
                        'FB': 'Fullback', 'CB': 'Cornerback',
                        'TE': 'Tight end', 'PK': 'Placekicker',
                        'DT': 'Defensive tackle', 'P': 'Probable',
                        'NT': "Nose tackle", 'G': "Guard", "F" : "Forward",
                        'C': "Center", 'G-F': "Forward-Guard",
                        'F-C': "Forward-Center", '?': ""}


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
                    'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado', \
                    "SA" : 'South Australia', "NSW": 'New South Wales', \
                    "ON" : "Ontario", 'NH': "New Hampshire", \
                    'DC': "District of Columbia", \
                    'ME': "Maine", 'AK': "Alaska", \
                    'LA': "Louisiana", 'VIC': "Victoria", \
                    'WA': 'Washington', 'QLD' : "Queensland", \
                    'PQ': 'Quebec', 'NV': "Nevada", \
                    'AZ': 'Arizona'}

def get_position(position):
    pos = ''
    for key, value in PLAYER_POSITION_DICT.iteritems():
        if position == key:
            pos = value
    return pos

class NCBplayers(VTVSpider):
    name = "ncb_players"
    start_urls = ['http://espn.go.com/mens-college-basketball/teams']


    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.today = datetime.datetime.now().date()

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="mod-content"]//ul/li')

        for node in nodes:
            team_link = extract_data(node, './/a[contains(@href, "college-basketball")]/@href')
            if team_link:
                team_link = team_link.replace('_', 'roster/_')
                if "roster" not in team_link:
                    continue
                yield Request("http://espn.go.com/mens-college-basketball/team/roster/_/id/399", callback=self.parse_teamdetails)

    def parse_teamdetails(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        participants = {}
        team_sk = response.url.split('/')[-2]
        pl_nodes = get_nodes(hxs, '//div[@class="mod-content"]/table/tr[contains(@class, "row")]')

        for pl_node in pl_nodes:
            pl_data = extract_list_data(pl_node, './td/text()')
            pos = get_position(pl_data[1])
            pl_link = extract_data(pl_node, './td/a/@href')

            if pl_link:
                yield Request(pl_link, callback=self.parse_players, meta = {'pos': pos})

    def parse_players(self, response):
        hxs = Selector(response)
        pl_sk = response.url.split('/')[-2]
        pos = response.meta['pos']
        nodes = get_nodes(hxs, '//div[@class="mod-content"]//div[@class="player-bio"]')
        for node in nodes:
            player_title = extract_data(node, './/preceding-sibling::h1/text()').strip()
            if "Null" in player_title:
                continue
            dobth = extract_data(node, './/ul//li[span[contains(text(), "Birth")]]/text()')

            if dobth:

                if "(Age" in dobth:
                    dob = dobth.split('(')[0].strip()
                    dob = (datetime.datetime(*time.strptime(dob.strip(), '%B %d, %Y')[0:6])).date()
                    age = dobth.split('(Age:')[1].strip().replace(')', '')

                else:
                    dob = (datetime.datetime(*time.strptime(dobth.strip(), '%B %d, %Y')[0:6])).date()
                    age = get_age(born = dobth.strip(), pattern = '%B %d, %Y')
            else:
                dob = ''
                age = ''

            height = extract_data(node, './/ul//li[span[contains(text(), "Height")]]/text()')
            weight = extract_data(node, './/ul//li[span[contains(text(), "Weight")]]/text()')

            if "-" in height:
                feets = height.split('-')[0]
                inches = height.split('-')[1]
            if height and "-" not in height:
                feets = height
                inches = ''
            if height:
                height = get_height(feets = feets, inches = inches)

            if "lbs" in weight:
                weight = weight.replace('.', '').replace('lbs', '').strip()
                weight = get_weight(lbs = weight)

            b_place = extract_data(node, './/ul//li[span[contains(text(), "Hometown")]]/text()').replace('.', '')

            if "," in b_place:
                city = b_place.split(',')[0].strip()
                state = REPLACE_STATE_DICT.get(b_place.split(',')[1].strip(), '')
                if state:
                    birth_place = city +", " + state
                else:
                    birth_place = ''
            else:
                birth_place =  b_place

            pl_exists, pl_id = check_player(self, pl_sk)

            if len(b_place.split(',')) == 2:
                city = b_place.split(',')[0].strip()
                state = REPLACE_STATE_DICT.get(b_place.split(',')[1].strip(), '')
                loc_id = 'select id from sports_locations where city ="%s" and state = "%s" limit 1' %(city, state)
                self.cursor.execute(loc_id)
                loc_id = self.cursor.fetchall()

                if loc_id:
                    loc_id = str(loc_id[0][0])
            else:
                print b_place
                loc_id = 0
            if loc_id == ():
                loc_id = ''

            if pl_exists == True:
                details = { 'age': age, 'birth_place': birth_place,
                            'height': height, 'weight': weight,
                            'pos': pos, 'pl_sk': pl_sk,
                            'loc_id': loc_id, 'ref_url': response.url}

                self.update_pl_details(details, pl_id)


            '''if pl_exists == False:
                if dobth:
                    dt = datetime.datetime.strptime(dobth.strip(), "%B %d, %Y")
                    dob_ = dt.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    dob_ = "0000-00-00 00:00:00"
                pts_id = check_title(self, player_title, dob_)

                if pts_id:
                    f = open('ncb_existing_players', 'w')
                    f.write('%s\t\%s\t%s\n'%(pl_sk, player_title, response.url))

                    #add_source_key(str(pts_id[0]), pl_sk)
                else:
                    img = 'http://sports.cbsimg.net/images/players/unknown_player.gif'
                    self.cursor.execute(MAX_ID_QUERY)
                    pl_data = self.cursor.fetchall()
                    max_id, max_gid = pl_data[0]
                    next_id = max_id + 1
                    next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                            replace('PL', '')) + 1)

                    values = (next_id, next_gid, player_title, AKA, GAME, '2', PAR_TYPE, img, \
                          BASE_POP, response.url, loc_id)
                    self.cursor.execute(PAR_QUERY, values)
                    values = (next_id, DEBUT, pos, ROLES, GENDER, \
                          age, height, weight, dob, birth_place, SAL_POP, RATING_POP, \
                          WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, '', '')

                    self.cursor.execute(PL_QUERY, values)
                    add_source_key(next_id, pl_sk)'''


    def update_pl_details(self, details, pl_id):
        birth_place = details.get('birth_place', '')
        age = details.get('age', '')
        height = details.get('height', '')
        weight = details.get('weight', '')
        role = details.get('pos', '')
        loc_id = details.get('loc_id', '')
        ref_url = details.get('ref_url', '')
        pl_id = str(pl_id[0])

        if birth_place:
            query = 'update sports_players set birth_place=%s where participant_id=%s'
            values = (birth_place, pl_id)
            self.cursor.execute(query, values)
        if age:
            query = 'update sports_players set age=%s where participant_id=%s'
            values = (age, pl_id)
            self.cursor.execute(query, values)
        if height:
            query = 'update sports_players set height=%s where participant_id=%s'
            values = (height, pl_id)
            self.cursor.execute(query, values)
        if weight:
            query = 'update sports_players set weight=%s where participant_id=%s'
            values = (weight, pl_id)
            self.cursor.execute(query, values)
        if role:
            query = 'update sports_players set main_role=%s where participant_id=%s'
            values = (role, pl_id)
            self.cursor.execute(query, values)
        if ref_url:
            query = 'update sports_participants set reference_url=%s where id=%s'
            values = (ref_url, pl_id)
            self.cursor.execute(query, values)
        if loc_id:
            query = 'update sports_participants set location_id=%s where id=%s'
            values = (loc_id, pl_id)
            self.cursor.execute(query, values)

