from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider_new import VTVSpider, extract_data, get_nodes,  \
extract_list_data
from vtvspider_new import get_height, get_weight, get_player_details, \
get_birth_place_id, get_sport_id
import re
import time
import datetime
import MySQLdb


PAR_QUERY = "insert into sports_participants (id, gid, title, aka, game, sport_id, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, \
            birth_place_id, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at, short_title, display_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, %s, now(), now(), %s, %s) on duplicate key update modified_at = now();"

MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="NHL" and source_key= "%s"'


PL_NAME_QUERY =  'select P.id from sports_participants P, sports_players PL where P.title="%s" and P.game="%s" and P.id=PL.participant_id and PL.birth_date="%s"'


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
                    "ON" : "Ontario", "AK": "Alaska", "BC": "British Columbia", \
                    "SK": "Saskatchewan", "QC": "Quebec", "AB": "Alberta", \
                    "NS" : "Nova Scotia", "MB": "Manitoba", "WA" : "Washington",  \
                    "NB" : "New Brunswick", 'AZ': "Arizona", \
                    'PE': "Prince Edward Island", \
                    'NL': "Newfoundland and Labrador", \
                    'ND': "North Dakota", "DC": "District of Columbia", \
                    'NH': "New Hampshire", 'ME': "Maine", 'LA': "Louisiana"}


GAME = 'hockey'
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


def add_source_key(self, entity_id, _id):
    if _id and entity_id:
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'NHL', _id)

        self.cursor.execute(query, values)


def check_player(self, pl_sk):
    self.cursor.execute(SK_QUERY % pl_sk)
    entity_id = self.cursor.fetchone()
    if entity_id:
        pl_exists = True
        pl_id = str(entity_id[0])
    else:
        pl_exists = False
        pl_id = ''
    return pl_exists, pl_id

def check_title(self, name, b_date):
    self.cursor.execute(PL_NAME_QUERY % (name, GAME, b_date))
    pl_id = self.cursor.fetchone()
    if pl_id:
        pl_id = str(pl_id[0])
    else:
        pl_id = ''
    return pl_id

def get_locations(self, b_place):
    if len(b_place.split(',')) == 3:
        city = b_place.split(',')[0].strip()
        state = REPLACE_STATE_DICT.get(b_place.split(',')[1].strip(), '')
        if not state:
            state = b_place.split(',')[1].strip()
        country = b_place.split(',')[-1].strip().replace('United States', 'USA')
        birth_place = city, state, country
        birth_place =  ', '.join(birth_place)
    elif len(b_place.split(',')) ==2:
        city = b_place.split(',')[0].strip()
        state =  ''
        country = b_place.split(',')[-1].strip().replace('United States', 'USA')
        birth_place = city, country
        birth_place =  ', '.join(birth_place)
    else:
        city = state = country = ''
        birth_place = ''
    if country == "United States":
        country = "USA"
    return city, state, country, birth_place


class NhlPlayers(VTVSpider):
    name = "nhl_playerss"
    start_urls = ['http://www.nhl.com/ice/teams.htm?navid=nav-tms-main']
    participants = {}

    def __init__(self):
        #self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.today = datetime.datetime.now().date()


    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@id="realignmentMap"]//ul[@class="teamData"]')
        last_node = nodes[-1]
        for node in nodes:
            callsign  = extract_data(node, './/a/@rel')
            team_url = extract_data(node, './/a[contains(@href, "roster")]/@href').strip()

            if "m_home" in team_url:
                continue

            yield Request(team_url, callback = self.parse_roster, \
            meta = {'call_sign': callsign})

    def parse_roster(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div/table[@class="data"]//tr[not(contains(@class, "hdr"))]')
        last_node = nodes[-1]

        for node in nodes[::1]:
            player_lk = extract_data(node, './/a/@href')
            player_id = re.findall(r"\d+", player_lk)

            if player_id:
                player_id = player_id[0]
            else:
                continue

            #player_link = 'http://www.nhl.com/ice/player.htm?id=%s' % (player_id)
            player_link = response.url.split('/club/')[0] + player_lk
            yield Request(player_link, callback = self.parse_details, \
            meta = {'call_sign': response.meta['call_sign'],
            'player_id': player_id})

    def parse_details(self, response):
        hxs = Selector(response)
        pl_link = response.url
        player_sk = response.meta['player_id']
        pl_exists, pl_id = check_player(self, player_sk)
        aka = loc = dbt = role = \
        sal_pop = rpop = wclass = mstatus = psince = csince = status = \
        status_remarks = standing = seed = ''

        pl_height = extract_data(hxs, '//div[@class="plyrTmbStatLine"]//span[contains(text(), "Height")]//following-sibling::text()')
        pl_weight = extract_data(hxs, '//div[@class="plyrTmbStatLine"]//span[contains(text(), "Weight")]//following-sibling::text()')
        birth_date = extract_data(hxs, '//div[@class="plyrTmbStatLine"]//span[contains(text(), "Born")]//following-sibling::text()')
        birth = birth_date.replace('\n', '').replace(u'\xa0', '').split('(Age')
        birth_place = extract_data(hxs, '//div[@class="plyrTmbStatLine"]//span[contains(text(), "Birthplace:")]//following-sibling::text()')
        b_place = birth_place.replace('Yekaterinbug', 'Yekaterinburg')
        name = extract_data(hxs, '//div[@class="plyrTmbPlayerName"]//text()').strip()
        player_role = extract_data(hxs, '//div[@class="plyrTmbPositionTeam"]//text()').strip()
        pl_image = extract_data(hxs, '//table//tr//td[@valign="top"]/img/@src')
        player_role = player_role.split('-')[0].strip()

        up_weight = up_height = ''

        if pl_weight:
            up_weight = get_weight(lbs = pl_weight)

        if pl_height:
            update_ft = pl_height.split("'")[0].strip()
            update_in = pl_height.split("'")[1].replace("'", '').replace('"', '').strip()
            up_height = get_height(feets = update_ft, inches = update_in)

        birth = birth_date.replace('\n', '').replace(u'\xa0', '').split('(Age')

        if len(birth) == 2:
            age = birth[1].replace(')', '').strip()
            if "," in birth[0]:
                try:
                    dt = datetime.datetime.strptime(birth[0].strip().strip(), '%B %d, %Y')
                    b_date = dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    dt = datetime.datetime.strptime(birth[0].strip().strip(), '%b %d, %Y')
                    b_date = dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                dt = datetime.datetime.strptime(birth[0].strip().strip(), '%d %b %Y')
                b_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            b_date = ''
            age = ''

        city, state, country, birth_place = get_locations(self, b_place)

        loc_id = ''
        if city:
            loc_id = get_birth_place_id(city, state, country)

        pid = ''
        if pl_exists == True:
            details = { 'age': age, 'birth_place': birth_place,
                            'height': up_height, 'weight': up_weight,
                            'pos': player_role,
                            'loc_id': loc_id, 'ref_url': response.url}

            get_player_details(details, pl_id)


        if pl_exists == False:
            pid = check_title(self, name, b_date)

        if not pid and pl_exists == False:

            self.cursor.execute(MAX_ID_QUERY)
            pl_data = self.cursor.fetchall()
            max_id, max_gid = pl_data[0]
            next_id = max_id + 1
            next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                    replace('PL', '')) + 1)
            sport_id = get_sport_id(GAME)
            values = (next_id, next_gid, name, '', 'hockey', sport_id, 'player', pl_image, 200,  \
                  response.url, loc_id)
            self.cursor.execute(PAR_QUERY, values)
            values = (next_id, DEBUT, player_role, role,  'male', \
                  age, up_height, up_weight, b_date, birth_place, loc_id, SAL_POP, RATING_POP, \
                  WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, '', '')

            self.cursor.execute(PL_QUERY, values)
            add_source_key(self, next_id, player_sk)
            print "Added player", name


