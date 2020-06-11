from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
from vtvspider_new import VTVSpider, extract_data, get_nodes, get_height, get_weight
from vtvspider_new import get_height, get_weight, get_player_details, \
get_birth_place_id, get_sport_id
import re
import datetime
import urllib2
import MySQLdb


PAR_QUERY = "insert into sports_participants (id, gid, title, aka, game, sport_id, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, birth_place_id, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at, display_title, short_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, %s, now(), now(), %s, %s) on duplicate key update modified_at = now();"

MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'


#PL_NAME_QUERY = 'select id from sports_participants where \
#title = "%s" and game="%s" and participant_type="player"';

PL_NAME_QUERY = 'select P.id from sports_participants P, sports_players PL where P.title="%s" and P.game="%s" and P.id=PL.participant_id and PL.birth_date="%s"'

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="MLB" and source_key= "%s"'


GAME = 'baseball'
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

ROLE_MAP = {"P" : "Pitcher", "C" : "Catcher",  \
            "1B": "First baseman", \
            "2B": "Second baseman","3B": "Third baseman", \
            "SS": "Shortstop", \
            "LF": "Left fielder", "CF": "Center fielder", "RF": "Right fielder", \
            "DH": "Designated hitter", "OF": "Outfielder"}

STATES_DICT = {'CO': 'Colorado', 'TX': 'Texas',
               'AL': 'Alabama', 'MI': 'Michigan',
               'PA': 'Pennsylvania', 'MO': 'Missouri',
               'NC': 'North Carolina', 'FL': 'Florida',
               'OK': 'Oklahoma', 'CA': 'California',
               'IN': 'Indiana', 'IL': 'Illinois',
               'MA': 'Massachusetts', 'NY': 'New York',
               'CT': 'Connecticut', 'TN': 'Tennessee',
               'OH': 'Ohio', 'AR': 'Arkansas', 'OR': 'Oregon',
               'VA': 'Virginia', 'WA': 'Washington', 'SC': 'South Carolina',
               'LA': 'Louisiana', 'NV': 'Nevada', 'NJ': 'New Jersey',
               'KY': 'Kentucky', 'MN': 'Minnesota', 'GA': 'Georgia',
               'KS': 'Kansas', 'MD': 'Maryland', 'AZ': 'Arizona',
               'SD': 'South Dakota', 'MS': 'Mississippi',
               'NE': 'Nebraska', 'BC': 'British Columbia',
               'DE': 'Delaware', 'HI': 'Hawaii', 'ND': 'North Dakota',
               'VI': 'United States Virgin Islands',
               'NH': 'New Hampshire', 'WI': 'Wisconsin', 'IA': 'Iowa',
               'NM': 'New Mexico', 'ID': 'Idaho', 'WY': 'Wyoming',
               'AK': 'Alaska', 'ME': 'Maine', 'DC': 'District of Columbia',
               'ON': 'Ontario', 'WV': 'West Virginia', 'RI': 'Rhode Island'}

class MlbPlayers(VTVSpider):
    name            = "mlb_players"
    start_urls      = ['http://mlb.mlb.com/mlb/players/index.jsp']
    player_ref_url  = 'http://m.mlb.com/player/%s/%s'

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        #self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

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


    def add_source_key(self, entity_id, _id):
        if _id and entity_id:
            query = "insert into sports_source_keys (entity_id, entity_type, \
                     source, source_key, created_at, modified_at) \
                     values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
            values = (entity_id, 'participant', 'MLB', _id)

            self.cursor.execute(query, values)

    def check_title(self, name, dob):
        self.cursor.execute(PL_NAME_QUERY % (name, GAME, dob))
        pl_id = self.cursor.fetchone()
        return pl_id


    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//select[@id="ps_team"]/option')
        for node in nodes:
            link = extract_data(node, './@value').strip()
            team_name = extract_data(node, './text()').strip()
            if "Team Rosters" in team_name:
                continue
            if "http:" not in link:
                continue
            yield Request(link, callback = self.parse_listing, \
                meta = {'team_name':team_name})

    def parse_listing(self, response):
        hxs = HtmlXPathSelector(response)
        nodes = get_nodes(hxs, '//table[@class="team_table_results"]/tbody/tr//a')
        last_node = nodes[-1]
        participants = {}
        for node in nodes:
            terminal_crawl = False
            if node == last_node:
                terminal_crawl = True
            player_link = extract_data(node, './@href')
            pl_link = response.url.split('/team')[0] + player_link
            if not player_link:
                continue
            player_id = re.findall('player_id=(\d*)', player_link)[0]
            pl_exists, pl_id = self.check_player(player_id)
            player_link = "http://mlb.mlb.com/lookup/json/named.player_info.bam?sport_code='mlb'&player_id=%s" % (player_id)
            yield Request (player_link, self.parse_playeradd, meta= {'pl_exists': pl_exists, 'pl_id': pl_id})

    def parse_playeradd(self, response):
        raw_data        = response.body
        data = eval(raw_data)
        p_info = data['player_info']['queryResults']['row']
        player_number = p_info['jersey_number']
        role = p_info['primary_position_txt']
        if ROLE_MAP.has_key(role):
            role = ROLE_MAP[role]

        sourcekey = p_info['player_id']
        team_callsign =  p_info['team_abbrev']
        player_name =  p_info['name_display_first_last']
        dob = p_info['birth_date'].replace('T', ' ').split(' ')[0].strip()
        try:
            dob = str(datetime.datetime.strptime(dob, '%Y-%m-%d'))
        except:
            dob = ''
        b_place = p_info['birth_city']
        b_state = p_info['birth_state']
        if len(b_state) == 2:
            if STATES_DICT.get(b_state, ''):
                b_state = STATES_DICT[b_state]
            else:
                print b_state, response.url
        b_country = p_info['birth_country']
        birth_place = ', '.join([place.strip() for place in [b_place, b_state, b_country] if place.strip()])
        weight = p_info['weight']
        feet = p_info['height_feet']
        inches = p_info['height_inches']
        main_role = ''
        age = p_info['age']
        debut_date = p_info.get('pro_debut_date', '').replace('T', ' ')
        pl_id = self.check_title(player_name, dob)

        height = get_height(feet, inches)
        weight = get_weight(weight)
        ref_url = self.player_ref_url % (sourcekey, player_name.replace(' ', '-').lower())
        loc_id = get_birth_place_id(b_place, b_state, b_country)

        if response.meta['pl_exists'] == True:
            details = { 'age': age, 'birth_place': birth_place,
                        'height': height, 'weight': weight,
                        'debut_date': debut_date, 'role': role,
                        'ref_url': ref_url, 'loc_id': loc_id}

            pl_id = response.meta['pl_id']
            get_player_details(details, pl_id)
        else:
            if pl_id:
                self.add_source_key(str(pl_id[0]), sourcekey)
                print "Added sk", player_name
            else:
                img = 'http://mlb.mlb.com/images/players/mugshot/ph_%s.jpg' %(sourcekey)
                self.cursor.execute(MAX_ID_QUERY)
                pl_data = self.cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                        replace('PL', '')) + 1)
                sport_id = get_sport_id(GAME)
                values = (next_id, next_gid, player_name, AKA, GAME, sport_id, PAR_TYPE, img, \
                      BASE_POP, ref_url, loc_id)
                self.cursor.execute(PAR_QUERY, values)

                values = (next_id, debut_date, role, ROLES, GENDER, \
                      age, height, weight, dob, birth_place, loc_id, SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, '', '')

                self.cursor.execute(PL_QUERY, values)

                self.add_source_key(next_id, sourcekey)
                print "Added player", player_name

