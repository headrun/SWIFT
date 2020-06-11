from scrapy.http import Request
import re
import datetime
from scrapy.selector import Selector
from sports_spiders.vtvspider import VTVSpider, extract_data, extract_list_data, \
    get_nodes, get_height, get_weight, get_height, get_weight, get_player_details, \
    get_birth_place_id, get_sport_id, get_state, get_country, get_age
import MySQLdb
import json


PAR_QUERY = "insert into sports_participants (id, gid, title, aka, sport_id, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, country, created_at, modified_at) \
             values (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, \
            birth_place_id, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, height_cms, weight_kgs, created_at, modified_at, short_title, display_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, %s, now(), now(), %s, %s) on duplicate key update modified_at = now();"

MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="euro_hockey" and source_key= "%s"'


PL_NAME_QUERY = 'select P.id from sports_participants P, sports_players PL where P.title="%s" \
                    and P.sport_id="%s" and P.id=PL.participant_id and PL.birth_date="%s"'

INSERT_PHA = "Insert into sports_participants_phrases(participant_id, phrase, language, \
                    culture, region, field, weight, created_at, modified_at) \
            values(%s, %s, %s, %s, %s, %s, %s, now(),now()) on duplicate key update modified_at = now()"

REPLACE_STATE_DICT = {'TN': 'Tennessee', 'OH': 'Ohio', 'VA': 'Virginia',
                      'TX': 'Texas', 'OK': 'Oklahoma', 'NY': 'New York',
                      'NJ': 'New Jersey', 'IL': 'Illinois', 'AL': 'Alabama',
                      'NC': 'North Carolina', 'SC': 'South Carolina', 'GA': 'Georgia',
                      'OR': 'Oregon', 'DE': 'Delaware', 'IA': 'Iowa',
                      'WV': 'West Virgina', 'FL': 'Florida',
                      'KS': 'Kansas', 'TN': 'Tennessee',
                      'LA': 'Louisiana', 'MO': 'Missouri',
                      'AR': 'Arkansas', 'SD': 'South Dakota',
                      'MS': 'Mississippi', 'MI': 'Michigan',
                      'UT': 'Utah', 'MT': 'Montana', 'NE': 'Nebraska',
                      'ID': 'Idaho', 'RI': 'Rhode Island',
                      'NM': 'New Mexico', 'MN': 'Minnesota',
                      'PA': 'Pennsylvania', 'MD': 'Maryland', 'IN': 'Indiana',
                      'CA': 'California', 'WI': 'Wisconsin', 'KY': 'Kentucky',
                      'MA': 'Massachusetts', 'CT': 'Connecticut', 'CO': 'Colorado',
                      "ON": "Ontario", "AK": "Alaska", "BC": "British Columbia",
                      "SK": "Saskatchewan", "QC": "Montreal", "AB": "Alberta",
                      "NS": "Nova Scotia", "MB": "Manitoba", "WA": "Washington",
                      "NB": "New Brunswick", 'AZ': "Arizona",
                      'PE': "Prince Edward Island", 'NV': "Nevada",
                      'NL': "Newfoundland and Labrador",
                      'ND': "North Dakota", "DC": "District of Columbia",
                      'NH': "New Hampshire", 'ME': "Maine", 'LA': "Louisiana",
                      "HI": "Hawaii", 'WY': "Wyoming"}


GAME = 'american football'
SPORT_ID = '4'
PAR_TYPE = 'player'
BASE_POP = "200"
LOC = '0'
DEBUT = "0000-00-00"
ROLES = ''
SAL_POP = ''
RATING_POP = ''
MARITAL_STATUS = ''
PAR_SINCE = COMP_SINCE = ''
WEIGHT_CLASS = ''


def add_source_key(self, entity_id, _id):
    if _id and entity_id:
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'espn_nfl', _id)

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
    self.cursor.execute(PL_NAME_QUERY % (name, SPORT_ID, b_date))
    pl_id = self.cursor.fetchone()
    if pl_id:
        pl_id = str(pl_id[0])
    else:
        pl_id = ''
    return pl_id


class ShlPlayer(VTVSpider):
    name = "nfl_espn_players"
    start_urls = ['http://www.espn.com/nfl/teams']

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.80", user="veveo",
                                    passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.today = datetime.datetime.now().date()
        self.domain_url = 'http://www.espn.com/nfl/'

    def parse(self, response):
        sel = Selector(response)
        teams_links = extract_list_data(
            sel, '//div[@class="TeamLinks__Links"]/span/a[contains(@href, "/team/roster/")]/@href')
        for team_links in teams_links:
            team_link = ''.join(re.findall('/name/(\w+)/', team_links))
            top_url = 'http://site.web.api.espn.com/apis/site/v2/sports/football/nfl/teams/%s/roster?region=us&lang=en&contentorigin=espn' % (
                team_link)
            yield Request(top_url, callback=self.parse_next)

    def parse_next(self, response):
        data = json.loads(response.body)
        player_info = data.get('athletes', [])
        for player_in in player_info:
            full_name = player_in.get('fullName', '')
            display_name = player_in.get('displayName', '')
            short_name = player_in.get('shortName', '')
            player_sk = player_in.get('id', '')
            player_pos = player_in.get('position', {}).get('name', '')
            player_dob = player_in.get('dateOfBirth', '')
            if player_dob:
                player_dob1 = player_dob.split('T')
                pl_birth_date = player_dob1[0]+' 00:00:00'
            pl_age = player_in.get('age', '')
            player_hei = player_in.get('displayHeight', '')
            if player_hei:
                player_hei = player_hei.split("'")
                player_hei_ft = int(player_hei[0].strip())
                player_hei_inch = int(player_hei[-1].strip())
                player_hei_inch += player_hei_ft * 12
                pl_hei_cm = round(player_hei_inch * 2.54, 1)
                if pl_hei_cm:
                    pl_hei_cm_cm = str(int(pl_hei_cm)) + ' cm'
            player_wei = player_in.get('weight', '')
            if player_wei:
                player_wei1 = round(int(player_wei)/(2.2))
                player_wei1_wt = str(int(player_wei1)) + ' kg'
            pl_debut = player_in.get('debutYear', '')
            player_urls = player_in.get('links', [])
            for player_ur in player_urls:
                player_url = player_ur.get('href', '')

            birth_place, loc_id, role = ['']*3
            city = state = country = ''
            birth_place_city = player_in.get('birthPlace', {}).get('city', '')
            birth_place_state = player_in.get(
                'birthPlace', {}).get('state', '')
            if birth_place_state:
                birth_place_stat = REPLACE_STATE_DICT.get(birth_place_state)
            birth_place_country = player_in.get(
                'birthPlace', {}).get('country', '')
            if birth_place_country == 'Virgin Islands':
                birth_place_country = 'US Virgin Islands'
            pl_nu = player_in.get('jersey', '')
            if city:
                country = 'USA'
                loc_id = get_birth_place_id(city, state, country)
                city = state = ''
                if loc_id == '' and birth_place_city != '':
                    loc_id = get_birth_place_id(city, state, country)
                elif loc_id == '' and birth_place_country != '':
                    loc_id = get_birth_place_id(
                        city, state, birth_place_country)
                elif loc_id == '' and birth_place_stat != '':
                    loc_id = get_birth_place_id(city, state, country)

            country_dict = {"Poland": "WIKI22936", "Brazil": "WIKI3383", "USA": "WIKI3434750",
                            "Belize": "WIKI3458", "Scotland": "WIKI26994", "South Korea": "WIKI27019",
                            "Turkey": "WIKI11125639", "Estonia": "WIKI28222445", "Germany": "WIKI11867",
                            "Cameroon": "WIKI5447", "Ghana": "WIKI12067", "Australia": "WIKI4689264",
                            "South Africa": "WIKI17416221", "Japan": "WIKI15573", "England": "WIKI9316",
                            "Albania": "WIKI738", "Nigeria": "WIKI21383", "Jamaica": "WIKI15660",
                            "Haiti": "WIKI13373", "Tonga": "WIKI30158", "American Samoa": "", "Sierra Leone": "",
                            "US Virgin Islands": "WIKI32135"}
            if birth_place_country:
                coun_wiki = country_dict.get(birth_place_country, '')
            else:
                coun_wiki = 'WIKI3434750'
            pl_exists, pl_id = check_player(self, player_sk)
            if pl_exists == True and pl_id:
                update_query_part = "update sports_participants set country='%s' where id='%s'" % (
                    coun_wiki, pl_id)
                self.cursor.execute(update_query_part)
                print('PLayer Already exists in the DB.')

            if pl_exists == False:
                pid = check_title(self, player_name, pl_birth_date)
                if pid:
                    update_query_part1 = "update sports_participants set country='%s' where id='%s'" % (
                        coun_wiki, pid)
                    self.cursor.execute(update_query_part1)
                    print('PLayer with Same details is already Present')
                    add_source_key(self, pid, player_sk)

                if not pid and pl_exists == False:
                    self.cursor.execute(MAX_ID_QUERY)
                    pl_data = self.cursor.fetchall()
                    max_id, max_gid = pl_data[0]
                    next_id = max_id + 1

                    next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').
                                              replace('PL', '')) + 1)
                    sport_id = SPORT_ID
                    values = (next_id, next_gid, player_name, '', sport_id, 'player', player_image, 200,
                              player_url, loc_id, coun_wiki)
                    self.cursor.execute(PAR_QUERY, values)
                    values1 = (next_id, DEBUT, player_role, role,  'male',
                               pl_age, height, weight, pl_birth_date, birth_place, loc_id, SAL_POP, RATING_POP,
                               WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, height.strip(' cm'), weight.strip(' kg'), '', '')
                    self.cursor.execute(PL_QUERY, values1)

                    add_source_key(self, next_id, player_sk)
                    print(("Added player", player_name))
