from scrapy.selector import Selector
from scrapy.http import Request
from sports_spiders.vtvspider import VTVSpider, get_nodes,\
    extract_data, extract_list_data, get_height, get_weight,\
    get_player_details, get_birth_place_id, get_sport_id, get_country
import re
import datetime
import time
import MySQLdb
import urllib.request
import urllib.parse
import urllib.error

TEAM_LIST = ('NE', 'BAL', 'CIN', 'CLE', 'CHI', 'DET',
             'GB', 'MIN', 'HOU', 'IND', 'JAC', 'TEN',
             'ATL', 'CAR', 'NO', 'TB', 'BUF', 'MIA',
             'DAL', 'NYG', 'PHI', 'NYJ', 'WAS', 'DEN',
             'KC', 'OAK', 'SD', 'ARI', 'SF', 'SEA', 'STL', 'PIT')


ROLES_DICT = {'C': 'Center', 'G': 'Guard', 'T': 'Tackle',
              'QB': 'Quarterback', 'RB': 'Running Back',
              'WR': 'Wide Receiver', 'TE': 'Tight End',
              'DT': 'Defensive Tackle', 'DE': 'Defensive End',
              'MLB': 'Middle Linebacker', 'OLB': 'Outside Linebacker',
              'CB': 'Cornerback', 'S': 'Safety', 'K': 'Kicker',
              'H': 'Holder', 'LS': 'Long Snapper',
              'P': 'Punter', 'PR': 'Punt Returner', 'KR': 'Kick Returner',
              'FB': 'Fullback', 'HB': 'Halfback', 'ILB': 'Inside Linebacker',
              'TB': 'Tailback', 'RG': 'Right Guard', 'LG': 'Left Guard',
              'RT': 'Right Tackle', 'LT': 'Left Tackle', 'NG': 'Nose Guard',
              'DL': 'Defensive Line', 'FS': 'Free Safety', 'LB': 'Linebacker',
              'NT': 'Defensive Tackle', 'DB': 'Defensive Back', 'PK': 'Placekicker',
              'SS': 'Strong Safety', 'OG': 'Offensive Guard',
              'OT': 'Offensive Tackle', 'OL': 'Offensive Line',
              'SAF': 'Safety'}


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


PAR_QUERY = "insert into sports_participants (id, gid, title, aka, sport_id, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, birth_place_id, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at, short_title, display_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, %s, now(), now(), %s, %s) on duplicate key update modified_at = now();"

MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'


def get_player_role(position):
    for key, value in list(ROLES_DICT.items()):
        if position == key:
            position = value
            return position


def check_player(self, pl_sk):
    self.cursor.execute(SK_QUERY % pl_sk)
    entity_id = self.cursor.fetchone()
    if entity_id:
        pl_id = str(entity_id[0])
        pl_exists = True
    else:
        pl_id = ''
        pl_exists = False

    return pl_exists, pl_id


def check_title(self, name, dob):
    name = name
    self.cursor.execute(PL_NAME_QUERY % (name, SPORT_ID, dob))
    pl_id = self.cursor.fetchone()
    if not pl_id:
        dob = "0000-00-00 00:00:00"
        self.cursor.execute(PL_NAME_QUERY % (name, SPORT_ID, dob))
        pl_id = self.cursor.fetchone()
    return pl_id


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


def add_source_key(self, entity_id, _id):
    if _id and entity_id:
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'NFL', _id)

        self.cursor.execute(query, values)


GAME = 'american football'
SPORT_ID = '4'
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

# PL_NAME_QUERY = 'select id from sports_participants where \
# title like "%s" and game="%s" and participant_type="player"';

PL_NAME_QUERY = 'select P.id from sports_participants P, sports_players PL where P.title="%s" and P.sport_id="%s" and P.id=PL.participant_id and PL.birth_date="%s"'


class NflPlayersss(VTVSpider):
    name = "nfl_playersss"
    start_urls = []

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo",
                                    passwd='veveo123', db="SPORTSDB", charset='utf8', use_unicode=True)
        #self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.today = datetime.datetime.now().date()

    def start_requests(self):
        st_url = ['http://www.nfl.com/teams/roster?team=%s']
        for url in st_url:
            for team in TEAM_LIST:
                t_url = url % (team)
                yield Request(t_url, callback=self.parse,
                              meta={'team_sk': team})

    def populate_player(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs,
                          '//div[@id="player-profile"]//div[@class="player-info"]')
        pl_link = response.meta['pl_link']
        pl_sk = response.meta['pl_sk']
        pl_exists, pl_id = check_player(self, pl_sk[0])
        status_remarks = standing = seed = status = ''
        player_role = response.meta['pos']

        for node in nodes:
            name = extract_data(node, './p/span[@class="player-name"]/text()')
            team = extract_data(node, './p/a[contains(@href, "team")]/@href').\
                split('=')[-1]
            pl_res = extract_list_data(node, './p[3]/text()')
            img = extract_data(node,
                               './preceding-sibling::div[@class="player-photo"]//@src')
            if len(pl_res) >= 4:
                pl_height, pl_weight, age = get_player_res((pl_res[1],
                                                            pl_res[2], pl_res[3]))
            else:
                pl_height, pl_weight = get_player_res((pl_res[1], pl_res[2]))
                age = ''
            up_weight = ''
            up_height = ''

            if pl_weight:
                up_weight = get_weight(lbs=pl_weight)
            if pl_height:
                update_ft = pl_height.split("-")[0].strip()
                update_in = pl_height.split("-")[1].strip()
                up_height = get_height(feets=update_ft, inches=update_in)

            pl_born = extract_list_data(node, './p[4]/text()')
            college = extract_list_data(
                node, './p[5]/text()')[0].replace(':', '').strip()

            country = dob = b_place = pl_state = birth_place = loc_id = ''
            if len(pl_born) == 2:
                pl_born = pl_born[1].split(',')
                if len(pl_born) == 2:
                    dob_list = get_player_dob((pl_born[0], pl_born[1]))
                    dob, b_place = dob_list[0], pl_born[0].replace(
                        ':', '').strip().split(' ')[1:]
                    b_place = " ".join(b_place).strip()
                    state = REPLACE_STATE_DICT.get(pl_born[1].strip())
                    if not state:
                        pl_state = pl_born[1].strip()
                    else:
                        pl_state = state

                else:
                    dob = pl_born[0].replace(': ', '').split(' ')[0]
                    b_place = ''
                    pl_state = ''
                dt = datetime.datetime.strptime(dob.strip(), "%m/%d/%Y")
                dob = dt.strftime("%Y-%m-%d %H:%M:%S")

            if b_place and pl_state:
                try:
                    country = get_country(city=b_place, state=pl_state)
                except:
                    pass
                if country:
                    data = b_place, pl_state, country
                    birth_place = ", ".join(data)
                else:
                    birth_place = ""

            if b_place:
                loc_id = get_birth_place_id(b_place, pl_state, country)

            if pl_exists == True:
                details = {'age': age, 'birth_place': birth_place,
                           'height': up_height, 'weight': up_weight,
                           'loc_id': loc_id, 'ref_url': response.url,
                           'birth_date': dob, 'sport_id': '4'}  # 'game': 'american football', 'sport_id': '4'}

                get_player_details(details, pl_id)

            pl_ids = check_title(self, name, dob)
            if pl_ids and pl_exists == False:
                if len(pl_ids) == 1:
                    add_source_key(
                        self, str(pl_ids[0]), response.meta['pl_sk'][0])
                    print(("Added source key", name))
                    continue

            if pl_exists == False and not pl_ids:
                self.cursor.execute(MAX_ID_QUERY)
                pl_data = self.cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').
                                          replace('PL', '')) + 1)
                #sport_id = get_sport_id(GAME)
                values = (next_id, next_gid, name, '', SPORT_ID, 'player', img, 200,
                          response.url, '')
                self.cursor.execute(PAR_QUERY, values)
                values = (next_id, DEBUT, player_role, ROLES, 'male',
                          age, up_height, up_weight, dob, birth_place, loc_id, SAL_POP, RATING_POP,
                          WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, '', '')

                self.cursor.execute(PL_QUERY, values)
                add_source_key(self, next_id, response.meta['pl_sk'][0])
                print(("Added player", name))

    def parse(self, response):
        season = datetime.datetime.now().year
        hxs = Selector(response)
        participants = {}

        nodes = get_nodes(hxs,
                          '//div[@id="team-stats-wrapper"]//table//tbody//tr')
        for node in nodes:
            link = extract_data(node, './td/a/@href').strip()
            pl_sk = (re.findall(r'/(\d+)/profile', link))
            if "http" not in link:
                pl_link = "http://www.nfl.com" + link
            else:
                pl_link = link
            position = extract_data(node, './td[3]/text()')
            position = get_player_role(position)

            yield Request(pl_link, self.populate_player,
                          meta={'pos': position, 'pl_sk': pl_sk,
                                'pl_link': pl_link})
