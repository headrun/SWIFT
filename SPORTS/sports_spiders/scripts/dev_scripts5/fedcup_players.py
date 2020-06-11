from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, get_nodes
from scrapy_spiders_dev.items import SportsSetupItem
import datetime
import MySQLdb

def create_cursor():
    con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
    cursor = con.cursor()
    return con, cursor


def add_source_key(entity_id, _id):
    if _id and entity_id:
        con, cursor = create_cursor()
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now())"
        values = (entity_id, 'participant', 'fedcup_tennis', _id)

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
            participant_since, competitor_since, created_at, modified_at) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now();"

MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'


PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"';

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="fedcup_tennis" and source_key= "%s"'


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

GAME = 'tennis'
PAR_TYPE = 'player'
BASE_POP = "200"
LOC = '0'
DEBUT = "0000-00-00"
ROLES = ''
SAL_POP = ''
RATING_POP = ''
GENDER = 'female'
MARITAL_STATUS = ''
PAR_SINCE = COMP_SINCE = ''
WEIGHT_CLASS = AKA = ''

class FedCupPlayer(VTVSpider):
    name = "fedcup_player"
    #start_urls = ['http://www.fedcup.com/en/teams/teams-a-to-z.aspx']
    start_urls = []
    participants = {}
    f1 = open('fedplayer.txt', 'r')
    for url in f1:
        pl_url = "http://www.fedcup.com" + url
        start_urls.append(pl_url.strip())

    '''def parse(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        nodes =  get_nodes(hxs, '//div[@class="az"]/ul//li//ul[@class="azlist clfx"]/li/a')
        for node in nodes:
            team_link = extract_data(node, './@href')
            team_link = "http://www.fedcup.com" + team_link
            team_sk    = team_link.rsplit('?id=')[-1]
            yield Request(team_link, callback=self.parse_team_info, meta = {'record': record})'''

    '''def parse_team_info(self, response):
        hxs = Selector(response)
        record = response.meta['record']
        team_link  = response.url
        team_sk    = team_link.rsplit('?id=')[-1]
        season = extract_data(hxs, '//span[@class="ajax__tab_inner"]/span[@id="__tab_TabRecentResultsThisYear"]/text()').strip()
        year = datetime.date.today().year
        player_nodes = get_nodes(hxs, '//div[@id="TabRecentResultsThisYear"]//table//tr//td/a[contains(@href, "/players/player")]')
        for pl_node in player_nodes:

            pl_link = "http://www.fedcup.com" + extract_data(pl_node, './@href').strip()
            if pl_link and int(year) == int(season):
                yield Request(pl_link, callback=self.parse_player_info, meta = {})'''

    def parse(self, response):
        hxs = Selector(response)
        players = {}
        pl_link = response.url
        player_sk = pl_link.rsplit('playerid=')[-1]
        player_title = extract_data(hxs, '//div[@class="clfx playerProfile"]/h1/text()').strip().title()
        player_title = player_title.encode('utf-8')
        if "Klaudia Jans" in player_title:
            player_title = "Klaudia Jans"
        if player_title == "Yen-Hsun Lu":
            player_title = "Lu Yen-hsun"
        if player_title == "Yung-Jan Chan":
            player_title = "Chan Yung-jan"
        if player_title == "Su-Wei Hsieh":
            player_title = "Hsieh Su-wei"
        pl_image = "http://www.fedcup.com" + extract_data(hxs, '//div[@id="playerHS"]/img[@id="webImagePlayer"]/@src').strip()
        dt = extract_data(hxs, '//div[@class="playerDetails"]/ul/li/span[contains(text(), "Date of birth")]/following-sibling::strong/text()').strip()

        if dt :
            yr = dt.split(' ')[-1]
            if int(yr) <= 1900:
                birth_date = ''
            else:
                dt = datetime.datetime.strptime(dt, "%d %b %Y")
                birth_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            birth_date = ''
        place = extract_data(hxs, '//div[@class="playerDetails"]/ul/li/span[contains(text(), "Birth place:")]/following-sibling::strong/text()').strip()
        role = extract_data(hxs, '//div[@class="playerDetails"]/ul/li/span[contains(text(), "Plays:")]/following-sibling::strong/text()').strip()
        if role.lower() == "unknown":
            role = ''
        else:
            role = role.split(' (')[0].strip()
            if 'left' in role.lower():
                role = 'Left Handed'
            elif 'right' in role.lower():
                role = 'Right Handed'

        if len(place.split(',')) == 2:
                city = place.split(',')[0].strip()
                country = place.split(',')[-1].strip()
                con, cursor = create_cursor()
                loc_id = 'select id from sports_locations where city ="%s" and country = "%s" limit 1' %(city, country)
                cursor.execute(loc_id)
                loc_id = cursor.fetchall()
                if not loc_id:
                    loc_id = 'select id from sports_locations where country ="%s" limit 1' %(country)
                    cursor.execute(loc_id)
                    loc_id = cursor.fetchall()
                if not loc_id:
                    loc_id = 'select id from sports_locations where city ="%s" limit 1' %(city)
                    cursor.execute(loc_id)
                    loc_id = cursor.fetchall()

                if loc_id:
                    loc_id = str(loc_id[0][0])
        else:
            loc_id = ''
        if loc_id == ():
            loc_id = ''
        con, cursor = create_cursor()
        pl_exists, pl_id = check_player(player_sk)
        if pl_exists == False:
            pts_id = check_title(player_title)
            if pts_id:
                add_source_key(str(pts_id[0]), player_sk)
                print "Added sk", player_sk
            else:
                print "added_palyer", player_title
                cursor.execute(MAX_ID_QUERY)
                pl_data = cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                        replace('PL', '')) + 1)

                values = (next_id, next_gid, player_title, AKA, GAME, PAR_TYPE, pl_image, \
                      BASE_POP, response.url, loc_id)
                cursor.execute(PAR_QUERY, values)

                values = (next_id, DEBUT, role, ROLES, GENDER, \
                      '', '', '', birth_date, place, SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE)

                cursor.execute(PL_QUERY, values)

                add_source_key(next_id, player_sk)
                con.close()

