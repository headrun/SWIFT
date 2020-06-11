from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime
import MySQLdb

COUNTRY_URL = "http://www.espncricinfo.com/ci/content/current/player/"

PL_LINK = "http://www.espncricinfo.com"

def create_cursor():
    con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = con.cursor()
    return con, cursor


def check_player(player_id):
    con, cursor = create_cursor()
    cursor.execute(SK_QUERY % player_id)
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
        values = (entity_id, 'participant', 'espn_cricket', _id)

        cursor.execute(query, values)
        con.close()

def check_title(name):
    con, cursor = create_cursor()
    cursor.execute(PL_NAME_QUERY % (name, GAME))
    pl_id = cursor.fetchone()
    con.close()
    return pl_id

def update_pl_location(pl_id, loc_id):
    con, cursor = create_cursor()
    values = (loc_id, pl_id)
    cursor.execute(LOC_QUERY % values)
    con.close()

LOC_QUERY = 'update sports_participants set location_id = %s where location_id = "" and id = %s and participant_type = "player" and game="cricket"'

PAR_QUERY = "insert into sports_participants (id, gid, title, aka, game, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at, display_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, now(), now(), %s) on duplicate key update modified_at = now();"
MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'


PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"';

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="espn_cricket" and source_key= "%s"'


GAME = 'cricket'
PAR_TYPE = 'player'
BASE_POP = "200"
LOC = '0'
DEBUT = "0000-00-00"
ROLES = ''
GENDER = "male"
SAL_POP = ''
RATING_POP = ''
MARITAL_STATUS = ''
PAR_SINCE = COMP_SINCE = ''
WEIGHT_CLASS = AKA = ''

class CricketPlayers(VTVSpider):
    name = "cricket_players"
    start_urls = ['http://www.espncricinfo.com/ci/content/current/player/index.html?']
    participants = {}


    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="ciPlayersHomeCtryList"]/ul//li')
        for node in nodes:
            country = extract_data(node, './a/text()')
            link = extract_data(node, './a/@href')
            country_url = COUNTRY_URL + link
            if link:
                yield Request(country_url, callback = self.parse_next, \
                            meta = {})

    def parse_next(self, response):
        hxs = Selector(response)
        players_li = get_nodes(hxs, '//div[@id="rectPlyr_Playerlistall"]\
                     //table[@class="playersTable"]//tr//td/a')
        for node in players_li[::1]:
            player_url = PL_LINK + extract_data(node, './@href')
            yield Request(player_url, callback = self.parse_players, \
                          meta = {})

    def parse_players(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        player_id = re.findall(r'player.(\d+).', response.url)[0]
        player_role = extract_data(hxs, '//b[contains(text(), "Playing role")] \
                        //following-sibling::span//text()')
        player_name = extract_data(hxs, '//b[contains(text(), "Full name")] \
                        //following-sibling::span//text()').strip()
        player_age = extract_data(hxs, '//b[contains(text(), "Current age")] \
                    //following-sibling::span//text()').split('years')[0].strip()
        player_img = extract_data(hxs, '//div[@style="float:left; margin-bottom:15px; width:160px;"]//img//@src')
        pl_bplace = extract_data(hxs, '//b[contains(text(), "Born")]//following-sibling::span//text()')
        if player_img:
            player_img = "http://www.espncricinfo.com" + player_img
        else:
            player_img = ''
        display_title = extract_data(hxs, '//div[@style="margin:0; float:left; padding-bottom:3px;"]//h1/text()').strip()
        con, cursor = create_cursor()
        pl_exists, pl_id = check_player(player_id)
        if pl_exists == True:
            if len(pl_bplace.split(",")) >= 3:
                city = pl_bplace.split(',')[2].strip().replace("'", '').strip()
                if city:
                    loc_id = 'select id from sports_locations where city ="%s" limit 1' %(city)
                    cursor.execute(loc_id)
                    loc_id = cursor.fetchall()
                    if loc_id:
                        loc_id = str(loc_id[0][0])
                        pl_id = 'select entity_id from sports_source_keys where source="espn_cricket" and entity_type ="participant" and source_key = "%s" limit 1' %(player_id)
                        cursor.execute(pl_id)
                        pl_id = cursor.fetchall()
                        update_pl_location(str(pl_id[0][0]), loc_id)
                    else:
                        f = open('missing_loc_cricket', 'w')
                        f.write('%s\t\%s\n'%(player_name, pl_bplace))
 
            pts_id = check_title(player_name)
            if pts_id:
                print player_name
                #add_source_key(str(pts_id[0]), player_id)
            else:
                print player_name
                img = ''
                cursor.execute(MAX_ID_QUERY)
                pl_data = cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                        replace('PL', '')) + 1)

                values = (next_id, next_gid, player_name, AKA, GAME, PAR_TYPE, player_img, \
                      BASE_POP, response.url, LOC)
                #cursor.execute(PAR_QUERY, values)
                values = (next_id, DEBUT, player_role, ROLES, GENDER, \
                      player_age, '', '', '', '', SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, display_title)

                #cursor.execute(PL_QUERY, values)
                #add_source_key(next_id, player_id)
                con.close()

