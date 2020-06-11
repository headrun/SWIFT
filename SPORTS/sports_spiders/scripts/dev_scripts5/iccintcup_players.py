from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime
import MySQLdb


PL_LINK = "http://www.espncricinfo.com"

def create_cursor():
    con = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
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

class ICCIntCupPlayers(VTVSpider):
    name = "iccintcup_players"
    start_urls = ['http://www.espncricinfo.com/indian-premier-league-2016/content/squad/index.html?object=968923']
    #start_urls =  ['http://www.espncricinfo.com/wcl-championship-2015-17/content/squad?object=870869']
    def parse(self, response):
        sel = Selector(response)
        rs_nodes = get_nodes(sel, '//div[@role="main"]//ul[@class="squads_list"]//li')
        for node in rs_nodes:
            team_link = extract_data(node, './/@href')
            title = extract_data(node, './/text()').split(' Squad')[0]
            if "http" not in team_link:
                team_link = PL_LINK + team_link
            yield Request(team_link, callback = self.parse_next, meta = {'title': title})


    def parse_next(self, response):
        sel = Selector(response)
        team_sk = response.meta['title']
        record = SportsSetupItem()
        pl_nodes = get_nodes(sel, '//div[@class="content main-section"]//ul//li')
        for pl_node in pl_nodes:
            pl_link = extract_list_data(pl_node, './/a[contains(@href, "player")]//@href')
            if pl_link:
                pl_link = pl_link[0]
                player_url = PL_LINK + pl_link
                yield Request(player_url, callback = self.parse_players, \
                          meta = {})

    def parse_players(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        player_id = re.findall(r'player.(\d+).', response.url)[0]
        player_role = extract_data(hxs, '//b[contains(text(), "Playing role")] \
                        //following-sibling::span//text()')

        if "Wicketkeeper" in player_role:
            player_role = "Wicket-keeper"
        if "Allrounder" in player_role:
            player_role = "All-rounder"

        player_name = extract_data(hxs, '//b[contains(text(), "Full name")] \
                        //following-sibling::span//text()').strip()
        player_name = player_name.replace('\t', ' ').strip()
        player_age = extract_data(hxs, '//b[contains(text(), "Current age")] \
                    //following-sibling::span//text()').split('years')[0].strip()
        player_img = extract_data(hxs, '//div[@style="float:left; margin-bottom:15px; width:160px;"]//img//@src')
        pl_bplace = extract_data(hxs, '//b[contains(text(), "Born")]//following-sibling::span//text()')
        pl_dob = "".join(pl_bplace.split(',')[:2])
        pl_bplce = pl_bplace.split(',')[2:]

        try:
            if pl_dob:
                dt = datetime.datetime.strptime(pl_dob, "%B %d %Y")
                birth_date = dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                birth_date = ''
        except:
            birth_date = ''

        if len(pl_bplce) == 2:
            city = pl_bplce[0]
            country = pl_bplce[1]
        elif len(pl_bplce) == 1:
            country = pl_bplce[0]
            city = ''
        else:
            city = ''
            country = ''
        loc_id = ''
        con, cursor = create_cursor()
        if pl_bplce:
            loc_id = 'select id from sports_locations where city ="%s" and state = "" and country = "%s" limit 1' %(city, country)
            cursor.execute(loc_id)
            loc_id = cursor.fetchall()
            if loc_id:
                loc_id = str(loc_id[0][0])
        if loc_id == ():
            loc_id = ''

        if player_img:
            player_img = "http://www.espncricinfo.com" + player_img
        else:
            player_img = ''
        display_title = extract_data(hxs, '//div[@style="margin:0; float:left; padding-bottom:3px;"]//h1/text()').strip()
        pl_exists, pl_id = check_player(player_id)
        if pl_exists == False:
            pts_id = check_title(player_name)
            if pts_id:
                add_source_key(str(pts_id[0]), player_id)
                print "add_sourcekey", player_name
            else:
                img = ''
                print "add_player", player_name
                cursor.execute(MAX_ID_QUERY)
                pl_data = cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                        replace('PL', '')) + 1)

                values = (next_id, next_gid, player_name, AKA, GAME, PAR_TYPE, player_img, \
                      BASE_POP, response.url, loc_id)
                cursor.execute(PAR_QUERY, values)
                values = (next_id, DEBUT, player_role, ROLES, GENDER, \
                      player_age, '', '', birth_date, '', SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, display_title, short_title)

                cursor.execute(PL_QUERY, values)
                add_source_key(next_id, player_id)
                con.close()

