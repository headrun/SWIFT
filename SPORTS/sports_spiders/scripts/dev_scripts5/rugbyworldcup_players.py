from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
import datetime
import MySQLdb

DOMAIN_LINK = "http://www.rugbyworldcup.com"


def create_cursor():
    con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
    #con  = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
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
        values = (entity_id, 'participant', 'rugby_worldcup', _id)

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
entity_type="participant" and source="rugby_worldcup" and source_key= "%s"'

GAME = 'rugby union'
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


class RugbyWorldCupPlayers(VTVSpider):
    name = "worldrugby_players"
    start_urls = ['http://www.rugbyworldcup.com/teams']

    def parse(self, response):
        sel = Selector(response)
        team_nodes = get_nodes(sel, '//div[@class="teamsListIndex"]//ul//li//a')

        for node in team_nodes:
            team_link = extract_data(node, './/@href')

            if "http" not in team_link:
                team_link = DOMAIN_LINK + team_link
            team_link = team_link + "/squad"

            yield Request(team_link, callback=self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        pl_nodes = get_nodes(sel, '//section[@class="team-squad"]//ul//li')

        for pl_node in pl_nodes:
            pl_link = extract_data(pl_node, './/a//@href')

            if 'http' not in pl_link:
                pl_link = DOMAIN_LINK + pl_link

            pl_pos = extract_data(pl_node, './/span[@class="tag"]//text()')
            pl_name = extract_data(pl_node, './/h3[@class="name"]//text()')
            yield Request(pl_link, callback = self.parse_players, \
                meta = {'pl_pos': pl_pos, 'pl_name': pl_name})


    def parse_players(self, response):
        sel = Selector(response)
        pl_sk = response.url.split('/')[-1].strip()
        tm_name = response.url.split('/')[-3].strip().replace('-', ' ')
        pl_name = extract_data(sel, '//div[@class="playerSummary"]//h2[@class="name"]//text()').strip()

        if not pl_name:
            pl_name = response.meta['pl_name']

        pl_img = extract_data(sel, '//div[@class="playerHeadshot header"]//img//@src')
        pl_pos = extract_data(sel, '//div[@class="playerStatsHeader"]//h4[contains(text(), "Position")]//following-sibling::h4//text()')
        pl_dob = extract_data(sel, '//div[@class="playerStatsHeader"]//h4[contains(text(), "Age")]/../p//text()')
        pl_age = extract_data(sel, '//div[@class="playerStatsHeader"]//h4[contains(text(), "Age")]//following-sibling::h4//text()')
        pl_height = extract_data(sel, '//div[@class="playerStatsHeader"]//h4[contains(text(), "Height")]//following-sibling::h4//text()')
        pl_weight = extract_data(sel, '//div[@class="playerStatsHeader"]//h4[contains(text(), "Weight")]//following-sibling::h4//text()')

        if pl_dob:
            dt = datetime.datetime.strptime(pl_dob, "%d %b %Y")
            birth_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            birth_date = ''
        con, cursor = create_cursor()
        loc_id = ''
        loc_id = 'select id from sports_locations where city ="" and state = "" and country = "%s" limit 1' %(tm_name)
        cursor.execute(loc_id)
        loc_id = cursor.fetchall()
        if loc_id:
            loc_id = str(loc_id[0][0])
        if loc_id == ():
            loc_id = ''
        pl_exists, pl_id = check_player(pl_sk)

        if pl_exists == False:
            pts_id = check_title(pl_name)

            if pts_id:
                add_source_key(str(pts_id[0]), pl_sk)

            else:
                con, cursor = create_cursor()
                cursor.execute(MAX_ID_QUERY)
                pl_data = cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                        replace('PL', '')) + 1)


                values = (next_id, next_gid, pl_name, '', GAME, PAR_TYPE, pl_img, \
                      BASE_POP, response.url, loc_id)
                cursor.execute(PAR_QUERY, values)

                values = (next_id, DEBUT, pl_pos, ROLES, 'male', \
                      pl_age, pl_height, pl_weight, birth_date, '', SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE)

                cursor.execute(PL_QUERY, values)

                add_source_key(next_id, pl_sk)
                con.close()

