from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
import datetime
import MySQLdb

PL_POS = {'Guards': 'Guard', 'Forwards': 'Forward', 'Centers': 'Center', 'Coach': 'Head coach'}

DOMAIN_LINK = 'http://www.scoresway.com/'

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
        values = (entity_id, 'participant', 'euro_basketball', _id)

        cursor.execute(query, values)
        con.close()

def check_title(self, pl_name):
    birth_date = ''
    name = pl_name
    con, cursor = create_cursor()
    cursor.execute(PL_NAME_QUERY % (name, GAME))
    pl_id = cursor.fetchone()
    if pl_id:
        query = 'select birth_date from sports_players where participant_id=%s'
        cursor.execute(query % (pl_id))
        birth_date = cursor.fetchone()
        if birth_date:
            birth_date = str(birth_date[0])

    return pl_id, birth_date


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
entity_type="participant" and source="euro_basketball" and source_key= "%s"'

GAME = 'basketball'
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


class BundesligaPlayers(VTVSpider):
    name ="bundesliga_players"
    allowed_domains = []
    start_urls = ['http://www.scoresway.com/?sport=basketball&page=competition&id=24']
    participants = {}

    def parse(self, response):
        sel = Selector(response)
        tm_nodes = get_nodes(sel, '//table[@class="leaguetable sortable table "]//tr//td')

        for node in tm_nodes:
            team_link = extract_data(node, './/a//@href')
            if "http" not in team_link:
                team_link = DOMAIN_LINK + team_link

                yield Request(team_link, callback = self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        pl_nodes = get_nodes(sel, '//div[@class="content"]//div[@class="squad-player"]//span[contains(@class, "name")]')

        for pl_node in pl_nodes:
            pl_link = extract_data(pl_node, './/a//@href')
            pl_pos  = extract_list_data(pl_node, '..//preceding-sibling::div[@class="squad-position-title group-head"]//text()')
            if pl_pos:
                pl_pos = pl_pos[-1]
            else:
                pl_pos = ''

            pl_pos  = PL_POS.get(pl_pos, '')
            pl_sk = pl_link.split('=')[-1].strip()

            if "http" not in pl_link:
                pl_link = DOMAIN_LINK + pl_link
                yield Request(pl_link, callback = self.parse_details, meta = {'pl_sk': pl_sk, 'pl_pos': pl_pos})


    def parse_details(self, response):
        sel = Selector(response)
        pl_sk = response.meta['pl_sk']
        pl_sk = "PL" + pl_sk
        pl_pos = response.meta['pl_pos']
        pl_name = extract_list_data(sel, '//div[@class="content-column"]//h2//text()')[0]
        pl_name = pl_name.split('|')[0].strip()
        pl_dob  = extract_list_data(sel, '//div[@class="clearfix"]//dt[contains(text(), "Date of birth")]//following-sibling::dd/text()')
        if pl_dob:
            pl_dob = pl_dob[0]
        else:
            pl_dob = ''
        pl_age  = extract_list_data(sel, '//div[@class="clearfix"]//dt[contains(text(), "Age")]//following-sibling::dd/text()')
        if pl_age:
            pl_age = pl_age[0]
        else:
            pl_age = ''
        pl_height = extract_list_data(sel, '//div[@class="clearfix"]//dt[contains(text(), "Height")]//following-sibling::dd/text()')
        if pl_height:
            pl_height = pl_height[0]
        else:
            pl_height = ''
        pl_weight = extract_list_data(sel, '//div[@class="clearfix"]//dt[contains(text(), "Weight")]//following-sibling::dd/text()')
        if pl_weight:
            pl_weight = pl_weight[0]
        else:
            pl_weight = ''
        pl_img = extract_data(sel, '//div[@class="yui-u"]//img//@src')
        pl_place = extract_list_data(sel, '//div[@class="clearfix"]//dt[contains(text(), "Place of birth")]//following-sibling::dd/text()')
        if pl_place:
            pl_place = pl_place[0]
            city = pl_place.split(',')[0].strip()
        else:
            city = ''
        if pl_dob:
            dt = datetime.datetime.strptime(pl_dob, "%d-%m-%y")
            birth_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            birth_date = '0000-00-00'
        con, cursor = create_cursor()
        loc_id = ''
        loc_id = 'select id from sports_locations where city = "%s" limit 1' %(city)
        cursor.execute(loc_id)
        loc_id = cursor.fetchall()
        if loc_id:
            loc_id = str(loc_id[0][0])
        if loc_id == ():
            loc_id = ''
        pl_exists, pl_id = check_player(pl_sk)

        if pl_exists == False:
            pts_id, db_birth_date = check_title(self, pl_name)

            if pts_id and str(pts_id[0]) and db_birth_date == birth_date:
                add_source_key(str(pts_id[0]), pl_sk)
                print "add_source_key", pl_name
            elif pts_id and str(pts_id[0]) and db_birth_date != birth_date:
                print "birth_date note matched"
                return

            else:
                print "add_players", pl_name
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

