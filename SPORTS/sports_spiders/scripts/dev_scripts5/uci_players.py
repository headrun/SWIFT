from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime
import MySQLdb



def create_cursor():
    con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
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
        values = (entity_id, 'participant', 'eurosport_cycling', _id)

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
entity_type="participant" and source="eurosport_cycling" and source_key= "%s"'


GAME = 'cycling'
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

class EuroCyclingPlayer(VTVSpider):
    name = "eurocyclinguci_player"
    start_urls = ['http://www.eurosport.com/cycling/usa-pro-challenge/2015/standing.shtml']

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="content standings active"]//tr//td[@class="player"]//a')
        for node in nodes:
            url = extract_data(node, './/@href')
            if "http" not in url:
                url = "http://www.eurosport.com" + url
                yield Request(url, callback = self.parse_next)


    def parse_next(sel, response):
        sel = Selector(response)
        ref_url = response.url
        pl_sk = ref_url.split('/')[-2].split('_')[0]
        pl_img = extract_data(sel, '//div[@class="player-img-container"]//img//@src')
        pl_name = extract_data(sel, '//div[@class="personal_data overview_data"]//ul//li//span[contains(text(), "Full name")]//following-sibling::span//text()').strip()
        if pl_name == "Jonathon Clarke":
            pl_name = "Jonathan Clarke"
        pl_country = extract_data(sel, '//div[@class="personal_data overview_data"]//ul//li//span[contains(text(), "Country")]//following-sibling::span//text()').strip()
        pl_country = pl_country.replace('United States', 'USA')
        pl_age = extract_data(sel, '//div[@class="personal_data overview_data"]//ul//li//span[contains(text(), "Age")]//following-sibling::span//text()').strip()
        pl_dob = extract_data(sel, '//div[@class="personal_data overview_data"]//ul//li//span[contains(text(), "Date of birth")]//following-sibling::span//text()').strip()
        pl_bop = extract_data(sel, '//div[@class="personal_data overview_data"]//ul//li//span[contains(text(), "Place of birth")]//following-sibling::span//text()').strip()
        if "," in pl_bop:
            pl_bop = pl_bop.split(',')[0].strip()

        height = extract_data(sel, '//div[@class="personal_data overview_data"]//ul//li//span[contains(text(), "Height")]//following-sibling::span//text()').strip()
        weight = extract_data(sel, '//div[@class="personal_data overview_data"]//ul//li//span[contains(text(), "Weight")]//following-sibling::span//text()').strip()

        birth_date = ''
        if pl_dob:
            dt = datetime.datetime.strptime(pl_dob, "%B %d %Y")
            birth_date = dt.strftime("%Y-%m-%d %H:%M:%S")

        loc_id = ''
        con, cursor = create_cursor()
        if pl_country:
            loc_id = 'select id from sports_locations where city ="%s" and country = "%s" limit 1' %(pl_bop, pl_country)
            cursor.execute(loc_id)
            loc_id = cursor.fetchall()
            if not loc_id:
                loc_id = 'select id from sports_locations where country ="%s" and city = "" and state = "" limit 1' %(pl_country)
                cursor.execute(loc_id)
                loc_id = cursor.fetchall()
            if not loc_id:
                loc_id = 'select id from sports_locations where city ="%s" limit 1' %(pl_bop)
                cursor.execute(loc_id)
                loc_id = cursor.fetchall()

            if loc_id:
                loc_id = str(loc_id[0][0])

        pl_exists, pl_id = check_player(pl_sk)
        if pl_exists == False:
            pts_id = check_title(pl_name)
            if pts_id:
                add_source_key(str(pts_id[0]), pl_sk)
                print "add source key", pl_name
            else:
                print "add player", pl_name
                cursor.execute(MAX_ID_QUERY)
                pl_data = cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                        replace('PL', '')) + 1)

                values = (next_id, next_gid, pl_name, AKA, GAME, PAR_TYPE, pl_img, \
                      BASE_POP, ref_url, loc_id)
                cursor.execute(PAR_QUERY, values)
                values = (next_id, DEBUT, '', ROLES, GENDER, \
                      pl_age, height, weight, birth_date, pl_bop, SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE)

                cursor.execute(PL_QUERY, values)
                add_source_key(next_id, pl_sk)
                con.close()

