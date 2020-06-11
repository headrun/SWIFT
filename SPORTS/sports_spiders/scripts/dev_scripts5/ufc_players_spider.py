from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider, extract_data, get_nodes
import MySQLdb

def create_cursor():
    con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
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
        values = (entity_id, 'participant', 'ufc', _id)

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
entity_type="participant" and source="ufc" and source_key= "%s"'


GAME = 'martial arts'
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



class UfcPlayersSpider(VTVSpider):
    name = "ufc_players"
    allowed_domains = ['www.ufc.com']
    start_urls =  ['http://www.ufc.com/fighter/Weight_Class/']
    record = SportsSetupItem()
    outfile = open("ufc_palyer.txt", 'w')

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[contains(@class, "label")]/a[contains(@href, "/fighter/Weight_Class/")]')
        for node in nodes:
            link = extract_data(node, './@href')
            url = "http://www.ufc.com" + link
            title = extract_data(node, './text()').replace(',', '')
            if url:
                yield Request(url, callback = self.parse_players, \
                                meta = {'title':title})

    def parse_players(self, response):
        hxs = Selector(response)
        title = response.meta['title']
        if "Women" in title:
            gender = 'female'
        else:
            gender = 'male'
        pl_node = get_nodes(hxs, '//table[@class="fighter-listing"]//tr[contains(@class,"fighter")]/td/div//a[contains(@href, "fighter")]')
        for node in pl_node:
            pl_link = extract_data(node, './@href')
            pl_link = "http://www.ufc.com"+pl_link
            if pl_link:
                yield Request(pl_link, callback = self.parse_stats, \
                                meta = {'gender':gender})
        next_page = extract_data(hxs, '//div[@id="page-buttons"]/a[@class="nextLink"]/@href').strip()
        next_page_link = "http://www.ufc.com" + next_page
        if next_page:
            yield Request(next_page_link, callback = self.parse_players, \
                            meta = {'title':title, 'gender':gender})

    def parse_stats(self, response):
        hxs = Selector(response)
        reference = response.url
        gender =  response.meta['gender']
        main_role = ''
        image_link = extract_data(hxs, '//div[@class="fighter-image"]/img/@src')
        aka = extract_data(hxs, '//div[@class="fighter-info"]//table//tr/td[@class="label"]/following::td[@id="fighter-nickname"]/text()')
        age = extract_data(hxs, '//div[@class="fighter-info"]//table//tr/td[@class="label"]/following::td[@id="fighter-age"]/text()').strip()
        height = extract_data(hxs, '//div[@class="fighter-info"]//table//tr/td[@class="label"]/following::td[@id="fighter-height"]/text()').strip()
        height = height.replace("&quot;", '"').split('(')[-1].replace(' ', '').replace(')', '').strip()
        weight = extract_data(hxs,'//div[@class="fighter-info"]//table//tr/td[@class="label"]/following::td[@id="fighter-weight"]/text()').strip()
        weight = weight.split("(")[-1].replace(')', '').strip()
        print height
        print weight
        pl_sk = reference.split("/")[-1]
        pl_title = reference.split("/")[-1].replace("-", ' ').title()

        ref = reference
        con, cursor = create_cursor()
        pl_exists, pl_id = check_player(pl_sk)
        if pl_exists == False:
            pts_id = check_title(pl_title)
            if pts_id:
                add_source_key(str(pts_id[0]), pl_sk)
                print "source_key", pl_title
            else:
                print pl_title
                con, cursor = create_cursor()
                dob = ''
                pos = ''
                cursor.execute(MAX_ID_QUERY)
                pl_data = cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                        replace('PL', '')) + 1)

                values = (next_id, next_gid, pl_title, aka, GAME, PAR_TYPE, image_link, \
                      BASE_POP, ref, LOC)
                cursor.execute(PAR_QUERY, values)
                values = (next_id, DEBUT, pos, ROLES, gender, \
                      age, height, weight, dob, '', SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, '', '')

                cursor.execute(PL_QUERY, values)
                add_source_key(next_id, pl_sk)
                con.close()


