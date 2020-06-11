import MySQLdb
import datetime
from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_new import VTVSpider, get_nodes, extract_data, extract_list_data, get_height, get_weight
from scrapy_spiders_dev.items import SportsSetupItem

DOMAIN_URL = "http://www.baseballamerica.com"

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
        values = (entity_id, 'participant', 'baseball_america', _id)

        cursor.execute(query, values)
        con.close()

def check_title(name, dob):
    con, cursor = create_cursor()
    cursor.execute(PL_NAME_QUERY % (name, GAME, dob))
    pl_id = cursor.fetchone()
    con.close()
    return pl_id, birth_date


PAR_QUERY = "insert into sports_participants (id, gid, title, aka, game, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"


PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at, short_title, display_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, now(), now(), %s, %s) on duplicate key update modified_at = now();"
MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'


PL_NAME_QUERY = 'select P.id from sports_participants P, sports_players PL where P.title="%s" and P.game="%s" and P.id=PL.participant_id and PL.birth_date="%s"'

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="baseball_america" and source_key= "%s"'

GAME = 'baseball'
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

PL_POS = {'Pitchers': 'Pitcher', 'Catchers': 'Catcher', \
            'Infielders': 'Infielder', 'Outfielders': 'Outfielder', \
            'Injuries': '', 'Designated Hitters': 'Designated hitter'}

class MexBaseballPlayers(VTVSpider):
    name = "mex_players"
    allowed_domains = []
    start_urls = ['http://www.baseballamerica.com/statistics/leagues/?lg_id=17']
    participants = {}

    def parse(self, response):
        sel = Selector(response)
        tm_nodes  = get_nodes(sel, '//table//tr//td//a[contains(@href, "roster")]')

        for tm_node in tm_nodes:
            tm_link = extract_data(tm_node, './/@href')

            if "http" not in tm_link:
                tm_link = DOMAIN_URL + tm_link

            yield Request(tm_link, callback = self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        pl_nodes = get_nodes(sel, '//table//tr')

        for node in pl_nodes:
            pl_link = extract_data(node, './/td//a[contains(@href, "player")]//@href')

            if not pl_link or 'tm_id' in pl_link:
                continue

            pl_pos = extract_list_data(node, './preceding-sibling::tr[@class="headerRow"]//td//text()')[-1]
            pl_pos = PL_POS.get(pl_pos, '')
            pl_sk = pl_link.split('/')[-1]
            pl_name = extract_data(node, './/td//a[contains(@href, "player")]//text()')
            pl_height = extract_data(node, './/td[5]//text()')
            pl_weight = extract_data(node, './/td[6]//text()')
            pl_age    = extract_data(node, './/td[7]//text()')
            birth_date = extract_data(node, './/td[8]//text()')

            if "http" not in pl_link:
                pl_link = DOMAIN_URL + pl_link

            yield Request(pl_link, callback=self.parse_players, \
            meta = {'pl_sk': pl_sk, 'pl_pos': pl_pos, 'pl_name': pl_name, \
            'pl_height': pl_height, 'pl_weight': pl_weight, \
            'pl_age': pl_age, 'birth_date': birth_date})

    def parse_players(self, response):
        sel = Selector(response)
        pl_sk      = response.url.split('/')[-1]
        pl_name     = response.meta['pl_name']
        pl_height   = response.meta['pl_height']
        pl_weight  = response.meta['pl_weight']
        pl_age     = response.meta['pl_age']
        pl_pos     = response.meta['pl_pos']
        pl_dob = response.meta['birth_date']
        if pl_height:
            feets = pl_height.split('-')[0].strip()
            inches = pl_height.split('-')[-1].strip()
            pl_height = get_height(feets = feets, inches = inches)
        if pl_weight:
            pl_weight = get_weight(lbs = pl_weight)

        con, cursor = create_cursor()
        pl_img = extract_list_data(sel, '//table//tr//td//img//@src')[0]
        pl_birth = extract_data(sel, '//table//tr//td//p//strong[contains(text(), "Born:")]//following-sibling::text()')
        if " in " in pl_birth:
            pl_place = pl_birth.split(' in ')[-1].strip()
        else:
            pl_place = ''

        loc_id = ''
        if pl_place:
            city = pl_place.split(',')[0].strip()
            loc_id = 'select id from sports_locations where city ="%s" limit 1' %(city)
            cursor.execute(loc_id)
            loc_id = cursor.fetchone()
            if loc_id:
                loc_id = str(loc_id[0])
            else:
                loc_id = ''

        if "http" not in pl_img:
            pl_img = DOMAIN_URL + pl_img

        if pl_dob:
            dt = datetime.datetime.strptime(pl_dob, "%m/%d/%Y")
            birth_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            birth_date = ''


        pl_exists, pl_id = check_player(pl_sk)
        if pl_exists == False:
            pts_id = check_title(pl_name, birth_date)
            if pts_id and str(pts_id[0]):
                add_source_key(str(pts_id[0]), pl_sk)
                print "source_key", pl_name

            else:
                print "add_player", pl_name
                con, cursor = create_cursor()
                pos = ''
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
                      pl_age, pl_height, pl_weight, birth_date, pl_place, SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, '', '')

                cursor.execute(PL_QUERY, values)

                add_source_key(next_id, pl_sk)
                con.close()

