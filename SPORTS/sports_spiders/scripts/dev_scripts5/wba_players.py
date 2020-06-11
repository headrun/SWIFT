from scrapy.selector import Selector
from scrapy.http import Request, FormRequest
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider, get_nodes, extract_data, extract_list_data
import MySQLdb
import datetime
import re



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
        values = (entity_id, 'participant', 'wba_boxing', _id)

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
entity_type="participant" and source="wba_boxing" and source_key= "%s"'


GAME = 'boxing'
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


class WBABoxingPlayers(VTVSpider):
    name = "wbaboxing_players"
    allowed_domains = []
    start_urls = ['http://www.wbanews.com/wba-ranking#.VdG0VXWSxCU']

    def parse(self, response):
        sel  = Selector(response)
        yr_nodes = get_nodes(sel, '//link[@rel="archives"]')

        for node in yr_nodes:
            yr_ = extract_data(node, './/@href')
            year = yr_.split('/')[-2]
            month_ = yr_.split('/')[-1]
            if '2014' in year or '2015' in year:
                yield FormRequest(url="http://wbanews.com/?page_id=2513", method = "POST",
                                formdata={'month': month_, 'year': year, 'page_id': '2513'},
                            callback=self.parse_next)


    def parse_next(self, response):
        sel = Selector(response)
        pl_nodes = get_nodes(sel, '//table[@id="ranking-over"]//tr//td//a')
        #pl_nodes = ['3183', '3163', '3125', '3135', '2960', '3126', '3137', '1805', '3127', '1174', '2998', '3129', '3158', '3751', '2847', '1299']
        #pl_nodes = ['3636', '3635', '3474', '3473', '3467', '3466', '2955', '3047', '3031', '3032', '2714', '2715', '2718', '3015', '2961', '2555']
        for pl_node in pl_nodes:
            pl_url = extract_data(pl_node, './/@href')
            #pl_url = "http://wbanews.com/boxer_profile.php?boxer=%s" %(pl_node)
            yield Request(pl_url, callback = self.parse_players)


    def parse_players(self, response):
        sel = Selector(response)
        ref_url = response.url
        pl_sk = ref_url.split('=')[-1].strip()
        pl_data = extract_list_data(sel, '//table[@class="tableprofile"]//tr//td//text()')
        pl_name_ = pl_data[0].strip().title()

        loc = ''
        pl_name = pl_name_
        loc_id = ''

        if "(" in pl_name_:
            pl_name = pl_name_.split('(')[0].strip()
            loc = pl_name_.split('(')[-1].strip().replace(')', '').upper()
        if loc:
            con, cursor = create_cursor()
            loc_id = 'select id from sports_locations where country != "" and city = "" and state = "" and iso = "%s"' %(loc)
            cursor.execute(loc_id)
            loc_id = cursor.fetchall()
            for loc_id_ in loc_id:
                loc_id = str(loc_id_[0])
        if loc_id == ():
            loc_id = ''
        pl_nm = pl_name.lower().replace(' ', '-').strip()
        pl_img = 'http://wbanews.com/photos/boxers/%s.png' %(pl_nm)
        pl_dob = pl_data[4]
        pl_age = pl_data[5]
        if "," in pl_dob:
            dt = datetime.datetime.strptime(pl_dob, "%B %d,%Y")
            birth_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            birth_date = ''
        pl_height = extract_list_data(sel, '//table[@class="tableprofile"]//tr//td[contains(text(), "cm")]//text()')
        if pl_height:
            pl_height = pl_height[0].strip()
        else:
            pl_height = ''
        if pl_age:
            pl_age = "".join(re.findall('\d+', pl_age))

        con, cursor = create_cursor()
        pl_exists, pl_id = check_player(pl_sk)

        if pl_exists == False:
            pts_id = check_title(pl_name)

            if pts_id:
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

                values = (next_id, DEBUT, '', ROLES, 'male', \
                      pl_age, pl_height, '', birth_date, '', SAL_POP, RATING_POP, \
                      WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE)

                cursor.execute(PL_QUERY, values)

                add_source_key(next_id, pl_sk)
                con.close()


