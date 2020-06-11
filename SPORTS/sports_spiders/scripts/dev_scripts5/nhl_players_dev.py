from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import time
import datetime
import MySQLdb
import urllib



SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="NHL" and source_key= "%s"'

PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"'

GAME = "hockey"
REF_QUERY = 'update sports_participants set reference_url = "%s" where id=%s and reference_url=""'

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', \
                    'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
                    'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', \
                    'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
                    'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', \
                    'WV' : 'West Virgina', 'FL' : 'Florida', \
                    'KS' : 'Kansas', 'TN' : 'Tennessee', \
                    'LA' : 'Louisiana', 'MO' : 'Missouri', \
                    'AR' : 'Arkansas', 'SD' : 'South Dakota', \
                    'MS' : 'Mississippi', 'MI' : 'Michigan', \
                    'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', \
                    'ID' : 'Idaho', 'RI' : 'Rhode Island', \
                    'NM' : 'New Mexico', 'MN' : 'Minnesota', \
                    'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
                    'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', \
                    'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado' }

PL_INSERT_URL = """http://10.4.18.34/cgi-bin/add_players.py?name=%s&aka=%s&game=%s
                &participant_type=%s&img=%s&bp=%s&ref=%s&loc=%s&debut=%s&main_role=%s
                &roles=%s&gender=%s&age=%s&height=%s&weight=%s&birth_date=%s&birth_place=%s
                &salary_pop=%s&rating_pop=%s&weight_class=%s&marital_status=%s
                &participant_since=%s&competitor_since=%s&src=%s&sk=%s&tou=%s&season=%s
                &status=%s&st_remarks=%s&standing=%s&seed=%s&action=submit"""

def mysql_connection():
    conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = conn.cursor()
    return conn, cursor


def update_pl_location(pl_id, loc_id):
    conn, cursor = mysql_connection()
    values = (loc_id, pl_id)
    cursor.execute(LOC_QUERY % values)
    conn.close()

LOC_QUERY = 'update sports_participants set location_id = %s where location_id = "" and id = %s and participant_type = "player" and game="hockey"'


def check_player(pl_sk):
    conn, cursor = mysql_connection()
    cursor.execute(SK_QUERY % pl_sk)
    entity_id = cursor.fetchone()
    if entity_id:
        pl_exists = True
        pl_id = entity_id
    else:
        pl_exists = False
        pl_id = ''
    return pl_exists, pl_id
    conn.close()

def check_title(name):
    conn, cursor = mysql_connection()
    cursor.execute(PL_NAME_QUERY % (name, GAME))
    pl_id = cursor.fetchone()
    return pl_id

def update_reference_url(pl_id, pl_link):
    values = (pl_link, pl_id)
    cursor.execute(REF_QUERY % values)

PL_SK = ['8477098', '8478419']
class NhlPlayersdev(VTVSpider):
    name = "nhl_playerssdev"
    start_urls = ['http://www.nhl.com/ice/draftsearch.htm?year=2015&team=&position=&round=1']
    participants = {}


    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//table[@class="data"]//tr//td')
        for node in nodes[::1]:
            player_id = extract_data(node, './/a/@href')
            player_id = re.findall(r"\d+", player_id)
            if player_id:
                player_id = player_id[0]
            else:
                continue
            if player_id[0] not in PL_SK:
                continue
            player_link = 'http://www.nhl.com/ice/player.htm?id=%s' % (player_id)
            yield Request(player_link, callback = self.parse_details, \
            meta = {'player_id': player_id})

    def parse_details(self, response):
        hxs = Selector(response)
        pl_link = response.url
        player_sk = response.meta['player_id']
        pl_exists, pl_id = check_player(player_sk)
        aka = dbt = role = \
        sal_pop = rpop = wclass = mstatus = psince = csince = status = \
        status_remarks = standing = seed = ''
        if pl_exists == False:
            season = datetime.datetime.now()
            season = season.year
            nodes = get_nodes(hxs, '//div[contains(@style, "border-bottom: 1px solid #666;")]')
            pl_height = extract_data(hxs, '//div//table[@class="bioInfo"]//tr/td[contains(text(), "HEIGHT")]/following-sibling::td[1]/text()')
            pl_weight = extract_data(hxs, '//div//table[@class="bioInfo"]//tr/td[contains(text(), "WEIGHT")]/following-sibling::td[1]/text()')
            birth_date = extract_data(hxs, '//div//table[@class="bioInfo"]//tr/td[contains(text(), "BIRTHDATE")]/following-sibling::td[1]/text()')
            #birth_date = birth_date.split('(')[0].strip()
            birth = birth_date.replace('\n', '').replace(u'\xa0', '').split('(AGE')
            if len(birth) == 2:
                age = birth[1].replace(')', '').strip()
                b_date = (datetime.datetime(*time.strptime(birth[0].strip(), '%B %d, %Y')[0:6])).date()
            else:
                b_date = ''
                age = ''
            birth_place = extract_data(hxs, '//div//table[@class="bioInfo"]//tr/td[contains(text(), "BIRTHPLACE")]/following-sibling::td[1]/text()')
            b_place = birth_place.encode('ascii', errors = 'ignore')
            conn, cursor =mysql_connection()
            if len(b_place.split(',')) == 3:
                city = b_place.split(',')[0].strip()
                state = REPLACE_STATE_DICT.get(b_place.split(',')[1].strip())
                if not state:
                    state = b_place.split(',')[1].strip()
                country = b_place.split(',')[-1].strip()
            elif len(b_place.split(',')) ==2:
                city = b_place.split(',')[0].strip()
                state =  ''
                country = b_place.split(',')[-1].strip()
            else:
                print b_place
                city = ''
                state =  ''
                country = ''
            if country == "United States":
                country = "USA"
            if city:
                loc_id = 'select id from sports_locations where city ="%s" and state = "%s" and country = "%s" limit 1' %(city, state, country)
                cursor.execute(loc_id)
                loc_id = cursor.fetchall()
                if not loc_id:
                    loc_id = 'select id from sports_locations where city ="%s" and country = "%s" limit 1' %(city, country)
                    cursor.execute(loc_id)
                    loc_id = cursor.fetchall()
                if not loc_id:
                    loc_id = 'select id from sports_locations where city ="%s" limit 1' %(city)
                    cursor.execute(loc_id)
                    loc_id = cursor.fetchall()
                if loc_id:
                    loc_id = str(loc_id[0][0])
                    pl_id = 'select entity_id from sports_source_keys where source="NHL" and entity_type ="participant" and source_key = "%s" limit 1' %(player_sk)
                    cursor.execute(pl_id)
                    pl_id = cursor.fetchall()
                    #update_pl_location(str(pl_id[0][0]), loc_id)
                else:
                    f = open('missing_loc_nhl', 'w')
                    f.write('%s\t\%s\n'%(player_sk, b_place))
            else:
                print b_place
            for node in nodes:
                name = extract_data(node, './/h1/div/text()')
                if pl_exists == False:
                    pid = check_title(name)
                player_role  = extract_data(node, './/span[@style="color: #666;"]//text()').replace('\n', '')
            pl_image = extract_data(hxs, '//div[@class="headshot"]//img/@src')

            pl_values = (name, '', 'hockey', 'player', pl_image, 200, \
                        response.url, b_place, dbt, player_role, role, 'male', age, \
                        pl_height, pl_weight, b_date, b_place, sal_pop, rpop, wclass, \
                        mstatus, psince, csince, 'NHL', player_sk, "NHL Hockey", \
                        '2015', status, status_remarks, standing, seed)
            if not pid:
                pl_add_url = (PL_INSERT_URL % pl_values).replace('\n', '')
                urllib.urlopen(pl_add_url)
                print "Added player", name
            conn.close()

