from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
import MySQLdb
import urllib
import datetime

CURSOR = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP").cursor()

#CURSOR = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB").cursor()

MAIN_LINK = "http://www.usopen.org"

PLAYER_DATA = './div[contains(text(), "%s")]/following-sibling::div[1]/text()'

SK_CHECK_QUERY = 'select entity_id from sports_source_keys \
where entity_type="participant" and source="usopen_tennis" and source_key="%s"'

PARTICIPANT_QUERY = 'select id from sports_participants \
where game="tennis" and title="%s" or aka="%s"'

INSERT_SK_QUERY = "insert into sports_source_keys (entity_id, entity_type, \
source, source_key, created_at, modified_at) \
values(%s, 'participant', 'usopen_tennis', '%s', now(), now())"

PL_INSERT_URL = """http://10.4.18.34/cgi-bin/add_players.py?name=%s&aka=%s&game=%s
                &participant_type=%s&img=%s&bp=%s&ref=%s&loc=%s&debut=%s&main_role=%s
                &roles=%s&gender=%s&age=%s&height=%s&weight=%s&birth_date=%s&birth_place=%s
                &salary_pop=%s&rating_pop=%s&weight_class=%s&marital_status=%s
                &participant_since=%s&competitor_since=%s&src=%s&sk=%s&tou=%s&season=%s
                &status=%s&st_remarks=%s&standing=%s&seed=%s&action=submit"""

def player_check(name, pl_sk):
    query = SK_CHECK_QUERY % (pl_sk)
    CURSOR.execute(query)

    entity_id = CURSOR.fetchone()
    if not entity_id:
        CURSOR.execute(PARTICIPANT_QUERY % (name, name))
        pt_id = CURSOR.fetchone()
        if pt_id:
            pt_id = str(pt_id[0])
            pl_exist = True
        else:
            pl_exist = False
    else:
        pl_exist = True
        pt_id = ''
    return pt_id, pl_exist

class UsOpenTennisPlayers(VTVSpider):
    name = "usopentennis_players"
    start_urls = ['http://www.usopen.org/en_US/players/nations/index.html?promo=subnav']

    def parse(self, response):
        hxs = Selector(response)
        countries = get_nodes(hxs, '//div[@id="contentArea"]//option')
        for country in countries:
            nation = extract_data(country, './text()')
            nation_url = extract_data(country, './@value')
            if nation_url:
                nation_url = MAIN_LINK + nation_url
                yield Request(nation_url, callback=self.parse_country)

    def parse_country(self, response):
        hxs = Selector(response)
        players = get_nodes(hxs, '//div[@id="contentArea"]//table/tr/td//a[contains(@href, "players")]')

        for player in players:
            pl_link = extract_data(player, './@href')
            if pl_link:
                pl_link = MAIN_LINK + pl_link
                yield Request(pl_link, callback=self.parse_player)

    def parse_player(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@id="playerProfile"]//div[@class="info"]')
        for node in nodes:
            pl_image = extract_data(node, './/preceding-sibling::div//@src')
            if "http" not in pl_image:
                pl_image = "http://www.usopen.org" +pl_image
            if not pl_image:
                pl_image = extract_data(node, './/@src')
            pl_sk = response.url.split('/')[-1].split('.')[-2]
            if "atp" in pl_sk:
                gender = "male"
            if "wta" in pl_sk:
                gender = "female"
            name = extract_data(node, './div[@class="value upper"]/text()')
            if name == "Jia-Jing Lu":
                name = "Lu Jiajing"
            pt_id, pl_exist = player_check(name, pl_sk)
            nation = extract_data(node, PLAYER_DATA % ("Country"))
            dob = extract_data(node, PLAYER_DATA % ("Birth Date"))
            place = extract_data(node, PLAYER_DATA % ("Birthplace"))
            height = extract_data(node, PLAYER_DATA % ("Height"))
            weight = extract_data(node, PLAYER_DATA % ("Weight"))
            plays = extract_data(node, PLAYER_DATA % ("Plays"))
            if dob:
                dt = datetime.datetime.strptime(dob, "%d %B %Y")
                dob = dt.strftime("%Y-%m-%d %H:%M:%S")
            height = height.split('(')[-1].replace('meters )', 'm').strip()
            weight = weight.split('(')[-1].replace('kilos )', 'kg').strip()
            aka = loc = dbt = mrl = role = age = \
            sal_pop = rpop = wclass = mstatus = psince = csince = status = \
            status_remarks = standing = seed = ''

            pl_values = (name, '', 'tennis', 'player', pl_image, 200, \
                        response.url, place, dbt, mrl, role, gender, age, \
                        height, weight, dob, place, sal_pop, rpop, wclass, \
                        mstatus, psince, csince, 'usopen_tennis', pl_sk, "U.S. Open (Tennis)", \
                        '2015', status, status_remarks, standing, seed)
            if pt_id:
                self.insert_sk(pt_id, pl_sk)
                print "Inserted Source Key", name, pl_sk
            if pl_exist == False:
                pl_add_url = (PL_INSERT_URL % pl_values).replace('\n', '')
                urllib.urlopen(pl_add_url)
                print "Missed Player", name

    def insert_sk(self, pt_id, pl_sk):
        query = INSERT_SK_QUERY % (pt_id, pl_sk)
        CURSOR.execute(query)
