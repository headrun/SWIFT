import datetime
from scrapy.selector import Selector
from vtvspider_new import VTVSpider, extract_data, \
extract_list_data, get_nodes, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re
import MySQLdb


def mysql_connection():
    connection = MySQLdb.connect(host = '10.4.18.183', user = 'root', db = 'SPORTSDB', charset='utf8', use_unicode=True)
    cursor = connection.cursor()
    return connection, cursor

def get_pl_id(pl_title):
    connection, cursor = mysql_connection()
    query = 'select id from sports_participants where game ="tennis" and participant_type ="player" and title = %s'
    values = (pl_title)
    cursor.execute(query, values)
    ids = cursor.fetchone()
    if ids:
        pl_id =  str(ids[0])
        return pl_id
    connection.close()

def get_pl_sk(pl_id):
    connection, cursor = mysql_connection()
    query = 'select source_key from sports_source_keys where entity_id =%s and source="espn_tennis" and entity_type="participant"'
    values = (pl_id)
    cursor.execute(query, values)
    sks = cursor.fetchone()
    if sks:
        pl_sk = str(sks[0])
        return pl_sk
    connection.close()



class RogersCupSeed(VTVSpider):
    name = "rogerscup_seed"
    allowed_domains = []
    start_urls = ['https://en.wikipedia.org/wiki/2015_Rogers_Cup']


    def parse(self, response):
        sel = Selector(response)
        seed_nodes = get_nodes(sel, '//table[contains(@class, "sortable wikitable")]//tr')
        record = SportsSetupItem()
        for node in seed_nodes:
            seed = extract_data(node, './/td[4]//text()')
            if not seed :
                continue
            group_name = extract_list_data(node, './/..//preceding-sibling::h2//span[@class="mw-headline"]//text()')
            group_name = group_name[-1].strip()
            rank = extract_data(node, './/td[3]//text()')
            pl_name = extract_data(node, './/td[2]//text()').encode('utf-8')
            if "Muguruza" in pl_name:
                pl_name = "Garbine Muguruza Blanco"
            pl_id = get_pl_id(pl_name)
            if pl_id:
                pl_sk = get_pl_sk(pl_id)
            else:
                continue
            results = {pl_sk : {'rank': rank, 'seed': seed}}
            if "ATP singles" in group_name:
                group_name = "Rogers Cup Men's Singles Seed"
            if "WTA singles" in group_name:
                group_name = "Rogers Cup Women's Singles Seed"
            record['tournament']    = group_name
            record['source']        = 'espn_tennis'
            record['season']        = "2015"
            record['participant_type'] = 'participant'
            record['result_type']   = 'group_standings'
            record['result']        = results
            yield record

