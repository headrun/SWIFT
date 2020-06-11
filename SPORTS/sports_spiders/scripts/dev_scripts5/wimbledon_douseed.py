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
    query = 'select source_key from sports_source_keys where entity_id =%s and source="wimbledon_tennis" and entity_type="participant"'
    values = (pl_id)
    cursor.execute(query, values)
    sks = cursor.fetchone()
    if sks:
        pl_sk = str(sks[0])
        return pl_sk
    connection.close()



class WimbledondouSeed(VTVSpider):
    name = "wimbledondob_seed"
    allowed_domains = []
    start_urls = ['https://en.wikipedia.org/wiki/2015_Wimbledon_Championships']

    def parse(self, response):
        sel = Selector(response)
        double_seeds = get_nodes(sel, '//table[@class="wikitable"]//tr[@style="background:#fcc;"]')
        record = SportsSetupItem()
        count = 0
        for double_seed in double_seeds:
            pl_names = extract_list_data(double_seed, './/td//a//text()')
            count += 1
            if int(count)<=14:
                group_name = "Wimbledon Men's Doubles Seed"
            elif int(count)>14 and int(count)<=30:
                group_name = "Wimbledon Women's Doubles Seed"
            else:
                group_name = "Wimbledon Mixed Doubles Seed"
            for pl_name in pl_names:
                pl_name = pl_name.encode('utf-8').strip()
                pl_id = get_pl_id(pl_name)
                if pl_id:
                    pl_sk = get_pl_sk(pl_id)
                else:
                    continue
                rank    = extract_data(double_seed, './/td[3]//text()')
                seed    = extract_data(double_seed, './/td[4]//text()')
                results = {pl_sk : {'rank': rank, 'seed': seed}}
                record['tournament']    = group_name
                record['source']        = 'wimbledon_tennis'
                record['season']        = "2015"
                record['participant_type'] = 'participant'
                record['result_type']   = 'group_standings'
                record['result']        = results
                yield record

