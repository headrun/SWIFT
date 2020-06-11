import datetime
from scrapy.selector import Selector
from vtvspider_new import VTVSpider, extract_data, \
extract_list_data, get_nodes, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re
import MySQLdb

def mysql_connection():
    connection = MySQLdb.connect(host = '10.4.18.34', user = 'root', db = 'SPORTSDB_BKP', charset='utf8', use_unicode=True)
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

group_dict = {"Men's Singles": "U.S. Open Men's Singles Seed", \
              "Women' Singles": "U.S. Open Women's Singles Seed", \
              "Men's Doubles": "U.S. Open Men's Doubles Seed", \
              "Women's Doubles": "U.S. Open Women's Doubles Seed"}

class WimbledonSeed(VTVSpider):
    name = "wimbledon_seed"
    allowed_domains = []
    start_urls = ['https://en.wikipedia.org/wiki/2015_Wimbledon_Championships']


    def parse(self, response):
        sel = Selector(response)
        seed_nodes = get_nodes(sel, '//table[contains(@class, "wikitable sortable")]//tr')
        record = SportsSetupItem()
        for node in seed_nodes:
            seed = extract_data(node, './/td[1]//text()')
            if not seed:
                continue

            rank = extract_data(node, './/td[2]//text()')
            pl_name = extract_data(node, './/td[3]//text()').encode('utf-8')
            if "Muguruza" in pl_name:
                pl_name = "Garbine Muguruza Blanco"
            pts_before = extract_data(node, './/td[4]//text()')
            ptf_def = extract_data(node, './/td[5]//text()').replace('(', '').replace(')', '')
            pts_won = extract_data(node, './/td[6]//text()')
            pts_after = extract_data(node, './/td[7]//text()')
            group_name = extract_data(node, './/..//preceding-sibling::h3//span[@class="mw-headline"]//a//text()')
            pl_id = get_pl_id(pl_name)
            if pl_id:
                pl_sk = get_pl_sk(pl_id)
            else:
                continue
            results = {pl_sk : {'rank': rank, 'seed': seed, 'pts_before': pts_before, \
            'ptf_def': ptf_def, 'pts_after': pts_after, 'pts_won': pts_won}}
            group_name = group_dict.get(group_name, '')
            if not group_name:
                group_name = "Wimbledon Women's Singles Seed"
            record['tournament']    = group_name
            record['source']        = 'wimbledon_tennis'
            record['season']        = "2015"
            record['participant_type'] = 'participant'
            record['result_type']   = 'group_standings'
            record['result']        = results
            yield record

        double_seeds = get_nodes(sel, '//table[@class="wikitable"]//tr')
        count = 0
        for double_seed in double_seeds:
            pl_names = extract_list_data(double_seed, './/td//a//text()')
            if not pl_names or len(pl_names) !=2:
                continue
            count += 1
            if int(count)<=16:
                group_name = "Wimbledon Men's Doubles Seed"
            elif int(count)>16 and int(count)<=32:
                group_name = "Wimbledon Women's Doubles Seed"
            elif int(count)>32:
                group_name = "Wimbledon Mixed Doubles Seed"
            for pl_name in pl_names:
                pl_name = pl_name.encode('utf-8').strip()

                if "Marcel Granollers" in pl_name:
                    pl_name = "Marcel Granollers-Pujol"
                if "Raquel Kops-Jones" in pl_name:
                    pl_name = "Rachel Kops-Jones"
                if "Anna-Lena Gr" in pl_name:
                    pl_name = "Anna-Lena Groenefeld"
                if "Bethanie Mattek" in pl_name:
                    pl_name = "Bethanie Mattek"
                if " Muguruza" in pl_name:
                    pl_name = "Garbine Muguruza Blanco"
                if "Anastasia Rodionova" in pl_name:
                    pl_name = "Anastassia Rodionova"
                pl_id = get_pl_id(pl_name)
                if pl_id:
                    pl_sk = get_pl_sk(pl_id)
                else:
                    continue

                rank    = extract_data(double_seed, './/td[3]//text()')
                if rank == "266" or rank == "44":
                    continue
                seed    = extract_data(double_seed, './/td[4]//text()')
                results = {pl_sk : {'rank': rank, 'seed': seed}}
                record['tournament']    = group_name
                record['source']        = 'wimbledon_tennis'
                record['season']        = "2015"
                record['participant_type'] = 'participant'
                record['result_type']   = 'group_standings'
                record['result']        = results
                yield record

