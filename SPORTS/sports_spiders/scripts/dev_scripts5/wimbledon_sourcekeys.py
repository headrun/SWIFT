from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re
import MySQLdb

true = True
false = False
null = ''

def create_cursor():
    con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = con.cursor()
    return con, cursor

def check_player(pl_sk):
    con, cursor = create_cursor()
    cursor.execute(SK_QUERY % pl_sk)
    entity_id = cursor.fetchone()
    con.close()
    if entity_id:
        pl_exists = True
        pl_id = entity_id
    else:
        pl_exists = False
        pl_id = ''
    return pl_exists, pl_id

def check_title(name):
    con, cursor = create_cursor()
    cursor.execute(PL_NAME_QUERY % (name, "tennis"))
    pl_id = cursor.fetchone()
    con.close()
    return pl_id

def add_source_key(entity_id, _id):
    if _id and entity_id:
        con, cursor = create_cursor()
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'wimbledon_tennis', _id)

        cursor.execute(query, values)
        con.close()

PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"';

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source= "wimbledon_tennis" and source_key= "%s"'

class WimbledonPlaye(VTVSpider):
    name = "wimbledon_pl"
    start_urls = ['http://www.wimbledon.com/en_GB/players/index.html']

    def parse(self, response):

        sel = Selector(response)
        nodes = get_nodes(sel, '//div//span//a[contains(@href, "overview")]')
        for node in nodes:
            ref_url = extract_data(node, './/@href')
            ref_url = "http://www.wimbledon.com/en_GB/players/" +ref_url
            yield Request(ref_url, callback=self.parse_players)

    def parse_players(self, response):
        sel = Selector(response)
        player_name  = extract_data(sel, '//section[@class="playerSection playerImageSection"]//h2//text()').strip()
        player_sk = response.url.split('/')[-1].replace('.html', '').replace('atp', '').replace('wta', '')
        pl_link  = response.url
        con, cursor = create_cursor()
        pl_exists, pl_id = check_player(player_sk)
        if pl_exists == False:
            pts_id = check_title(player_name)
            if pts_id:
                add_source_key(str(pts_id[0]), player_sk)
            else:
                f1 = open('tennis_players', 'w')
                f1.write('%s\t\%s\n' % (player_name, pl_link))
