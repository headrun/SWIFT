import datetime
import time
import re
import MySQLdb
from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

#CURSOR = MySQLdb.connect(host="10.4.18.183", user="root", \
         #db="SPORTSDB").cursor()
CURSOR = MySQLdb.connect(host="10.4.18.34", user="root", \
        db="SPORTSDB_BKP").cursor()

def check_player(pl_sk):
    CURSOR.execute(SK_QUERY % pl_sk)
    entity_id = CURSOR.fetchone()
    if entity_id:
        pl_exists = True
        pl_id = entity_id
    else:
        pl_exists = False
        pl_id = ''
    return pl_exists, pl_id

def add_source_key(entity_id, _id):
    if _id and entity_id:
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'worldtour_tennis', _id)

        CURSOR.execute(query, values)

def create_cursor(host, db):
    con = MySQLdb.connect(host=host, user="root", db=db)
    cursor = con.cursor()
    return cursor

def check_title(name):
    CURSOR.execute(PL_NAME_QUERY % (name, 'tennis'))
    pl_id = CURSOR.fetchone()
    return pl_id

PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"';

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="worldtour_tennis" and source_key= "%s"'

class Tennisplayers(VTVSpider):
    name = "tennis_players"
    start_urls = ['http://m.atpworldtour.com/Rankings/Singles.aspx']

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//tr//td[@class="playerName"]//a')
        for node in nodes:
            player_link = extract_data(node, './/@href')
            player_link = "http://www.atpworldtour.com" + player_link
            yield Request(player_link, callback=self.parse_details, meta = {})


    def parse_details(self, response):
        hxs = Selector(response)
        pl_sk = response.url.split('/')[-1].replace('.aspx', '').strip().lower()
        pl_name = response.url.split('/')[-1].replace('.aspx', '').replace('-', ' ').strip()
        pl_exists, pl_id = check_player(pl_sk)
        if pl_exists == False:
            pts_id = check_title(pl_name)
            if pts_id:
                add_source_key(str(pts_id[0]), pl_sk)

