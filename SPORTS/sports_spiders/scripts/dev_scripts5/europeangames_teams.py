import re
from vtvspider_dev import VTVSpider
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import get_nodes, extract_data, extract_list_data
import MySQLdb
import urllib

GAME = "beach soccer"

TEAM_NAME_QUERY = 'select id from sports_participants where \
title like "%s" and game="%s" and participant_type="team"'

INSERT_SK = 'insert into sports_source_keys (entity_id, entity_type, \
source, source_key, created_at, modified_at) \
values("%s", "%s", "%s", "%s", now(), now())'

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="eco_baku" and source_key= "%s"'

TEAM_URL = """http://10.4.18.34/cgi-bin/add_teams_bak.py?name=%s&aka=%s&game=%s&participant_type=%s&img=%s&bp=%s&ref=%s&loc=%s&short=%s&callsign=%s&category=%s&kws=%s&tou_name=%s&division=%s&gender=%s&formed=%s&tz=%s&logo=%s&vtap=%s&yt=%s&std=0&src=%s&sk=%s&action=submit"""

#TEAM_URL = """http://10.4.18.34/cgi-bin/add_teams.py?name=%s&aka=%s&game=%s&participant_type=%s&img=%s&bp=%s&ref=%s&loc=%s&short=%s&callsign=%s&category=%s&kws=%s&tou_name=%s&division=%s&gender=%s&formed=%s&tz=%s&logo=%s&vtap=%s&yt=%s&std=0&src=%s&sk=%s&action=submit"""

def create_cursor():
    con = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
    cursor = con.cursor()
    return con, cursor


def check_title(name):
    con, cursor = create_cursor()
    team_name = "%" + name + "%"
    cursor.execute(TEAM_NAME_QUERY % (team_name, GAME))
    pl_id = cursor.fetchall()
    con.close()
    return pl_id

def check_sk(pl_sk):
    con, cursor = create_cursor()
    cursor.execute(SK_QUERY % pl_sk)
    entity_id = cursor.fetchone()
    if entity_id:
        pl_exists = True
        pl_id = entity_id
    else:
        pl_exists = False
        pl_id = ''
    return pl_exists, pl_id

aka, bp = "", "-150"
tou = "European Games - 3x3 Basketball"
#tou = "European Games - Men's Beach Soccer"
category = "eoc"
par_type = "team"
game = "basketball"
formed = "="
vtap = ""
you_tube = ""
logo_url = ""
tz = ""
source = "eoc_baku"
def add_source_key(entity_id, _id):
    if _id and entity_id:
        con, cursor = create_cursor()
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'eoc_baku', _id)

        cursor.execute(query, values)
        con.close()


class EoropeanGamesTeam(VTVSpider):
    name ="european_game_teams"
    allowed_domains = []
    start_urls = ['http://www.baku2015.com/basketball3x3/teams/index.html?intcmp=sport-hp']
    #start_urls = ['http://www.baku2015.com/beach-soccer/teams/index.html?intcmp=sport-hp']

    def parse(self, response):
        sel = Selector(response)
        team_nodes = get_nodes(sel, '//div[@class="or-team-by-sport"]//ul//li')
        for node in team_nodes:
            team_url = extract_data(node, './/a//@href')
            team_title = extract_data(node, './/span[@class="or-item-name"]//text()')
            if not team_title:
                 continue
            short_title = team_title
            team_image = extract_data(node, './/img//@src')
            team_sk = "".join(re.findall('team=(.*)/', team_url))
            team_sk = team_sk.split('-')[-1].strip()
            team_link = "http://www.baku2015.com" + team_url
            '''if '/men/' in team_url:
                gendre = "men"
                team_title =  team_title + " men's national 3x3 team"
            elif '/women/' in team_url:
                gender = "women"
                team_title = team_title + " women's national 3x3 team"
            else:
                continue'''
            gender = "men"
            team_title = team_title + " national beach soccer team"
            con, cursor = create_cursor()
            loc_id = 'select id from sports_locations where city ="" and state = "" and country = "%s" limit 1' %(short_title)
            cursor.execute(loc_id)
            loc_id = cursor.fetchall()
            if loc_id:
                loc_id = str(loc_id[0][0])
            else:
                loc_id = ''
            pl_exists, pl_id = check_sk(team_sk)
            if pl_exists == False:
                pts_id = check_title(team_title)
                if pts_id:
                    print "source key", team_title
                    add_source_key(str(pts_id[0]), team_sk)
                else:
                    pt_ids = check_title(team_title)
                    team_values = (team_title, aka, game, par_type, team_image, bp, team_link, loc_id, short_title, '', category, "", tou, "", gender, formed, tz, logo_url, vtap, you_tube, source, team_sk)
                    if team_title:
                        tm_add_url = (TEAM_URL % team_values).replace('\n', '')
                        urllib.urlopen(tm_add_url)
                        print  "Added team ", team_title

