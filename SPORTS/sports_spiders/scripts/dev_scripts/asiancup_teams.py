from vtvspider_dev import VTVSpider, extract_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
import MySQLdb

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="afc_soccer" and source_key= "%s"'

PL_NAME_QUERY = 'select id from sports_participants where \
title like "%s" and game="%s" and participant_type!="player"';


def mysql_connection():
    conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = conn.cursor()
    return conn, cursor

def check_player(pl_sk):
    conn, cursor = mysql_connection()
    cursor.execute(SK_QUERY % pl_sk)
    entity_id = cursor.fetchone()
    conn.close()
    if entity_id:
        pl_exists = True
    else:
        pl_exists = False

    return pl_exists

def check_title(name):
    conn, cursor = mysql_connection()
    name = "%" + name + " national football team%"
    cursor.execute(PL_NAME_QUERY % (name, "soccer"))
    pl_id = cursor.fetchone()
    conn.close()
    return pl_id


class AsianCupTeams(VTVSpider):
    name = "asiancup_teams"
    start_urls = ['http://www.afcasiancup.com/']
    domain_url = "http://www.afcasiancup.com"

    def parse(self, response):
        hxs = Selector(response)
        teams_link = extract_data(hxs, '//li//a[contains(text(), "Teams")]/@href')
        if teams_link:
            if "http" not in teams_link:
                team_link = self.domain_url + teams_link
            yield Request(team_link, self.parse_teams)

    def parse_teams(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@id="teams"]//li[@class="team"]')

        for node in nodes:
            team_name = extract_data(node, './div[contains(@class, "teamName")]/text()')
            team_sk = team_name.strip().lower().replace(' ', '-')
            pl_exists = check_player(team_sk)
            if pl_exists == False:
                pl_id = check_title(team_name)
                if pl_id:
                    self.populate_sk(team_sk, pl_id[0])
                else:
                    print team_name

    def populate_sk(self, team_sk, pl_id):
        conn, cursor = mysql_connection()
        query = "insert into sports_source_keys (entity_id, entity_type, \
                source, source_key, created_at, modified_at) \
                values(%s, %s, %s, %s, now(), now())"
        values = (pl_id, 'participant', 'afc_soccer', team_sk)
        cursor.execute(query, values)
        conn.close()

