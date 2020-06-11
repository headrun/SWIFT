import urlparse
import MySQLdb
import re
from vtvspider_dev import VTVSpider, get_nodes, extract_data
from scrapy.selector import Selector
from scrapy.http import Request
import unicodedata
from StringUtil import cleanString
from difflib import SequenceMatcher
import codecs


PL_NAME_QUERY = 'select id, gid from sports_participants where \
title like "%s" and game="%s" and participant_type="player"'

GAME = "soccer"

PLAYERS_PERMUTATIONS = {}
PLAYERS_IN_DB        = {}
SPACE = ' '
THRESHOLD   = 0.90

tou_id_dict = {
"serie-a": 573
}


class Superesportes(VTVSpider):
    name = "superesportes_browse"
    start_urls =["http://www.mg.superesportes.com.br/campeonatos/2015/brasileiro/serie-a/",
                "http://www.mg.superesportes.com.br/campeonatos/2015/copa-do-brasil/",
                "http://www.mg.superesportes.com.br/futebol/futebol-internacional/"]
    #allowed_domains = ['www.mg.superesportes.com.br']
    start_urls =["http://www.mg.superesportes.com.br/campeonatos/2015/brasileiro/serie-a/"]

    f = open('sports_text.txt', 'w')

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.f = codecs.open('failed_players_file.txt', 'wb', encoding='utf-8')

    def parse(self, response):
        import pdb;pdb.set_trace()
        sel = Selector(response)
        tournament_name = response.split('/')[-2].strip()
        tou_id = tou_id_dict.get(tournament_name, '')
        team_dict = {}
        teams = extract_list_data(sel, '//table[@class="table table-cup table-striped margin-bottom-25"]//tr//td[2]//text()')
        for team in teams:
            team_title = team
            if "Time" in team_title:
                continue
            sp_id, sp_type = self.get_db_id(tou_id, team_title)

            if sp_id:
                iso = 'BRZ'
                titles_query = "insert into sports_titles_regions (entity_id,entity_type,title,aka,short_title,iso,description,modified_at,created_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
                values = (str(sp_id), sp_type, sp_title, '', '', iso, '')
                self.cursor.execute(titles_query, values)

            else:
                self.f.write('%s\n' %(team_title))

    def get_db_id(self, tou_id, team_title):
        team_title = '%' + team_title + '%'
        query = 'select SP.id, SP.participant_type from sports_participants SP, sports_tournaments_participants TP where TP.tournament_id="573" and SP.id=TP.participant_id and SP.title like %s'
        values = (team_title)
        self.cursor.execute(query, values)
        date = self.cursor.fetchone()
        sp_id = ''
        sp_title = ''

        if len(data) == 1:
            sp_id = date[0]
            sp_type = data[1]
        return sp_id, sp_type


