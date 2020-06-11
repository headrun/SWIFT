import urlparse
import MySQLdb
import re
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
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
"serie-a": 573,
"liga-europa": '215'
}


class SuperesportsTitles(VTVSpider):
    name = "superesportestitles_browse"
    start_urls =["http://www.mg.superesportes.com.br/campeonatos/2015/brasileiro/serie-a/"]

    f = open('sports_text.txt', 'w')

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.f = codecs.open('failed_players_file.txt', 'wb', encoding='utf-8')

    def parse(self, response):
        sel             = Selector(response)
        tournament_name = response.url.split('/')[-2].strip()
        tou_id          = tou_id_dict.get(tournament_name, '')
        team_dict = {}
        teams = extract_list_data(sel, '//table[@class="table table-cup table-striped margin-bottom-25"]//tr//td[2]//text()')

        for team in teams:
            team_title = team.strip()
            if "Time" in team_title or not team_title:
                continue
            sp_id, sp_type, iso= self.get_db_id(tou_id, team_title)

            if sp_id:
                iso = 'PRT'
                titles_query = "insert into sports_titles_regions (entity_id,entity_type,title,aka,short_title,iso,description,modified_at,created_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
                values = (str(sp_id), sp_type, team_title, '', '', iso, '')
                self.cursor.execute(titles_query, values)

            else:
                self.f.write('%s\n' %(team_title))
        '''pl_node = extract_list_data(sel, '//table[@class="table table-cup"]//tr[@class="active"]//td[2]//text()')
        for pl_nd in pl_node:
            pl_title = pl_nd.strip()
            if not pl_title:
                continue
            if pl_title:
                pl_id, pl_type =  self.get_playerid(pl_title)
                if pl_id:
                    iso = 'BRA'
                    titles_query = "insert into sports_titles_regions (entity_id,entity_type,title,aka,short_title,iso,description,modified_at,created_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
                    values = (str(pl_id), pl_type, pl_title, '', '', iso, '')
                    self.cursor.execute(titles_query, values)

                else:
                    self.f.write('%s\n' %(team_title))'''

    def get_playerid(self, pl_title):
        pl_title = '%' + pl_title + '%'
        pl_query = 'select id, participant_type from sports_participants where title like %s and game="soccer" and participant_type="player"'
        values = (pl_title)
        self.cursor.execute(pl_query, values) 
        data = self.cursor.fetchone()
        sp_id = ''
        sp_type = ''
        if data and len(data)==2:
            sp_id = data[0]
            sp_type = data[1]
        return sp_id, sp_type


    def get_db_id(self, tou_id, team_title):
        if "tico-MG" in team_title:
            team_title = "Clube Atletico Mineiro"
        if "tico-PR" in team_title:
            team_title = "Clube Atletico Paranaense"
        if "Legia Vars" in team_title:
            team_title = "Legia Warsaw"
        if "Olympique M" in team_title:
            team_title = "Olympique de Marseille"
        if "Lokomotiv Mosco" in team_title:
            team_title = "Lokomotiv Moscow"
        if "Rapid Vienna" in team_title:
            team_title = "Rapid Wien"
        if "Sparta Praga" in team_title :
            team_title = "Sparta Prague"

        team_title = '%' + team_title + '%'
        query = 'select SP.id, SP.participant_type, SP.location_id from sports_participants SP, sports_tournaments_participants TP where TP.tournament_id=%s and SP.id=TP.participant_id and SP.title like %s'
        values = (tou_id, team_title)
        self.cursor.execute(query, values)
        data = self.cursor.fetchone()
        sp_id = sp_type = sp_loc = ''
        if data:
            sp_id, sp_type, sp_loc = data
        return sp_id, sp_type, sp_loc
