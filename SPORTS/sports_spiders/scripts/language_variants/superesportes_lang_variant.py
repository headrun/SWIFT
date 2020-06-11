import urlparse
import MySQLdb
import datetime
import time
import re
from vtvspider import VTVSpider, get_nodes, extract_data, get_utc_time
from vtvspider import get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request
from sports_spiders.items import SportsSetupItem
import unicodedata
from StringUtil import cleanString
from difflib import SequenceMatcher
import codecs



PLAYERS_PERMUTATIONS = {}
PLAYERS_IN_DB        = {}
SPACE = ' '
THRESHOLD   = 0.70


def permutations(iterable, r=None):
    # permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
    # permutations(range(3)) --> 012 021 102 120 201 210
    pool = tuple(iterable)
    n = len(pool)
    if r is None:
        r = n 

    if r > n:
        return
    indices = range(n)
    cycles = range(n, n-r, -1) 
    yield tuple(pool[i] for i in indices[:r])
    while n:
        for i in reversed(range(r)):
            cycles[i] -= 1
            if cycles[i] == 0:
                indices[i:] = indices[i+1:] + indices[i:i+1]
                cycles[i] = n - i 
            else:
                j = cycles[i]
                indices[i], indices[-j] = indices[-j], indices[i]
                yield tuple(pool[i] for i in indices[:r])
                break
        else:
            return

def get_player_permutations(existing_players):
    for existing_player in existing_players:
        if SPACE in existing_player:
            existing_player = cleanString(existing_player)
            existing_player_list = existing_player.split(SPACE)
            if len(existing_player_list) <= 5:
                existing_player_set = [SPACE.join(i) for i in \
                                      permutations(existing_player_list)]
            else:
                existing_player_set = [existing_player]

        else:
            existing_player_set = [existing_player]

        PLAYERS_PERMUTATIONS[existing_player] = existing_player_set


def find_players_to_add(db_players, tou_players):
    titles_present = {}
    mstly_matched_titles = {}

    for player in tou_players:
        player = player
        if player in db_players:
            titles_present[player] = (player, 'EXACTLY PRESENT')
            continue
        check_flag = False
        seqmatcher = SequenceMatcher(None, player)

        for existing_player in db_players:
            if SPACE in player and SPACE in existing_player:
                existing_player_set = PLAYERS_PERMUTATIONS.\
                                      get(existing_player, [existing_player])
            else:
                existing_player_set = [existing_player]

            for ex_player in existing_player_set:
                seqmatcher.set_seq2(ex_player)
                if seqmatcher.ratio() >= THRESHOLD:
                    mstly_matched_titles[player] = existing_player
                    titles_present[player] = (player, 'MOSTLY PRESENT')
                    check_flag = True
                    break

            if check_flag: break
        else:
            titles_present[player] = (player, 'NOT PRESENT')

    mostly_present = [player for player, details in titles_present.items() \
                     if details[1] == 'MOSTLY PRESENT']
    exactly_present = [player for player, details in titles_present.items() \
                     if details[1] == 'EXACTLY PRESENT']
    not_present = [player for player, details in titles_present.items() \
                     if details[1] == 'NOT PRESENT']
    return exactly_present, mostly_present, not_present, mstly_matched_titles

class Superesportes(VTVSpider):
    name = "superesportes_langvariant"
    start_urls = ["http://www.mg.superesportes.com.br/campeonatos/2015/ingles-2015/16/"]
                    #"http://www.mg.superesportes.com.br/campeonatos/2015/liga-europa/"]
    start_urls = ['http://www.mg.superesportes.com.br/campeonatos/2015/liga-dos-campeoes/']
    today = str(datetime.datetime.now().date())

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.players_file = open('/home/veveo/headrun/language_variant/language_variant_%s' % self.today, 'a+')

    def parse(self, response):
        sel = Selector(response)

        if "ingles" in response.url:
            tournament_name = "Premier League"
        if "liga-europa" in response.url:
            tournament_name = "UEFA Europa League"
        if "copa-america" in response.url:
            tournament_name = "Copa America"
        if "liga-dos-campeoes" in response.url:
            tournament_name = "UEFA Champions League"

        links = sel.xpath('//tr[@class="small table-cup__date"]//td[@class="text-center"]//a//@href').extract()
        for link in links:
            yield Request(link, callback = self.parse_next, meta = {'tournament_name': tournament_name})

    def parse_next(self, response):
        print response.url
        sel = Selector(response)
        tournament_name = response.meta['tournament_name']

        team_dict = {}
        teams = sel.xpath('//div[@class="row rt__header"]/div[@class="col-sxs-2 col-xs-3 col-sm-2 col-md-3"]/figure/img/@title').extract()
        player1 = sel.xpath('//div[@class="col-sm-3 hidden-xs "]/article/ul[@id="player-1-escalacao"]/li[not(contains(text(), "cnico:"))]/text()').extract()

        if player1:
            team_dict.update({teams[0]: player1})

        player2 = sel.xpath('//div[@class="col-sm-3 hidden-xs "]/article/ul[@id="player-2-escalacao"]/li[not(contains(text(), "cnico:"))]/text()').extract()
        if player2:
            team_dict.update({teams[1]: player2})

        if team_dict:
            for team, players in team_dict.iteritems():
                self.get_db_players(tournament_name, team)
                get_player_permutations(self.PLAYERS_IN_DB.keys())

                for player in players:
                    player      = player.strip()
                    player_id   = self.PLAYERS_IN_DB.get(player.lower())  
                    if not player_id:
                        ex, x, y, z = find_players_to_add([player], self.PLAYERS_IN_DB.keys()) 
                        player_id = self.PLAYERS_IN_DB[x[0].lower()]
                    if player_id:
                        self.parse_player_insertion(player_id, player) 
                    else:
                        print player_id
                        self.players_file.write('%s<>%s\n' % (player.encode('utf-8'), team.encode('utf-8')))

    def parse_player_insertion(self, player_id, player):
        sp_type = "player"
        iso = 'PRT'
        titles_query = "insert into sports_titles_regions (entity_id,entity_type,title,aka,short_title,iso,description,modified_at,created_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (str(player_id), sp_type, player.strip(), '', '', iso, '')
        self.cursor.execute(titles_query, values)


    def get_db_players(self, tou_id, team):
        print "%s ---- %s" %(tou_id, team)
        '''get tournament id '''
        query = 'select id, title from sports_tournaments where title  = "%s"' %tou_id
        self.cursor.execute(query)
        tou_id = self.cursor.fetchall()
        if tou_id:
            tou_id = tou_id[0][0]
        else:
            print "No Tournament foudn ..."
            return

        ''' Get teams '''
        query = 'select Sp.id, Sp.title from sports_participants Sp, sports_tournaments_participants Stp where Sp.title like "%' +team +'%" and Sp.participant_type="team" and Stp.tournament_id='+ str(tou_id) +' and Sp.id=Stp.participant_id'
        self.cursor.execute(query)
        team_id = self.cursor.fetchall()

        if team_id and len(team_id) == 1:
            team_id = team_id[0][0]
        else:
            print "No Team name"
            return

        print tou_id, team_id
        '''get all the players titles '''
        query = 'select P.title, Sr.player_id, Sr.team_id from sports_participants P, sports_roster Sr where Sr.team_id = %s and P.id = Sr.player_id'
        values = (team_id)
        self.cursor.execute(query, values)
        players = self.cursor.fetchall() 

        self.PLAYERS_IN_DB = {}
        for player in players:
            title, player_id, team_id = player
            title = title.strip().lower()
            self.PLAYERS_IN_DB[title] = int(player_id)
