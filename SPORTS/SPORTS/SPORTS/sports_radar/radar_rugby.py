import json
import MySQLdb
import genericFileInterfaces
from vtvspider import VTVSpider, get_nodes, extract_data
from scrapy.selector import Selector
from scrapy.http import Request
from lxml import etree
import datetime
from StringIO import StringIO
import unicodedata
from StringUtil import cleanString
from difflib import SequenceMatcher
import codecs
import MySQLdb
import datetime
import time
import re


PLAYERS_PERMUTATIONS = {}
PLAYERS_IN_DB        = {}
SPACE = ' '
THRESHOLD   = 0.70

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, "%s", "%s", "%s", now(), now())'

PL_QRY = 'select id from sports_participants where title = "%s" and game like "%rugby%"'
PL_DOB_QRY = 'select birth_date from sports_players where participant_id = %s'

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


class CricketRadar(VTVSpider):
    name = 'rugby_radar'

    def start_requests(self):
        teams_url = 'https://api.sportradar.us/rugby-p1/teams/2016/hierarchy.xml?api_key=8zp2sjv9ra28tct2rzfqh73a'
        yield Request(teams_url, callback = self.parse_one)

    def __init__(self):
        self.conn        = MySQLdb.connect(host='10.28.216.45', user='veveo', passwd='veveo123', db= 'SPORTSDB_DEV', charset='utf8', use_unicode=True)
        self.cursor      = self.conn.cursor()
	self.url = 'https://api.sportradar.us/rugby-p1/teams/%s/profile.xml?api_key=8zp2sjv9ra28tct2rzfqh73a'
        self.cricket_exists_file = open('rugby_exists_file', 'w')
        self.cricket_missed_file = open('rugby_missed_file', 'w')

    def check_sk(self, sk):
        query = 'select entity_id from sports_source_keys where source=%s and entity_type=%s and source_key=%s'
        values = ('radar', 'participant', sk)
        self.cursor.execute(query, values)
        data = self.cursor.fetchone()
        if data:
            return True
        else:
            return False

    def parse_one(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        team_nodes = sel.xpath('//team')
        for node in team_nodes:
            node_id = extract_data(node, './@id')
            team_name = extract_data(node, './@name')
            if 'women' in team_name.lower(): continue
            yield Request(self.url%node_id, callback = self.parse_teams)

    def parse_teams(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        team_name = ''.join(sel.xpath('//team/@name').extract())
        self.get_db_players(team_name)
        get_player_permutations(self.PLAYERS_IN_DB.keys())
        player_nodes = sel.xpath('//player')
        for node in player_nodes:
            player_id = ''.join(node.xpath('./@id').extract())
            player_name = ''.join(node.xpath('./@full_name').extract())
            player_nick = ''.join(node.xpath('./@first_name').extract())
            player_dob = ''.join(node.xpath('./@birth_date').extract())

            if player_dob:
                site_birt_date = str(datetime.datetime.strptime(player_dob, "%Y-%m-%d"))
            else:
                site_birt_date = "0000-00-00"

            pl_exists = self.check_sk(player_id)
            if pl_exists == True:
                continue

            pl_id         = self.get_plid(site_birt_date, player_name, player_nick)

            if not pl_id:
                pl_id = self.PLAYERS_IN_DB.get(player_name.lower())
                if not pl_id:
                    ex, x, y, z = find_players_to_add([player_name], self.PLAYERS_IN_DB.keys())
                    try:
                        pl_id = self.PLAYERS_IN_DB[x[0].lower()]
                    except:
                        pl_id = ''
            if not pl_id:
                record = [player_id, player_name, player_dob,team_name]
                self.cricket_missed_file.write('%s\n' % record)
            else:
                self.cursor.execute(INSERT_SK %(pl_id, "participant", "radar", player_id))

    def get_plid(self, site_birt_date, full_name, player_nick):
        qry = 'select P.id from sports_participants P, sports_players PL where P.title="%s"'%full_name + ' and P.game like "%rugby%" and P.id=PL.participant_id and ' + 'PL.birth_date="%s"'%site_birt_date
        self.cursor.execute(qry)
        data = self.cursor.fetchone()
        if data:
            pl_id = data[0]
        else:
            qry ='select P.id from sports_participants P, sports_players PL where P.title like "%s"'%('%'+player_nick+'%') +' and P.game like "%rugby%" and P.id=PL.participant_id and '+ 'PL.birth_date="%s"'%site_birt_date
            self.cursor.execute(qry)
            data = self.cursor.fetchone()
            if data:
                pl_id = data[0]
            else: pl_id = ''
        return pl_id

    def get_db_players(self, team):
        query = 'select id, title from sports_participants where title like "%s" and game like "%rugby%" and participant_type = "team"'
        self.cursor.execute(query)          
        tou_id = self.cursor.fetchall()
        if tou_id:
            tou_id = tou_id[0][0]
        else:
            print "No Tournament foudn ..."
            print team
            return
        
        query = 'select id, title from sports_participants where id in (select player_id from sports_roster where team_id = %s)'%tou_id
        self.cursor.execute(query)
        players = self.cursor.fetchall()

        self.PLAYERS_IN_DB = {}
        for player in players:
            player_id, title = player
            title = title.strip().lower()
            self.PLAYERS_IN_DB[title] = int(player_id)

