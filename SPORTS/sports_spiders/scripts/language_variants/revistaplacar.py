import datetime
import time
import re
from vtvspider import VTVSpider, get_nodes, extract_data, get_utc_time
from vtvspider import get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request
from sports_spiders.items import SportsSetupItem
import MySQLdb
import unicodedata
from StringUtil import cleanString
from difflib import SequenceMatcher
import codecs

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="soccerway_soccer" and source_key= "%s"'

PL_NAME_QUERY = 'select id, gid from sports_participants where \
title like "%s" and game="%s" and participant_type="player"'

GAME = "soccer"

PLAYERS_PERMUTATIONS = {}
PLAYERS_IN_DB        = {}
SPACE = ' '
THRESHOLD   = 0.90

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



class Revista(VTVSpider):
    name = 'revista_spider'

    def start_requests(self):
        start_urls = ['http://revistaplacar.uol.com.br/static/tabelas/html/v3/htmlCenter/data/deportes/futbol/portugal/pages/pt/planteles.html?h=dfMc-page-88e7b4d0-e84a-467d-9925-b8dd4dd8e651','http://revistaplacar.uol.com.br/static/tabelas/html/v3/htmlCenter/data/deportes/futbol/alemania/pages/pt/planteles.html?h=dfMc-page-34a6c5bd-cd47-42b6-b291-346f99dbde32','http://revistaplacar.uol.com.br/static/tabelas/html/v3/htmlCenter/data/deportes/futbol/premierleague/pages/pt/planteles.html?h=dfMc-page-203fcdfa-b95a-47d6-8327-c5288780d4ef','http://revistaplacar.uol.com.br/static/tabelas/html/v3/htmlCenter/data/deportes/futbol/espana/pages/pt/planteles.html?h=dfMc-page-e85b8dae-5ae0-4437-ab51-7ab34c4cfc4b']

        for ul in start_urls:
            yield Request(ul, callback=self.parse)

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.f = codecs.open('failed_players_file.txt', 'wb', encoding='utf-8')

    def get_db_players(self, source):
        query = 'select id, title from sports_participants where game = "%s" \
            and participant_type = "player"' % source
        self.cursor.execute(query)
        players_data = self.cursor.fetchall()
        for player_data in players_data:
            id_, title = player_data
            title = title.strip().lower()
            PLAYERS_IN_DB[title] = int(id_)
        get_player_permutations(PLAYERS_IN_DB.keys())

    def get_player_name(self, first_name, last_name):
        first_name = first_name.split(' ')
        if isinstance(first_name, list):
            first_name = first_name[0]
        last_name = last_name.split(' ')
        if isinstance(last_name, list):
            last_name = last_name[0]

        name = first_name + " " + last_name

        return name.strip()

    def get_location(self, city, country):
        query = 'select id from sports_locations where country="%s" and state="" and city=""'
        self.cursor.execute(query % country)
        data = self.cursor.fetchone()
        if data:
            data = data[0]
        return data

    def check_player(self, pl_sk):
        self.cursor.execute(SK_QUERY % pl_sk)
        entity_id = self.cursor.fetchone()
        if entity_id:
            pl_exists = True
        else:
            pl_exists = False
        return pl_exists, entity_id

    def check_title(self, name, pl_name):
        birth_date = ''
        _name = "%" + name + "%"
        self.cursor.execute(PL_NAME_QUERY % (_name, GAME))
        try:
            pl_id, pl_gid = self.cursor.fetchone()
        except:
            pl_id = pl_gid = ''
        if not pl_id:
            pl_na = "%" + pl_name+ "%"
            self.cursor.execute(PL_NAME_QUERY % (pl_na, GAME))
            pl_id = self.cursor.fetchone()
        if pl_id:
            query = 'select birth_date from sports_players where participant_id=%s'
            self.cursor.execute(query % (pl_id))
            birth_date = self.cursor.fetchone()
            if birth_date:
                birth_date = str(birth_date[0])

        return pl_id, birth_date, pl_gid

    def add_source_key(self, pl_sk, entity_id):
        if pl_sk and entity_id:
            query = "insert into sports_source_keys (entity_id, entity_type, \
                    source, source_key, created_at, modified_at) \
                    values(%s, %s, %s, %s, now(), now())"
            values = (str(entity_id[0]), 'participant', 'soccerway_soccer', pl_sk)
            self.cursor.execute(query, values)


    def parse(self, response):
        sel = Selector(response)
        self.get_db_players('soccer')
        team_nodes = sel.xpath('//div[contains(@class,"planteles pageCtn page-planteles-tour")]/div[contains(@class,"equipos")]')
        for node in team_nodes:
            team_name = extract_data(node, './div[@class="head"]/span[@class="country"]/span[@class="name"]/text()')
            print team_name
            player_name_nodes = node.xpath('./table[@class="table table-condensed responsive"]/tr[not(contains(@class,"cabezal"))]')[1:]
            for nod in player_name_nodes:
                player_name = extract_data(nod, './td[@class="nombre"]/div[@class="border"]/span[2]/text()')
                player_jercy = extract_data(nod, './td[@class="nombre"]/div[@class="border"]/span[@class="badge"]/text()')
                dob_info = extract_data(nod, './td[@class="nombre"]/div[@class="border"]/div/ul/li[strong[contains(text(),"Data de nascimento")]]/text()')
                dob = dob_info.split('(')[0].strip()
                if dob:
                    birth_date = str(datetime.datetime.strptime(dob, "%d-%m-%Y"))
                else:
                    birth_date = "0000-00-00"
                pl_id, db_birth_date ,pl_gid    = self.check_title(player_name, player_name)
                if pl_gid:
                    if db_birth_date == birth_date:
                        qry = 'insert into sports_titles_regions (entity_id, entity_type, title, iso, created_at, modified_at, aka, short_title, description) values (%s, %s, %s, %s, now(), now(), %s, %s, %s) on duplicate key update modified_at = now()'
                        vals = (pl_id, 'player', player_name, 'PRT', '','','')
                        self.cursor.execute(qry, vals)
                    else:
                        self.f.write('%s<>%s<>%s\n'%(player_name, dob, team_name))
                else:
                    self.f.write('%s<>%s<>%s\n'%(player_name, dob, team_name))
        self.f.flush()



