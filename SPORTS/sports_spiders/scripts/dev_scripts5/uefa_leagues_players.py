import re
import time
import datetime
import MySQLdb
from vtvspider import VTVSpider
from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider import log, get_utc_time
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider import extract_data, get_nodes, extract_list_data
import unicodedata
from StringUtil import cleanString
from difflib import SequenceMatcher
import urllib


ALLOWED_TOURNMANETS = [ 'UEFA Champions League', 'UEFA Europa League']

LEAGUES             = [ 'aut','eng','den','por','sco','ger','gre','esp','sui','tur',
                        'rus','fra','bel','swe','ned','ita', 'rou', 'ukr', 'pol']

LEAGUES = ['den']

ROLES_DICT = {'Midfield': 'Midfielder', 'Right Back': 'Right back'}

PLAYERS_PERMUTATIONS = {}
PLAYERS_IN_DB        = {}
SPACE = ' '
THRESHOLD   = 0.90


def clean_text(data):
    data = data.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ').strip()
    data = data.rstrip().lstrip().strip()
    return data

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="uefa_soccer" and source_key= "%s"'

PL_NAME_QUERY = 'select id from sports_participants where \
title like "%s" and game="%s" and participant_type="player"'

GAME = 'soccer'

class UEFALeaguesPlayers(VTVSpider):
    name = 'uefa_leagues_players'
    allowed_domains = ['www.uefa.com']
    domain_url = "http://www.uefa.com"
    start_urls = []
    roster_details = {}

    def __init__(self):
        self.conn           = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor         = self.conn.cursor()
        self.get_db_players()
        self.all_players = {}
        self.countries   = {}

    def get_location(self, city, country):
        query = 'select id from sports_locations where country="%s" and state="" and city=""'
        self.cursor.execute(query % country)
        data = self.cursor.fetchone()
        if data:
            data = data[0]
        return data

    def add_source_key(self, pl_sk, entity_id):
        if pl_sk and entity_id:
            query = "insert into sports_source_keys (entity_id, entity_type, \
                    source, source_key, created_at, modified_at) \
                    values(%s, %s, %s, %s, now(), now())"
            values = (str(entity_id[0]), 'participant', 'uefa_soccer', pl_sk)
            self.cursor.execute(query, values)

    def start_requests(self):
        top_url = 'http://www.uefa.com/memberassociations/association=%s/index.html'
        for league in LEAGUES:
            url = top_url % league
            yield Request(url, callback=self.parse, meta = {})

    @log
    def parse(self, response):
        sel = Selector(response)
        season = extract_data(sel, '//div[@class="t_standings"]//h3[contains(@class, "bigTitle")]/text()')
        if season:
            ssn = "".join(re.findall(r' (\d+/\d+)', season)) or \
                  "".join(re.findall(r' (\d+-\d+)', season))
            if '/' in season or '-' in ssn:
                season = ssn.replace('/', '-')
            else:
                season = "".join(re.findall(r' (\d+)', season))

        team_links = get_nodes(sel, '//div[@class="t_standings"]//a[contains(@href, "teams")]')
        for link in team_links:
            team_url = extract_data(link, './@href')
            if not "http" in team_url and team_url:
                team_url = self.domain_url + team_url
                team_url = "http://www.uefa.com/teamsandplayers/teams/club=64257/domestic/index.html"
                yield Request(team_url, callback=self.parse_team_details, meta = {'season' : season})

    @log
    def parse_team_details(self, response):
        sel = Selector(response)

        team_id   = ''
        if "club" in response.url:
            team_id = re.findall('/club=(\d+)/', response.url)[0]
        else:
            team_id = re.findall('/team=(\d+)/', response.url)[0]

        team_name = extract_data(sel, '//h1[@class="bigTitle"]//text()')
        root_nodes = get_nodes(sel, '//div[@id="SquadList"]//div')
        for root_node in root_nodes:
            pl_links = get_nodes(root_node, './/table/tr[@class="player"]')
            for node in pl_links[::1]:
                pl_link = extract_data(node, './td/a/@href')
                name = extract_data(node, './td[@class="playername l"]//text()')
                if not pl_link:
                    print name
                else:
                    player_sk = "PL" + re.findall('player=(\d+)', pl_link)[0]
                    pl_exists = self.check_player(player_sk)
                    if pl_exists == False:
                        if "http" not in pl_link:
                            pl_link = self.domain_url + pl_link
                        self.all_players[name] = pl_link

        if self.all_players:
            exactly_present, mostly_present, = '', ''
            not_present, mstly_matched_titles = '', ''
            player_names = self.all_players.keys()

            exactly_present, mostly_present, not_present, mstly_matched_titles \
            = self.find_players_to_add(PLAYERS_IN_DB.keys(), player_names)
            print 'Exactly present >>>> ', len(exactly_present)
            print 'Mostly present >>>>> ', len(mostly_present)
            print 'Not Present>>>>>>>>> ', len(not_present)
            print 'Mostly matched >>>>> ', len(mstly_matched_titles)
 
            if not_present:
                missing_players_data = dict((player, self.all_players[player]) \
                                     for player in not_present if self.all_players.has_key(player))
            else:
                missing_players_data = {}
            matched_players_data = dict((player, self.all_players[player]) \
                                    for player in exactly_present if self.all_players.has_key(player))
            par_matched_players_data = dict((player, self.all_players[player]) \
                                for player in mostly_present  if self.all_players.has_key(player))
            if matched_players_data:
                for value in matched_players_data.values():
                    yield Request(value, self.parse_add_sks)
            for pl_link in missing_players_data.values():
                yield Request(pl_link, self.parse_add_sks)

    def parse_add_sks(self, response):
        hxs = Selector(response)
        name = extract_data(hxs, '//li[span[contains(text(), "Name")]]/text()')
        dob = extract_list_data(hxs, '//li[span[contains(text(), "Date of birth")]]//text()')
        if len([db.replace('(', '') for db in dob[1:] if db!=')']) == 2:
            pl_dob, age = [db.replace('(', '').strip() for db in dob[1:] if db!=')']
            pl_dob = str(datetime.datetime.strptime(pl_dob, '%d/%m/%Y')).split(' ')[0]
        else:
            print 'no dob'
            return
        pl_sk = 'PL' + re.findall('\d+', response.url)[0]
        pl_id, db_dob = self.check_player_title(name)
        if pl_id:
            if pl_dob == db_dob:
                self.add_source_key(pl_sk, pl_id)
                print 'added sk', name
                return
            else:
                print 'dob not matched', name
        country = extract_data(hxs, '//li[span[contains(text(), "Country")]]/text()')
        pl_role = extract_data(hxs, '//li[span[contains(text(), "Position")]]/text()')
        print name
        pl_exists = self.check_player(pl_sk)
        import pdb; pdb.set_trace()
        pl_loc = self.get_location('', country)
        if pl_exists == False:
            self.cursor.execute('select id, gid from sports_participants where id in (select max(id) from sports_participants)')
            table_data = self.cursor.fetchall()
            max_id, max_gid = table_data[0]

            next_id = max_id + 1
            next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').replace('PL', '')) + 1)

            query = "insert into sports_participants (id, gid, title, aka, game, participant_type, image_link, base_popularity, reference_url, location_id, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"
            values = (next_id, next_gid, name, '', 'soccer' ,'player', '', '200', response.url, pl_loc)
            self.cursor.execute(query, values)

            query = "insert into sports_players (participant_id, debut, main_role, roles, gender, age, height, weight, birth_date, birth_place, salary_pop, rating_pop, weight_class, marital_status, participant_since, competitor_since, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now());"
            values = (next_id, '', pl_role, '', 'male', age, '', '', pl_dob, country, '', '', '', '', '', '')
            self.cursor.execute(query, values)

            query = "insert into sports_source_keys (entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, %s, now(), now())"
            values = (next_id, 'participant', 'uefa_soccer', pl_sk)
            self.cursor.execute(query, values)



    def check_player_title(self, name):
        pl_name = "%" + name + "%"
        self.cursor.execute(PL_NAME_QUERY % (pl_name, GAME))
        pl_id = self.cursor.fetchone()
        if pl_id:
            query = 'select birth_date from sports_players where participant_id=%s'
            self.cursor.execute(query, pl_id[0])
            data = self.cursor.fetchone()
            if data:
                pl_dob = str(data[0]).split(' ')[0]
        else:
            pl_dob = ''
        return pl_id, pl_dob
 

    def check_player(self, player_sk):
        self.cursor.execute(SK_QUERY % player_sk)
        data = self.cursor.fetchone()
        if data:
            pl_exists = True
        else:
            pl_exists = False

        return pl_exists

    @log
    def parse_player_details(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        meta = response.meta

        player_sk = "PL" + re.findall('player=(\d+)', response.url)[0]

        role = extract_data(sel, '//ul//li[contains(text(),"Position")]//text()')
        role = re.findall('Position :(.*)', role)
        if role:
            role = role[0].strip()
        else:
            role = ''
        number = extract_data(sel, '//ul//li/span[contains(text(),"Squad number")]/../text()')

        if "/" in meta['team']:
            if "(" in meta['team']:
                team_name   = re.findall('(.*)\(.*\)', meta['team'])
                if team_name:
                    team_name = team_name[0].strip().replace("\n", " ")

        elif "(" in meta['team']:
            team_name  = re.findall('(.*)\(.*\)', meta['team'])
            if team_name:
                team_name = team_name[0].strip().replace("\n", " ")
        else:
            team_name = meta['team'].replace("\n", " ").strip()

        player = {player_sk : {'player_role' : role, \
                          'player_number' : number, 'season' : meta['season'], \
                          'status' : 'active'}}
        self.roster_details.setdefault(meta['team_sk'], {}).update(player)
        record['result_type'] = 'roster'
        record['source'] = 'uefa_soccer'
        record['season'] = meta['season']
        record['participants'] = self.roster_details

        if meta['terminal_crawl']:
            yield record


    def permutations(self, iterable, r=None):
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

    def get_player_permutations(self, existing_players):
        for existing_player in existing_players:
            if SPACE in existing_player:
                existing_player = cleanString(existing_player)
                existing_player_list = existing_player.split(SPACE)
                if len(existing_player_list) <= 5:
                    existing_player_set = [SPACE.join(i) for i in \
                                          self.permutations(existing_player_list)]
                else:
                    existing_player_set = [existing_player]

            else:
                existing_player_set = [existing_player]

            PLAYERS_PERMUTATIONS[existing_player] = existing_player_set

    def get_db_players(self):
        query = 'select id, title from sports_participants where game= "soccer" \
            and participant_type = "player"'
        self.cursor.execute(query)
        players_data = self.cursor.fetchall()
        for player_data in players_data:
            id_, title = player_data
            if type(title) == unicode:
                title = unicodedata.normalize('NFKD', title).encode('ascii','ignore').strip().lower()
            else:
                title = title.strip().lower()
            PLAYERS_IN_DB[title] = int(id_)
        self.get_player_permutations(PLAYERS_IN_DB.keys())

    def find_players_to_add(self, db_players, tou_players):
        titles_present = {}
        mstly_matched_titles = {}
        for player in tou_players:
            player = player.encode('utf-8')
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

