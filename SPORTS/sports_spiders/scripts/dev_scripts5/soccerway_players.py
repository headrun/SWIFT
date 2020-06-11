from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
import unicodedata
from StringUtil import cleanString
from difflib import SequenceMatcher
import MySQLdb
import datetime
import urllib
import time

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="soccerway_soccer" and source_key= "%s"'

PL_NAME_QUERY = 'select id from sports_participants where \
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



class SoccerwayPlayers(VTVSpider):
    start_urls = ['http://int.soccerway.com/teams/club-teams/']
    name       = 'soccerway_players'

    domain_url   = 'http://int.soccerway.com'
    #country_list = ['Uruguay', 'Argentina', 'England', 'Brazil', 'Venezuela', 'Chile', 'France', 'Germany', 'Italy', 'Ecuador', 'Spain', 'Paraguay', 'Japan', 'Norway']

    #country_list = ['Finland']
    country_list = ['England']

    country_dict = {'Uruguay': 'primera-division',
                    'Argentina': 'primera-division',
                    'Brazil': 'serie-a',
                    'Venezuela': 'primera-division',
                    'Chile': 'primera-division',
                    'England': 'conference-national',
                    'France': 'ligue-2',
                    'Germany': '2-bundesliga',
                    'Italy': 'serie-b',
                    'Ecuador': 'primera-a',
                    'Paraguay': 'division-profesional',
                    'Japan': 'j1-league',
                    'Norway': 'eliteserien'}
    country_dict = {'Finland': 'veikkausliiga'}
    country_dict = {'England' : 'premier-league'}

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

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



    def parse(self, response):
        hxs = Selector(response)
        self.get_db_players('soccer')
        leagues = get_nodes(hxs, '//ul[@class="areas"]/li[@class="expandable "]/div[@class="row"]')

        for league in leagues:
            league_name = extract_data(league, './a/text()').strip()
            print league_name
            if league_name in self.country_list:
                league_link = extract_data(league, './a/@href').strip()
                if  "England" in league_name:
                    league_link = self.domain_url + league_link
                    yield Request(league_link, self.parse_league, meta={'league': league_name})

    def parse_league(self, response):
        hxs = Selector(response)
        league = response.meta['league']

        country = extract_data(hxs, '//div[@class="block  clearfix block_competition_left_tree-wrapper"]/h2/text()')
        nodes = get_nodes(hxs, '//ul[@class="left-tree"]/li/a')
        for node in nodes:
            league_link = extract_data(node, './@href')
            for key, value in self.country_dict.iteritems():
                if key in league and value in league_link:
                    league_link = self.domain_url + league_link
                    print league_link
                    if "/national/england/premier-league/20152016/regular-round/" in league_link:
                        yield Request(league_link, self.parse_details, dont_filter=True)

    def parse_details(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//table[@class="leaguetable sortable table detailed-table"]//tr[contains(@class,"team_rank")]')
        for node in nodes:
            team_sk = extract_data(node, './/@data-team_id')
            link = self.domain_url + extract_data(node, './/a[contains(@href, "/teams/")]/@href')
            yield Request(link, self.parse_players, meta={'team_sk': team_sk})

    def parse_players(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//table[@class="table squad sortable"]//tr/td/a[contains(@href, "/players/")] | //a[contains(@href, "/coaches/")]')
        self.season = extract_data(hxs, '//select[@name="season_id"]//option[@selected]/text()').replace('/', '-').strip()
        for node in nodes:
            pl_link     = self.domain_url + extract_data(node, './@href')
            pl_sk       = 'PL' + pl_link.split('/')[-2]
            pl_exists, pl_id = self.check_player(pl_sk)
            if pl_exists == False:
                yield Request(pl_link, self.parse_player_details, meta={'team_sk': response.meta['team_sk']}, dont_filter=True)

    def parse_player_details(self, response):
        hxs = Selector(response)
        main_xpath = '//div[@class="yui-u first"]/div[@class="clearfix"]/dl/dt[contains(text(), "%s")]/following-sibling::dd[1]/text()'
        pl_image   = extract_data(hxs, '//div[@class="yui-u"]/img/@src')
        first_name = extract_data(hxs, main_xpath % ('First name'))
        last_name  = extract_data(hxs, main_xpath % ('Last name'))
        full_name = first_name + " " + last_name
        exactly_present, mostly_present, not_present, mstly_matched_titles \
        = find_players_to_add(PLAYERS_IN_DB.keys(), [full_name])

        name       = self.get_player_name(first_name, last_name)
        #name = full_name
        if name == "Kasim Prosper":
            name = "Prosper Kasim"
        if name ==  "Sabah Lawson":
            name =  "Lawson Sabah"
        if name == "Kyriakos Stamatopoulos":
            name = "Kenny Stamatopoulos"
        birth_dt   = extract_data(hxs, main_xpath % ('Date of birth'))
        age        = extract_data(hxs, main_xpath % ('Age'))
        country    = extract_data(hxs, main_xpath % ('Country of birth'))
        place      = extract_data(hxs, main_xpath % ('Place of birth'))
        height     = extract_data(hxs, main_xpath % ('Height'))
        weight     = extract_data(hxs, main_xpath % ('Weight'))
        foot       = extract_data(hxs, main_xpath % ('Foot'))
        position   = extract_data(hxs, main_xpath % ('Position'))
        if "/coaches" in response.url:
            position = "Head coach"
        if birth_dt:
            birth_date = str(datetime.datetime.strptime(birth_dt, "%d %B %Y"))
        else:
            birth_date = "0000-00-00"
        pl_sk       = 'PL' + response.url.split('/')[-2]
        pl_num = response.url.split('/')[-2]
        pl_name = first_name + ' ' + last_name
        pl_id, db_birth_date     = self.check_title(name, pl_name)
        if pl_id and pl_num not in ['22243','176873','102840','394811','22544','37636','421374','74073','172404','243844','22355','170139','172353','14505','22466','170231','170280','176534','421373','75411','22486','192797','22429','285418','171451','186483','194809','198931','323838','277378','116786','214164','277378','180735','177913','424559','293477','3308432','343317','147050','410529','59785','297576','53945','22570','290488','186175','295473','22667','283622','349589','395377','96540','74448','173082','22493','353435','169740','166526','23729','177996','247116','229704','1962409','176869','351479','351619','180605','22671','22294','192409','174099','398935','428917','14208']:
        #if pl_id and pl_num not in ['428935', '429007', '428936', '247129']:
            if db_birth_date == birth_date:
                self.add_source_key(pl_sk, pl_id)
                print "added source key", name, response.url
                return
            else:
                print "birthdate not matched", name, db_birth_date, birth_date, response.url
                name = unicodedata.normalize('NFKD',name).encode('ascii','ignore')
                file("dobp", "ab+").write("%s %s %s %s \n"%(name, db_birth_date, birth_date, response.url))
        else:
            if exactly_present:
                print 'check and add source key'
            elif not_present:
                print 'player not present , once check and continue'
                pl_exists, pl_id = self.check_player(pl_sk)
                pl_loc = self.get_location('', country)
                if pl_exists == False:
                    pl_name = first_name + ' ' + last_name

                    self.cursor.execute('select id, gid from sports_participants where id in (select max(id) from sports_participants)')
                    table_data = self.cursor.fetchall()
                    max_id, max_gid = table_data[0]

                    next_id = max_id + 1
                    next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').replace('PL', '')) + 1)

                    query = "insert into sports_participants (id, gid, title, aka, game, participant_type, image_link, base_popularity, reference_url, location_id, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"
                    values = (next_id, next_gid, pl_name, '', 'soccer' ,'player', pl_image, '200', response.url, pl_loc)
                    self.cursor.execute(query, values)

                    query = "insert into sports_players (participant_id, debut, main_role, roles, gender, age, height, weight, birth_date, birth_place, salary_pop, rating_pop, weight_class, marital_status, participant_since, competitor_since, created_at, modified_at, display_title, short_title) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now(), %s, %s);"
                    values = (next_id, '', position, '', 'male', age, '', '', birth_date, country, '', '', '', '', '', '', '', '')
                    self.cursor.execute(query, values)

                    query = "insert into sports_source_keys (entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, %s, now(), now())"
                    values = (next_id, 'participant', 'soccerway_soccer', pl_sk)
                    self.cursor.execute(query, values)

                    print "Added player" , name

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
        pl_id = self.cursor.fetchone()
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

        return pl_id, birth_date

    def add_source_key(self, pl_sk, entity_id):
        if pl_sk and entity_id:
            query = "insert into sports_source_keys (entity_id, entity_type, \
                    source, source_key, created_at, modified_at) \
                    values(%s, %s, %s, %s, now(), now())"
            values = (str(entity_id[0]), 'participant', 'soccerway_soccer', pl_sk)
            self.cursor.execute(query, values)

