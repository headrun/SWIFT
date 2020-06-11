'''import datetime
import re
import time
import MySQLdb
import genericMergeLib
import urllib
from lxml import etree
from difflib import SequenceMatcher
from urlparse import urlparse
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, \
extract_data, extract_list_data, get_nodes, get_utc_time
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


true = True
false = False
null = ''

PLAYERS_IN_DB = {}
PLAYERS_PERMUTATIONS = {}
SPACE = ' '

CURSOR = MySQLdb.connect(host="10.4.15.132", user="root", \
            db="SPORTSDB_BKP").cursor()


def permutations(iterable, r=None):
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
def vtv_compress_space(s):
    s = s.strip()
    return re.sub('\s+',' ',s)

def cleanString(s, make_lower=True):
    s = s.replace("'s ", " ")
    s = s.replace("`s ", " ")
    s = s.replace("'", "")
    s = s.replace("`", "")
    s = genericMergeLib.convert_latin1_ascii(s, False)
    s = str(s).strip()
    s = vtv_compress_space(s)

    return s

def get_player_permutations(existing_players):
    global PLAYERS_PERMUTATIONS

    for existing_player in existing_players:
        if SPACE in existing_player:
            existing_player = cleanString(existing_player)
            existing_player_list = existing_player.split(SPACE)
            if len(existing_player_list) <= 5:
                existing_player_set = [SPACE.join(i) for i in permutations(existing_player_list)]
            else:
                print '%s is too large player to compare' % existing_player
                existing_player_set = [existing_player]
        else:
            existing_player_set = [existing_player]

        PLAYERS_PERMUTATIONS[existing_player] = existing_player

def get_db_players(CURSOR, source):
    global PLAYERS_IN_DB

    query = 'select id, title from sports_participants where game = "%s" and participant_type = "player";' % source
    CURSOR.execute(query)
    players_data = CURSOR.fetchall()
    for player_data in players_data:
        id, title = player_data
        title = title.strip().lower()
        title = modify(title)
        PLAYERS_IN_DB[title] = int(id)

    get_player_permutations(PLAYERS_IN_DB.keys())

def get_full_url(exp_url, base_url):
    url_data = urlparse(exp_url)
    if isinstance(url_data, tuple):
        scheme, domain, path = url_data[:3]
    else:
        scheme, domain, path = url_data.scheme, url_data.netloc, url_data.path

    if not path:
        print "Path is mandatory, Given Url: %s" % exp_url
        return exp_url
    else:
        path = path.strip()

    if domain and scheme:
        return exp_url

    if not path.startswith('/'):
        path += '/'

    base_url_data = urlparse(base_url)
    if isinstance(base_url_data, tuple):
        base_scheme, base_domain = base_url_data[:2]
    else:
        base_scheme, base_domain = base_url_data.scheme, base_url_data.netloc

    if not base_scheme and not base_domain:
        print "Provide valid Base Url, Given Url: %s" % base_url
        return exp_url
    return base_scheme + '://' + base_domain + path

def modify(data):
    try:
        data = ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
        return data
    except ValueError or UnicodeDecodeError or UnicodeEncodeError:
        try:
            return data.encode('utf8')
        except  ValueError or UnicodeEncodeError or UnicodeDecodeError:
            try:
                return data
            except ValueError or UnicodeEncodeError or UnicodeDecodeError:
                try:
                    return data.encode('utf-8').decode('ascii')
                except UnicodeDecodeError:
                    data = normalize('NFKD', data.decode('utf-8')).encode('ascii')
                    return data
def get_def_date(data):
    if not data:
        data = "0000-00-00 00:00:00"
    return data

def get_db_players(CURSOR, source):
    global PLAYERS_IN_DB

    query = 'select id, title from sports_participants where game = "%s" and participant_type = "player";' % source
    CURSOR.execute(query)
    players_data = CURSOR.fetchall()
    for player_data in players_data:
        id, title = player_data
        title = title.strip().lower()
        title = modify(title)
        PLAYERS_IN_DB[title] = int(id)

    get_player_permutations(PLAYERS_IN_DB.keys())

PLAYERS_IN_DB = {}
PLAYERS_PERMUTATIONS = {}
SPACE = ' '

REPLACE_WORDS = [" (AM)", " (Am)", " (am)", " (I)", " (Usa)"]

def update_populate(sk = "", name = "", image = "", height = "", weight = "", date_birth = "", link = ""):
    query = 'select entity_id from sports_source_keys where entity_type = "participant" and source = "%s" and source_key = %s' % (source, sk)
    CURSOR.execute(query)
    data = CURSOR.fetchone()
    failed_count = 0
    if not data:
        failed_count += 1
        missing_players[sk] = {'ref_link' : link, 'img_link': image}
    else:

        player_id = data[0]

        update_query = 'update sports_participants set image_link= "%s",reference_url = "%s" where id = %s' % (image, link, player_id)
        CURSOR.execute(update_query)

        update_query_1 = 'update sports_players set height="%s",weight="%s",birth_date="%s" where participant_id = %s' % (height, weight, date_birth, player_id)
        CURSOR.execute(update_query_1)

def insert_populate(name = "", aka = "", game = "golf", participant_type = "player", image = "", bp = 0, link = "", loc = 0, debut = "0000-00-00 00:00:00", main_role = "", roles = "", gender = "male", age = 0, height = "", weight = "", birth_date = "0000-00-00 00:00:00", salary_pop = "", rating_pop = "", weight_class = "", marital_status = "", participant_since = "0000-00-00 00:00:00", competitor_since = "0000-00-00 00:00:00", source = "", sk = "", country = "", city = ""):
    CURSOR.execute('select id, gid from sports_participants where id in (select max(id) from sports_participants)')
    tou_data = CURSOR.fetchall()
    max_id, max_gid = tou_data[0]

    next_id = max_id+1
    next_gid = 'PL'+str(int(max_gid.replace('TEAM', '').replace('PL', ''))+1)

    aka = "###".join(aka.split('<>'))
    CURSOR.execute('select id from sports_locations where city = "%s" and country = "%s"' % (city, country))
    loc_id = CURSOR.fetchone()
    if loc_id:
        birth_place = loc_id[0]
    else:
        birth_place = ""
    query = 'insert ignore into sports_participants (id, gid, title, aka, game, participant_type, image_link, base_popularity, reference_url, location_id, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())'
    values = (next_id, next_gid, name, aka, game, participant_type, image, bp, link, loc)
    try:
        CURSOR.execute(query, values)
        print query, values
    except:
        CURSOR.execute((query, values).decode('utf-8', 'ignore'))
        print query, values
    query = "insert ignore into sports_players (participant_id, debut, main_role, roles, gender, age, height, weight, birth_date, birth_place, salary_pop, rating_pop, weight_class, marital_status, participant_since, competitor_since, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now());"
    values =  (next_id, debut, main_role, roles, gender, age, height, weight, birth_date, birth_place, salary_pop, rating_pop, weight_class, marital_status, participant_since, competitor_since)
    CURSOR.execute(query, values)

    query = "insert ignore into sports_source_keys (entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE source_key = %s"
    values = (next_id, 'participant', source, sk, sk)
    CURSOR.execute(query, values)
THRESHOLD   = 0.90
def find_players_to_add(db_players, tou_players):
    titles_present = {}
    mstly_matched_titles = {}
    for player in tou_players:
        if player in db_players:
            titles_present[player] = (player, 'EXACTLY PRESENT')
            continue

        check_flag = False
        seqMatcher = SequenceMatcher(None, player)
        for existing_player in db_players:
            if SPACE in player and SPACE in existing_player:
                existing_player_set = player_permutations.get(existing_player, [existing_player])
            else:
                existing_player_set = [existing_player]

            for ex_player in existing_player_set:
                seqMatcher.set_seq2(ex_player)
                if seqMatcher.ratio() >= THRESHOLD:
                    mstly_matched_titles[player] = existing_player
                    titles_present[player] = (player, 'MOSTLY PRESENT')
                    check_flag = True
                    break
            if check_flag: break
        else:
            titles_present[player] = (player, 'NOT PRESENT')

    mostly_present = [player for player, details in titles_present.items() if details[1] == 'MOSTLY PRESENT']
    exactly_present = [player for player, details in titles_present.items() if details[1] == 'EXACTLY PRESENT']
    not_present = [player for player, details in titles_present.items() if details[1] == 'NOT PRESENT']

    return exactly_present, mostly_present, not_present, mstly_matched_titles

def get_partl_match(player_name, titles_map):
    player_map = titles_map.get(player_name, '')
    if player_map:
        return PLAYERS_IN_DB.get(player_map, 0)
    else:
        return 0

SK_CHECK_QUERY = "select entity_id from sports_source_keys where \
                  entity_type=%s and source_key=%s and source=%s"

def check_source_keys(sou_key, entity_type, source):
    CURSOR.execute(SK_CHECK_QUERY, (entity_type, sou_key, source))
    res = [str(i[0]) for i in CURSOR.fetchall()]
    res_ = "<>".join(res)
    if len(res) == 1:
        return 1, res_
    elif len(res) == 0:
        return 0, res_
    else:
        return 2, res_


def create_source_key(players_data, source, entity_type, titles_map={}):
    existing_count, created_count = 0, 0
    for player_name, player_id in players_data.iteritems():
        try:
            exsisting_player = PLAYERS_IN_DB[player_name]
        except:
            exsisting_player = get_partl_match(player_name, titles_map)

        if not exsisting_player:
            print "Failed to Get existing player id , Name: %s --- Id: %s" % (player_name, player_id)
            continue
        existance, result = check_source_keys(player_id, entity_type, source)
        if existance == 1:
            query = 'select A.id, A.title, A.aka, A.participant_type, B.source_key, B.source from sports_participants A, sports_source_keys B where A.id = B.entity_id and B.entity_type = "participant" and B.source_key = "%s";' % player_id
            existing_count += 1
        else:
            query = 'INSERT IGNORE INTO sports_source_keys (entity_id, entity_type, source, source_key, created_at) VALUES (%s, "%s", "%s", "%s", now())'
            query = query % (exsisting_player, entity_type, source, player_id)
            CURSOR.execute(query)
            created_count += 1

    return existing_count, created_count



player_name_map = {}
player_permutations = {}
class EuroRoster(VTVSpider):
    name = "euro_players"
    start_urls = ['http://www.europeantour.com/europeantour/players/atoz/index.html']
    #CURSOR = create_CURSOR('10.4.15.132', 'SPORTSDB_BKP')
    get_db_players(CURSOR, 'golf')
    missing_players_file = open('missing_players', 'wb')
    outfile = open('out_file.txt', 'w')

    def parse(self, response):
        hxs = Selector(response)
        source = "euro_golf"
        nodes  = get_nodes(hxs, '//ul[@class="letters-list"]/li')
        for node in nodes:
            name = extract_data(node, './a/text()')
            links = "http://www.europeantour.com/library/playerdata/search/cupkindid=1/letter=" + name + "/_searchPlayer.html"
            res = urllib.urlopen(links)
            data = res.read()
            data_x = etree.HTML(data)
            players_links = get_nodes(data_x, '//table//tr/td/a')
            for player_link in players_links:
                player_ref = extract_data(player_link, './@href')
                player_det = extract_data(player_link, './text()')
                player_det = modify(player_det)
                for i in REPLACE_WORDS:
                    player_det = player_det.replace(i, '').strip()
                if player_ref:
                    _id = re.findall(r'playerid=(\d+)', player_ref)
                    if _id and len(_id) == 1:
                        player_id = _id[0].strip()
                    else:
                        player_id = 0
                else:
                    player_id = 0

                if player_id and not player_id.isdigit():
                    player_id = 0
                if ' ' in player_det:
                    player_det = [i.strip() for i in player_det.split(' ') if i]
                    if len(player_det) == 2:
                        f_name, l_name = player_det
                    elif len(player_det) == 3:
                        f_name, ext, l_name = player_det
                        f_name = f_name +' '+ ext
                    elif len(player_det) == 4:
                        f_name, l_name = player_det[0], player_det[-1]
                        f_name = f_name + ' ' + player_det[1]
                        l_name = player_det[-2] + ' ' + l_name
                    elif len(player_det) > 4:
                        f_name, l_name = player_det[0], player_det[-1]
                        f_name = f_name + ' ' + player_det[1] + ' ' + player_det[2]
                        l_name = player_det[-2] + ' ' + l_name
                    else:
                        f_name, l_name = player_det, ''
                else:
                    f_name, l_name = player_det, 'ck = '

                if player_ref and 'http' not in player_ref:
                    player_ref = get_full_url(player_ref, response.url)

                if not f_name or not player_id:
                    print "Failed to get Player Info, Id: %s --- Title: %s %s --- Ref: %s" % (player_id, f_name, l_name, player_ref)
                    continue
                player_name = ' '.join(i.strip() for i in [f_name, l_name] if i).lower()
                player_name_map[player_name.lower()] = (player_id, player_ref)
            player_names = player_name_map.keys()
            exactly_present, mostly_present, = '', ''
            not_present, mstly_matched_titles = '', ''
            exactly_present, mostly_present, not_present, mstly_matched_titles = find_players_to_add(PLAYERS_IN_DB.keys(), player_names)
            total_len = len(exactly_present) + len(mostly_present) + len(not_present)
            if not_present:
                missing_players_data = dict((player, player_name_map[player]) for player in not_present if player_name_map.has_key(player))
                print "Len of Missing: %s --- Len of players data: %s" % (len(not_present), len(missing_players_data))
                self.missing_players_file.write('%s\n' %repr(missing_players_data))
            else:
                missing_players_data = {}
            matched_players_data = dict((player, player_name_map[player][0]) for player in exactly_present if player_name_map.has_key(player))
            par_matched_players_data = dict((player, player_name_map[player][0]) for player in mostly_present if player_name_map.has_key(player))

            players_count = 0
            e_pr_existing, e_pr_created = create_source_key(matched_players_data, source, 'participant')
            players_count += len(matched_players_data)

            p_pr_existing, p_pr_created = create_source_key(par_matched_players_data, source, 'participant', titles_map = mstly_matched_titles)
            players_count += len(par_matched_players_data)

            for player_name, player_data in player_name_map.iteritems():
                is_existing = True
                if missing_players_data.has_key(player_name):
                    is_existing = False
                    print "Need to create new record for player_id -- %s" % (player_id)
                player_id, player_ref = player_data
                yield Request(player_ref, callback = self.parse_euro_details, meta = {'source' : source, 'sk' : player_id, 'CURSOR': CURSOR, 'is_existing': is_existing, 'name' : player_det, 'missing_players_data' : missing_players_data})

    def parse_euro_details(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        source = "euro_golf"
        sk = response.meta['sk']
        dob = extract_data(hxs, '//table[@class="biography"]/tr/td[contains(text(),"Date of Birth:")]/following-sibling::td/text()')
        p_name = extract_data(hxs, '//div[@class="player-name"]/h1/text()').title()
        for i in REPLACE_WORDS:
            p_name = p_name.replace(i, '').strip()
        try:
            p_name = modify(p_name)
        except:
            p_name = modify(p_name)

        if dob:
            dob = time.strptime(dob, "%d/%m/%Y")
            dob = "%s-%s-%s" % (dob.tm_year, dob.tm_mon, dob.tm_mday)
        p_image = "http://www.europeantour.com" + extract_data(hxs, '//div[@class="player-pic"]/img/@src')
        if not p_image:
            p_image = "http://www.europeantour.com/imgml/newsnopic/600.gif"

        height_weight = extract_data(hxs, '//table[@class="biography"]/tr/td[contains(text(),"Height / Weight")]/following-sibling::td/text()')
        height_1 = ''.join(re.findall(r'(\d+)cm', height_weight))
        height_2 = ''.join(re.findall(r'(\d+) cm', height_weight))
        if height_1:
            height = height_1 + "cm"
        else:
            height = height_2 + "cm"

        if "-" in height:
            height = height.replace("-", "'")

        weight_1 = ''.join(re.findall(r'(\d+)kgs', height_weight))
        weight_2 = ''.join(re.findall(r'(\d+) kgs', height_weight))
        if weight_1:
            weight = weight_1 + "kgs"
        else:
            weight = weight_2 + "kgs"

        if "," in weight:
            weight = weight.replace(",", "")
        if response.meta['is_existing'] == True:
            print "update"
            #update_populate(sk = sk, image = p_image, height = height, weight = weight, date_birth = dob, link = response.url, name = p_name)
        elif response.meta['is_existing'] == False:
            insert_populate(sk = sk, image = p_image, height = height, weight = weight, birth_date = dob, source = source, link = response.url, name = p_name)

        if response.meta['missing_players_data']:
            self.outfile.write('%s\n' %repr(response.meta['missing_players_data']))
        record['participant_type'] = "player"
        record['source'] = "euro_golf"
        record['participants'] = '' '''



