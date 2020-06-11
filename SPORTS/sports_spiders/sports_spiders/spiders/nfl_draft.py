from scrapy.http import Request
import re
from scrapy.selector import Selector
from sports_spiders.vtvspider import VTVSpider, extract_data, \
    get_nodes, extract_list_data, get_birth_place_id, get_sport_id
import MySQLdb
import datetime

PAR_QUERY = "insert into sports_participants (id, gid, title, aka, sport_id, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, \
            birth_place_id, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at, short_title, display_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, %s, now(), now(), %s, %s) on duplicate key update modified_at = now();"

MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="NFL_WIKI" and source_key= "%s"'


PL_NAME_QUERY = 'select P.id from sports_participants P, sports_players PL where P.title="%s" and P.sport_id="%s" and P.id=PL.participant_id and PL.birth_date="%s"'

GAME = 'american football'
SPORT_ID = '4'
PAR_TYPE = 'player'
BASE_POP = "200"
LOC = '0'
DEBUT = "0000-00-00"
ROLES = ''
SAL_POP = ''
RATING_POP = ''
MARITAL_STATUS = ''
PAR_SINCE = COMP_SINCE = ''
WEIGHT_CLASS = AKA = ''


def add_source_key(self, entity_id, _id):
    if _id and entity_id:
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'NFL_WIKI', _id)

        self.cursor.execute(query, values)


def check_player(self, pl_sk):
    self.cursor.execute(SK_QUERY % pl_sk)
    entity_id = self.cursor.fetchone()
    if entity_id:
        pl_exists = True
        pl_id = str(entity_id[0])
    else:
        pl_exists = False
        pl_id = ''
    return pl_exists, pl_id


def check_title(self, name, dob):
    name = name.replace('B. J.', 'B.J.').replace('D. J.', 'D.J.'). \
        replace('K. J.', 'K.J.').replace('T. J.', 'T.J.').replace('III', '')
    name = name.strip()
    self.cursor.execute(PL_NAME_QUERY % (name, SPORT_ID, dob))
    pl_id = self.cursor.fetchone()
    if not pl_id:
        dob = "0000-00-00 00:00:00"
        self.cursor.execute(PL_NAME_QUERY % (name, SPORT_ID, dob))
        pl_id = self.cursor.fetchone()
    return pl_id


class NFLDraft(VTVSpider):
    name = "nfl_draft"
    allowed_domains = []
    start_urls = ['https://en.wikipedia.org/wiki/2017_NFL_draft']

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo",
                                    passwd="veveo123", db="SPORTSDB", charset='utf8',     use_unicode=True)
        self.cursor = self.conn.cursor()
        self.merge_dict = {}
        self.get_merge()

    def get_merge(self):
        merges = open('sports_to_wiki_guid_merge.txt', 'r').readlines()
        for merge in merges:
            if 'PL' in merge.strip():
                wiki_gid, pl_gid = merge.strip().split('<>')
                self.merge_dict[wiki_gid] = pl_gid

    def get_pl_ids(self, pls):
        pl_ids = []
        pl = 'WIKI' + pls
        if self.merge_dict.get(pl, ''):
            pl_gid = self.merge_dict[pl]
        else:
            pl_gid = ''
        if pl_gid:
            query = 'select id from sports_participants where gid=%s'
            self.cursor.execute(query, pl_gid)
            data = self.cursor.fetchone()
            pl_id = data[0]
            pl_ids.append(pl_id)

        return pl_ids

    def parse(self, response):
        sel = Selector(response)
        pl_nodes = get_nodes(sel, '//table[@class="wikitable sortable"]//tr')
        count = 0
        for node in pl_nodes:
            pl_title = extract_data(node, './/td//span[@class="fn"]/a/text()')
            pl_link = extract_data(node, './/td//span[@class="fn"]/a/@href')

            if not pl_title:
                continue

            if "http" not in pl_link:
                pl_link = "https://en.wikipedia.org" + pl_link

            count += 1
            round_ = extract_data(node, './/th[1]//text()')
            if not round_:
                round_ = extract_data(
                    node, './/td[@align="center"][1]//text()').replace('*', '').strip()
            pick = extract_data(node, './/th[2]//text()')
            if not pick:
                pick = extract_data(node, './/td[@align="center"][2]//text()')

            pl_alt_title = extract_data(
                node, './/td//span[@class="fn"]/a/@title')

            if "page does not exist" in pl_alt_title:
                b_date = "0000-00-00 00:00:00"
                pl_id = check_title(self, pl_title, b_date)

                if pl_id:
                    data = {'pick': pick, 'rank': count, 'round': round_}
                    self.populate_draft(pl_id, data)
                    print('Popuated the Draft results for pl_id')

            if pl_link:
                yield Request(pl_link, callback=self.parse_next,
                              meta={'round_': round_, 'pick': pick, 'rank': count})

    def parse_next(self, response):
        sel = Selector(response)
        rank = response.meta['rank']
        pick = response.meta['pick']
        round_ = response.meta['round_']
        pl_link = response.url
        pl_sk = response.url.split('/')[-1]
        wiki_gid = ''.join(re.findall('"wgArticleId":\d+', response.body)). \
            strip('"').replace('wgArticleId":', '').strip('"').strip()
        title = extract_data(
            sel, '//h1[@id="firstHeading"]//text()').split('(')[0].strip()
        dob = extract_list_data(sel, '//span[@class="bday"]//text()')
        if dob:
            dt = datetime.datetime.strptime(dob[0], '%Y-%m-%d')
            b_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            b_date = '0000-00-00 00:00:00'

        pl_id = self.get_pl_ids(wiki_gid)

        if not pl_id:
            pl_id = check_title(self, title, b_date)

        if pl_id:
            data = {'pick': pick, 'rank': rank, 'round': round_}
            self.populate_draft(pl_id, data)
            print('Popuated the Draft results for pl_id')
        else:
            pos = extract_data(sel, '//table[@class="infobox vcard"]//tr//th[contains(text(), "Position:")]//following-sibling::td//a//text()').strip(
            ).replace('Nose Tackle', 'Defensive tackle')
            age = extract_data(
                sel, '//span[@class="noprint ForceAgeToShow"]//text()')
            height = extract_data(
                sel, '//table[@class="infobox vcard"]//tr//th[contains(text(), "Height:")]//following-sibling::td//text()').strip()
            weight = extract_data(
                sel, '//table[@class="infobox vcard"]//tr//th[contains(text(), "Weight:")]//following-sibling::td//text()').strip()
            birth_place = extract_data(
                sel, '//table[@class="infobox vcard"]//tr//th//span[contains(text(), "Place of birth:")]//..//following-sibling::td//text()').strip()
            pl_img = extract_data(
                sel, '//table[@class="infobox vcard"]//tr//td//a[@class="image"]//img//@src')
            if not pl_img:
                pl_img = extract_data(
                    sel, '//div[@class="thumbinner"]//a[class="image"]//img//@src')

            pl_age = age.encode(
                'utf-8').replace('(age\xc2\xa0', '').replace(')', '').strip()

            if "http" not in pl_img and pl_img:
                pl_img = "https:" + pl_img

            pl_height = height.encode(
                'utf-8').split(' ')[0].replace('\xc2\xa0m', ' cm').replace('.', '')
            pl_weight = weight.split('(')[0].strip()

            if "ft" in pl_height:
                pl_height = height.split(
                    '(')[-1].encode('utf-8').replace('\xc2\xa0m)', ' cm').replace('.', '').strip()
            if "lb" in pl_weight:
                pl_weight = weight.split(
                    '(')[-1].encode('utf-8').replace('\xc2\xa0kg)', ' kg')

            pl_exists, pl_id = check_player(self, pl_sk)

            pid = loc_id = city = state = country = ''

            if birth_place and len(birth_place.split(',')) == 2:
                city, state = birth_place.split(',')
                loc_id = get_birth_place_id(
                    city.strip(), state.strip(), country)

            if birth_place and len(birth_place.split(',')) == 3:
                city, state, country = birth_place.split(',')
                loc_id = get_birth_place_id(
                    city.strip(), state.strip(), country.strip())
                birth_place = city + "," + state

            if pl_exists == False:
                pid = check_title(self, title, b_date)
            if pid:
                add_source_key(self, str(pid[0]), pl_sk)
                print('Added the Source key')

            if pl_exists == False and not pid:
                self.cursor.execute(MAX_ID_QUERY)
                pl_data = self.cursor.fetchall()
                max_id, max_gid = pl_data[0]
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').
                                          replace('PL', '')) + 1)
                #sport_id = get_sport_id(GAME)
                values = (next_id, next_gid, title, '', SPORT_ID, 'player', pl_img, 200,
                          response.url, '')
                self.cursor.execute(PAR_QUERY, values)
                values = (next_id, DEBUT, pos, ROLES, 'male',
                          pl_age, pl_height, pl_weight, b_date, birth_place, loc_id, SAL_POP, RATING_POP,
                          WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, '', '')

                self.cursor.execute(PL_QUERY, values)
                add_source_key(self, next_id, pl_sk)
                print(("Added player", title))

    def populate_draft(self, pl_id, data):
        query = 'insert ignore into sports_tournaments_results (tournament_id, participant_id, season, result_type, result_sub_type, result_value, modified_at) values ("%s", "%s", "%s", "%s", "%s", "%s", now())'
        for key in list(data.keys()):
            res_value = data[key]
            values = ('2985', pl_id[0], '2017', 'draft', key, res_value)
            self.cursor.execute(query % values)
