#from vtvspider import VTVSpider, extract_data, get_nodes
from scrapy.selector import Selector
from scrapy.http import Request
import MySQLdb

CURSOR = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True).cursor()

#CURSOR = MySQLdb.connect(host="10.4.15.132", user="root", db="SPORTSDB_BKP").cursor()

PL_LINK = "http://www.pgatour.com/content/pgatour/players/player.%s.%s.html"

PGA_LINK = "http://www.pgatour.com"

def get_player_name(name):
    name = name.replace('(a)', '')
    name = name.replace(' ', '').split(',')[::-1]
    pl_name = name[0] + " " + name[1]
    return pl_name

def add_source_key(_id, entity_id):
    if _id and entity_id:
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now())"
        values = (entity_id, 'participant', 'champions_golf', _id)
        CURSOR.execute(query, values)

class ChampionsUsplayers(VTVSpider):
    name = "champions_players"
    start_urls = ['http://www.usga.org/scoring/2014/SeniorOpen/dyn/tee1.xml']

    def parse(self, response):
        hxs = Selector(response)
        players = get_nodes(hxs, '//players/player')
        for player in players:
            _id = extract_data(player, './@idint')
            name = extract_data(player, './@name')
            name = get_player_name(name)
            l_name = name.lower().replace(' ', '-')
            if _id:
                query = 'select entity_id from sports_source_keys where entity_type = "participant" and source = "champions_golf" and source_key = "%s"' % (_id)
                CURSOR.execute(query)
                entity_id = CURSOR.fetchone()
                if not entity_id:
                    query = 'select id from sports_participants where game = "golf" and title = "%s"' % name
                    CURSOR.execute(query)
                    pl_name = CURSOR.fetchone()
                    if pl_name:
                        pl_id = pl_name[0]
                        add_source_key(_id, pl_id)
                    else:
                        pl_link = PL_LINK % (_id, l_name)
                        yield Request(pl_link, callback =self.parse_player, meta ={'_id': _id, 'pl_name': name})

    def parse_player(self, response):
        hxs = Selector(response)
        _id = response.meta['_id']
        pl_name = response.meta['pl_name']
        nodes = get_nodes(hxs, '//div[@class="player-bio-frame1 hidden-small"]//ul[@class="player-bio-cols"]')

        main_role = aka = height = weight = age = birth_place = image = \
        country = name = roles = weight_class = marital_status = \
        participant_since = competitor_since = ref = ''

        birth_date = "0000-00-00 00:00:00"
        if nodes:
            for node in nodes:
                image = extract_data(node, './/li[@class="col1"]//@src')
                name = extract_data(node, './/li[@class="col2"]/h3/text()')
        else:
            nodes = get_nodes(hxs, '//div[@class="player-info-banner"]')
            for node in nodes:
                name = extract_data(node, './/div[@class="name"]/text()')
                country = extract_data(node, './/span[@class="country"]//text()')
                image = PGA_LINK + extract_data(node, './/span[@class="country"]//@src')

        game = 'golf'
        participant_type = 'player'
        basepop = "200"
        ref = response.url
        loc = '0'
        debut = "0000-00-00"
        salary_pop = ''
        rating_pop = ''
        birth_place = country
        gender = 'male'
        if not name:
            name = pl_name

        if name:
            CURSOR.execute('select id, gid from sports_participants where id in (select max(id) from sports_participants)')
            pl_data = CURSOR.fetchall()
            max_id, max_gid = pl_data[0]
            next_id = max_id + 1
            next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').replace('PL', '')) + 1)
            query = "insert into sports_participants (id, gid, title, aka, game, participant_type, image_link, base_popularity, reference_url, location_id, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"
            values = (next_id, next_gid, name, aka, game, participant_type, image, basepop, ref, loc)
            CURSOR.execute(query, values)

            query = "insert into sports_players (participant_id, debut, main_role, roles, gender, age, height, weight, birth_date, birth_place, salary_pop, rating_pop, weight_class, marital_status, participant_since, competitor_since, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now());"
            values = (next_id, debut, main_role, roles, gender, age, height, weight, birth_date, birth_place, salary_pop, rating_pop, weight_class, marital_status, participant_since, competitor_since)
            CURSOR.execute(query, values)

            add_source_key(_id, next_id)
