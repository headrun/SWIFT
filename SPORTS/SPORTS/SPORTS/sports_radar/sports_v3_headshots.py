from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import re
import json
import datetime
import MySQLdb
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector


IMAGE_QUERY = 'insert into sports_images (url_sk, image_url, image_type, height, width, description, image_created, image_updated, league, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s , now(), now()) on duplicate key update modified_at=now()'

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'

IMG_MAP_QUERY = 'insert into sports_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, 0, now(), now()) on duplicate key update modified_at=now()'

SK_CHECK = "select entity_id from sports_source_keys where source=%s and entity_type=%s and source_key=%s"

UP_QRY = 'update sports_images set description=%s, image_created=%s, image_updated=%s,league = %s where url_sk=%s limit 1'

SP_IMG_RIGHT_ID = 'insert into sports_image_rights_mapping(image_id, right_id, created_at, modified_at) values(%s, %s, now(), now()) on duplicate key update modified_at=now()'

class HeadshotsV3Images(VTVSpider):
    name = "headshotsv3_images"
    start_urls = []

    today = str(datetime.datetime.now().date())
    url = 'https://api.sportradar.us/%s-images-p%s/usat/headshots/players/%s/manifest.xml?api_key=%s'
    image_url = 'https://api.sportradar.us/%s-images-p%s/usat%s?api_key=%s'

    apis_dict = {'nfl': {'api-key': 'udpvrhreg5nqdp86h5mbhvbz', 'version': '3'},
                 'mlb': {'api-key': 'fchuxvp995h9c8wyh325t54t', 'version': '3'},
                 'nhl': {'api-key': 'tdmzav36syyamm4uhqvzbqar', 'version': '3'},
                 'nba': {'api-key': 'ux3pbej3jnqvyss2kfjjb2v8', 'version': '3'}}



    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.images_file = open('image_v3_details', 'w+')
        self.pl_exists = open('pl_exists_v3_images', 'w+') 
        self.player_images = []

    def start_requests(self):
        for key, values in self.apis_dict.iteritems():
            api_key = values['api-key']
            version = values['version']
            year = '2017'
            req_url = self.url % (key, version, year, api_key)
            yield Request(req_url, self.parse, meta={'game': key, 'version': version, 'api-key': api_key})

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        game = response.meta['game']
        version = response.meta['version']
        api_key = response.meta['api-key']
        nodes = get_nodes(sel, '//assetlist/asset')
        for node in nodes:
            player_sk = extract_data(node, './@player_id')
            player_id = self.get_player_id(player_sk)
            if player_id:
                self.populate_images(node, player_id, game, version, api_key)
            else:
                name = extract_data(node, './/title/text()')
                self.get_player_details(name, player_sk, node, game, version, api_key)

    def populate_images(self, node, player_id, game, version, api_key):
        player_name = extract_data(node, './/title/text()')
        images = get_nodes(node, './links/link')
        player_sk = extract_data(node, './@player_id')
        image_created = extract_data(node, './@created')
        image_updated = extract_data(node, './@updated')
        description  = extract_data(node, './/description//text()').replace('amp;', '').strip()
        for image_node in images:
            pl_image = extract_data(image_node, './/@href')
            height = extract_data(image_node, './@height')
            width  = extract_data(image_node, './@width')
            image = self.image_url % (game, version, pl_image, api_key)
            image_type = 'headshots'
            image_sk = get_md5(image)
            query = 'select id from sports_images where url_sk=%s'
            self.cursor.execute(query, image_sk)
            count = self.cursor.fetchone()
            if count:
                count = str(count[0])
                values = (description, image_created, image_updated, game,image_sk)
                self.cursor.execute(UP_QRY, values)
            else:
                self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_images' and TABLE_SCHEMA='SPORTSDB'")
                count = str(self.cursor.fetchone()[0])
            league = game
            values = (image_sk, image, image_type, height, width, description, image_created, image_updated, league)
            self.cursor.execute(IMAGE_QUERY, values)

            right_values = (count, '2')
            self.cursor.execute(SP_IMG_RIGHT_ID, right_values)

            if player_id:
                values = (player_id, 'player', count)
                self.cursor.execute(IMG_MAP_QUERY, values)
            else:
                self.images_file.write('%s<>%s<>%s<>%s\n' % (player_name, image_sk, game, player_sk))

    def get_player_details(self, name, player_sk, node, game, version, api_key):
        pl_id = ''
        if game == 'nfl':
            key_game = '4'
        elif game == 'mlb':
            key_game = '1'
        elif game == 'nhl':
            key_game = '3'
        elif game == 'nba':
            key_game = '2'
        query = 'select id from sports_participants where sport_id=%s and title=%s'
        self.cursor.execute(query, (key_game, name))
        data = self.cursor.fetchall()
        data = [str(dt[0]) for dt in data]
        if data and len(data) == 1:
            values = (data[0], 'participant', 'radar', player_sk)
            self.cursor.execute(INSERT_SK, values)
            pl_id = data[0]
        if data:
            rec = '<>'.join(data)
            record = '%s<>%s<>%s' % (rec, name, player_sk)
            self.pl_exists.write('%s\n' % record)
        self.populate_images(node, pl_id, game, version, api_key)


    def get_player_id(self, sk):
        self.cursor.execute(SK_CHECK , ('radar', 'participant', sk))
        data = self.cursor.fetchone()
        if data:
            data = str(data[0])

        return data
    

