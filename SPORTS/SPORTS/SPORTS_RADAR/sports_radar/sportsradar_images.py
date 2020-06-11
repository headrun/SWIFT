from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import re
import json
import datetime
import MySQLdb
import genericFileInterfaces
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
from scrapy import signals


IMAGE_QUERY = 'insert into sports_radar_images (url_sk, image_url, image_type, height, width, description, image_created, image_updated, league, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s , now(), now()) on duplicate key update modified_at=now()'

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'

IMG_MAP_QUERY = 'insert into sports_radar_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, 0, now(), now()) on duplicate key update modified_at=now()'

SK_CHECK = "select entity_id from sports_source_keys where source=%s and entity_type=%s and source_key=%s"

UP_QRY = 'update sports_radar_images set description=%s, image_created=%s, image_updated=%s,league = %s where url_sk=%s limit 1'

class APIImages(VTVSpider):
    name = "api_images"
    start_urls = []

    today = str(datetime.datetime.now().date())
    url = 'https://api.sportradar.us/%s-images-p%s/usat/players/%s/manifests/all_assets.xml?api_key=%s'
    image_url = 'https://api.sportradar.us/%s-images-p%s/usat%s?api_key=%s'

    apis_dict = {'nfl': {'api-key': '894sgpfhzk57d3uqgu66q9uq', 'version': '2'},
                 'mlb': {'api-key': '8wx5byub6csaptr2exravty6', 'version': '2'},
                 'nhl': {'api-key': 'jus7a4ekgc7tdwktcdztcpzy', 'version': '2'},
                 'nba': {'api-key': 'qqaus38ebkt8jk4cwaskjdhf', 'version': '2'},
                 'ncaamb': {'api-key': '8y92tdnebqj2tdxs7qr92vv9', 'version': '2'},
                 'nascar': {'api-key': 'fcnjtrm7fxnwb5nyedk7byxp', 'version': '2'},
                 'golf': {'api-key': 'rbxxuajx2dxkycdna9yhacz8', 'version': '2'},
                 'ncaafb': {'api-key': 'khre8g93qaqz787x8h6yur7u', 'version': '2'},
                 'mls': {'api-key': '96hpa9c6pzbqk9d625mymzfx', 'version': '2'}}

    #apis_dict = {'ncaafb': {'api-key': 'khre8g93qaqz787x8h6yur7u', 'version': '2'}}



    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.images_file = open('image_details', 'w+')
        self.pl_exists = open('pl_exists_images', 'w+') 
        self.player_images = []
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def start_requests(self):
        for image_type in ['headshots']:
            for key, values in self.apis_dict.iteritems():
                api_key = values['api-key']
                version = values['version']
                req_url = self.url % (key, version, image_type, api_key)
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
            if '/actionshots/' in image:
                image_type = 'actionshots'
            elif '/headshots/' in image:
                image_type = 'headshots'
            image_sk = get_md5(image)
            query = 'select id from sports_radar_images where url_sk=%s'
            self.cursor.execute(query, image_sk)
            count = self.cursor.fetchone()
            if count:
                count = str(count[0])
                values = (description, image_created, image_updated, game,image_sk)
                self.cursor.execute(UP_QRY, values)
            else:
                self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_radar_images' and TABLE_SCHEMA='SPORTSDB'")
                count = str(self.cursor.fetchone()[0])
            league = game
            values = (image_sk, image, image_type, height, width, description, image_created, image_updated, league)
            self.cursor.execute(IMAGE_QUERY, values)
            if player_id:
                values = (player_id, 'player', count)
                self.cursor.execute(IMG_MAP_QUERY, values)
            else:
                self.images_file.write('%s<>%s<>%s<>%s\n' % (player_name, image_sk, game, player_sk))

    def get_player_details(self, name, player_sk, node, game, version, api_key):
        pl_id = ''
        if game == 'nfl':
            key_game = 'football'
        elif game == 'mlb':
            key_game = 'baseball'
        elif game == 'nhl':
            key_game = 'hockey'
        elif game == 'nba':
            key_game = 'basketball'
        elif game == 'ncaamb':
            key_game = 'basketball'
        elif game == 'nascar':
            key_game = 'auto racing'
        elif game == 'ncaafb':
            key_game = 'football'
        elif game == 'golf':
            key_game = 'golf'
        elif game == 'mls':
            key_game = 'soccer'
        query = 'select id from sports_participants where game=%s and title=%s'
        self.cursor.execute(query, (key_game, name))
        data = self.cursor.fetchall()
        data = [str(dt[0]) for dt in data]
        '''if data and len(data) == 1:
            values = (data[0], 'participant', 'radar', player_sk)
            self.cursor.execute(INSERT_SK, values)
            pl_id = data[0]'''
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
    
    def spider_closed(self):
        spider_stats = self.crawler.stats.get_stats()
        start_time   = spider_stats.get('start_time')
        finish_time = spider_stats.get('finish_time')
        spider_stats['start_time'] = str(start_time)
        spider_stats['finish_time'] = str(finish_time)
        query = "insert into WEBSOURCEDB.crawler_summary(crawler, start_datetime, end_datetime, type, count, aux_info, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now())"
        values = (self.name, start_time, finish_time, '','', json.dumps(spider_stats))
        self.cursor.execute(query,values)

