from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import re
import datetime
import MySQLdb

IMAGE_QUERY = 'insert into sports_radar_images (url_sk, image_url, image_type, height, width, description, image_created, image_updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'

IMG_MAP_QUERY = 'insert into sports_radar_images_mapping(entity_id, entity_type, image_id, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

SK_CHECK = "select entity_id from sports_source_keys where source=%s and entity_type=%s and source_key=%s"

UP_QRY = 'update sports_radar_images set description=%s, image_created=%s, image_updated=%s where url_sk=%s limit 1'

class SoccerAPIImages(VTVSpider):
    name = "soccerapi_images"
    start_urls = []

    today = str(datetime.datetime.now().date())
    url = 'https://api.sportradar.us/%s-images-p%s/%s/%s/headshots/players/%s/manifest.xml?api_key=%s'
    image_url = 'https://api.sportradar.us/%s-images-p%s/%s%s?api_key=%s'

    apis_dict = {'bundesliga': {'api-key': '8vk2yjmu8a6fv8evv2eyuu3f', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'},
                 'epl': {'api-key': 'tq9dv7vy2mz3tuq6f5ymez8y', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'},
                 'la-liga': {'api-key': 'xvensng8xmtnczbw3pnze4t3', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'}, 
                 'serie-a': {'api-key': 'hbuvfr86gryc4eqq5jdftcqu', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'},
                 'ligue-1': {'api-key': '8y9a9gjspp8bcykxnn4cj2wk', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'}

                 }



    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()
        self.images_file = open('soccer_image_details', 'w+')
        self.pl_exists = open('soccer_pl_exists_images', 'w+') 
        self.player_images = []

    def start_requests(self):
        for image_year in ['2016', '2015']:
            for key, values in self.apis_dict.iteritems():
                api_key = values['api-key']
                version = values['version']
                sport = values['sport']
                provider = values['provider']
                req_url = self.url % (sport, version, provider, key, image_year, api_key)
                yield Request(req_url, self.parse,  \
                meta={'game': key, 'version': version, \
                'api-key': api_key, 'provider': provider, 'sport': sport})

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        game = response.meta['game']
        version = response.meta['version']
        api_key = response.meta['api-key']
        provider = response.meta['provider']
        sport = response.meta['sport']
        nodes = get_nodes(sel, '//assetlist/asset')
        for node in nodes:
            player_sk = extract_data(node, './@player_id')
            player_id = self.get_player_id(player_sk)
            if player_id:
                self.populate_images(node, player_id, sport, version, provider, api_key)
            else:
                name = extract_data(node, './/title/text()')
                self.get_player_details(name, player_sk, node, version, sport, api_key)

    def populate_images(self, node, player_id, sport, version, provider, api_key):
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
            image = self.image_url % (sport, version, provider, pl_image, api_key)
            if '/headshots/' in image:
                image_type = 'headshots'
            image_sk = get_md5(image)
            query = 'select id from sports_radar_images where url_sk=%s'
            self.cursor.execute(query, image_sk)
            count = self.cursor.fetchone()
            if count:
                count = str(count[0])
                values = (description, image_created, image_updated, image_sk)
                self.cursor.execute(UP_QRY, values)
            else:
                self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_radar_images' and TABLE_SCHEMA='SPORTSDB'")
                count = str(self.cursor.fetchone()[0])
            values = (image_sk, image, image_type, height, width, description, image_created, image_updated)
            self.cursor.execute(IMAGE_QUERY, values)
            if player_id:
                values = (player_id, 'player', count)
                self.cursor.execute(IMG_MAP_QUERY, values)
            else:
                self.images_file.write('%s<>%s<>%s<>%s\n' % (player_name, image_sk, game, player_sk))



    def get_player_details(self, name, player_sk, node, version, sport, api_key):
        pl_id = ''
        key_game = sport
        query = 'select id from sports_participants where game=%s and title=%s'
        self.cursor.execute(query, (key_game, name))
        data = self.cursor.fetchall()
        data = [str(dt[0]) for dt in data]
        if data:
            rec = '<>'.join(data)
            record = '%s<>%s<>%s' % (rec, name, player_sk)
            self.pl_exists.write('%s\n' % record)


    def get_player_id(self, sk):
        self.cursor.execute(SK_CHECK , ('radar', 'participant', sk))
        data = self.cursor.fetchone()
        if data:
            data = str(data[0])

        return data

