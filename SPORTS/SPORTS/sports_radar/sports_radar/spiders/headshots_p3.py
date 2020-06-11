from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import datetime
import MySQLdb

IMAGE_QUERY = 'insert into sports_images (url_sk, image_url, image_type, height, width, description, image_created, image_updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'

IMG_MAP_QUERY = 'insert into sports_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, %s,now(), now()) on duplicate key update modified_at=now()'

SK_CHECK = "select entity_id from sports_source_keys where source=%s and entity_type=%s and source_key=%s"

UP_QRY = 'update sports_images set description=%s, image_created=%s, image_updated=%s where url_sk=%s limit 1'

SP_IMG_RIGHT_ID = 'insert into sports_image_rights_mapping(image_id, right_id, created_at, modified_at) values(%s, %s, now(), now()) on duplicate key update modified_at=now()'

class SoccerAPIImages(VTVSpider):
    name = "api_headshots"
    start_urls = []

    today = str(datetime.datetime.now().date())
    url = 'https://api.sportradar.us/%s-images-p%s/%s/headshots/players/%s/manifest.xml?api_key=%s'
    image_url = 'https://api.sportradar.us/%s-images-p%s/%s%s?api_key=%s'

#    apis_dict = {'cricket': {'api-key': '2aumnpe8yfa2fyk469g464pz', 'version': '3', 'sport': 'cricket', 'provider': 'reuters'},}
    apis_dict = {'rugby': {'api-key': 'v5k8n3syyg4kexmyh8dyf2s5', 'version': '3', 'sport': 'rugby', 'provider': 'reuters'},}
#    apis_dict = {'f1': {'api-key': 'gyx2zqve6y4323pjsgf737wk', 'version': '3', 'sport': 'f1', 'provider': 'reuters'},}



    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.216.45", user="veveo", passwd = 'veveo123', db="SPORTSDB_DEV")
        self.cursor = self.conn.cursor()
        self.images_file = open('headshots_image_details', 'w+')
        self.pl_exists = open('headshots_pl_exists_images', 'w+') 
        self.player_images = []

    def start_requests(self):
        for image_year in ['2016', '2015']:
            for key, values in self.apis_dict.iteritems():
                api_key = values['api-key']
                version = values['version']
                sport = values['sport']
                provider = values['provider']
                req_url = self.url % (sport, version, provider, image_year, api_key)
                yield Request(req_url, self.parse,  \
                meta={'game': key, 'version': version, \
                'api-key': api_key, 'provider': provider, 'sport': sport,
                'proxy': 'http://game-dynamics-proxy-dev-0-489291303.us-east-1.elb.amazonaws.com:8080/'},
                headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})

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
            image = self.image_url % (sport, version, provider, '/'+'/'.join(pl_image.split('/')[2:]), api_key)
            if '/headshots/' in image:
                image_type = 'headshots'
            image_sk = get_md5(image)
            query = 'select id from sports_images where url_sk=%s'
            self.cursor.execute(query, image_sk)
            count = self.cursor.fetchone()
            if count:
                count = str(count[0])
                values = (description, image_created, image_updated, image_sk)
                self.cursor.execute(UP_QRY, values)
                entity_qry = 'select entity_type from sports_images_mapping where id in(select id from sports_images where url_sk=%s)'
                self.cursor.execute(entity_qry,image_sk)
                entity_type = self.cursor.fetchone()
                UP_MAP_QRY = "update sports_images_mapping set is_primary =%s where image_id in(select id from sports_images where url_sk=%s) limit 1"
                if entity_type[0]=='player':
                    values=  (1,image_sk)
                    self.cursor.execute(UP_MAP_QRY, values)             
            else:
                self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_images' and TABLE_SCHEMA='SPORTSDB_DEV'")
                count = str(self.cursor.fetchone()[0])
            values = (image_sk, image, image_type, height, width, description, image_created, image_updated)
            self.cursor.execute(IMAGE_QUERY, values)
        
            right_values = (count, '2')
            self.cursor.execute(SP_IMG_RIGHT_ID, right_values)

            if player_id:
                is_primary = 1
                values = (player_id, 'player', count,is_primary)
                self.cursor.execute(IMG_MAP_QUERY, values)
            else:
                self.images_file.write('%s<>%s<>%s<>%s\n' % (player_name, image_sk, sport, player_sk))



    def get_player_details(self, name, player_sk, node, version, sport, api_key):
        pl_id = ''
        key_game = sport
        if "rugby" in key_game:
            key_game = "11"
        if "cricket" in key_game:
            key_game = "6"
        if "f1" in key_game:
            key_game = "10" 
        query = 'select id from sports_participants where sport_id=%s and title=%s'
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

