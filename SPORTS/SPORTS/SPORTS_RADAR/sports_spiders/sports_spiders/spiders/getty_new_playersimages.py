from scrapy.selector import Selector
from vtvspider import VTVSpider, extract_data, \
get_nodes, extract_list_data, get_utc_time, get_tzinfo
from scrapy.http import FormRequest, Request
import re
import requests
import MySQLdb
import json

IMAGE_QUERY = 'insert into sports_images (url_sk, image_url, image_type, height, width, description, image_created, image_updated, league, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), image_type= %s'

IMG_MAP_QUERY = 'insert into sports_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, 0, now(), now()) on duplicate key update modified_at=now()'

RIGHT_QUERY = 'insert into sports_image_rights_mapping(image_id, right_id, created_at, modified_at) values(%s, 2, now(), now())'

class GettyNewcrawler(VTVSpider):
    name = 'getty_new_playersimages'
    start_urls = []
    handle_httpstatus_list = [401]

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.headers = {'api-Key': 'ha2c5vhscszmuueu8tn67edt'}

    def start_requests(self):
        bearer_key = self.get_bearer_key()
        self.headers.update({'authorization': 'Bearer %s' % bearer_key})
        PLAYERS_QUERY = "select getty_player_id, league from GETTY_IMAGES.getty_players where is_crawled=0"
        self.cursor.execute(PLAYERS_QUERY)
        data =  self.cursor.fetchall()
        for each in data:
            player_id = str(each[0])
            league = each[1]
            url = 'https://api.gettyimages.com/v3/search/images?phrase=%s&keyword_ids=%s&page_size=100&page=%s' % (league, player_id, 1)
            yield Request(url, callback=self.parse_images, headers=self.headers, meta={'player_id':player_id, 'league':league})

    def parse_images(self, response):  
        json_data = json.loads(response.body)
        bearer_key = self.get_bearer_key()
        player_id = response.meta['player_id']
        league = response.meta['league']
        self.headers.update({'authorization': 'Bearer %s' % bearer_key})
        images = json_data.get('images', '')
        EVENTS_QUERY = "update GETTY_IMAGES.getty_players set is_crawled=1 where getty_player_id=%s limit 1"
        self.cursor.execute(EVENTS_QUERY % response.meta['player_id'])
        ids = ''
        for each_image in images:
            id_ = each_image['id']
            title = each_image['title']
            desc = each_image['caption']
            ids =  ids+id_+','

        print ids
        images = 'https://api.gettyimages.com/v3/images?ids='+ids.strip(',').replace(',','%2C')+'&fields=caption%2Ccountry%2Cdetail_set%2Cevent_ids%2Cid%2Ckeywords%2Ctitle'
        print images
        yield Request(images, callback=self.parse_images_all, headers=self.headers, meta={'league':league, 'player_id':player_id})

        if images:
            cur_page = str(re.findall('page=(\d+)', response.url)[0])
            next_page = str(int(cur_page) + 1)
            url = response.url.replace('page='+cur_page, 'page='+next_page)
            yield Request(url, callback=self.parse_images, headers=self.headers, meta={'player_id':player_id, 'league':league})

    def parse_images_all(self, response):
        json_data = json.loads(response.body)
        player_id = response.meta['player_id']
        league = response.meta['league']
       
        if response.status == 401 :
            bearer_key = self.get_bearer_key()
            self.headers.update({'authorization': 'Bearer %s' % bearer_key})
            if not response.meta.get('counter', ''):
                yield Request(images, callback=self.parse_images_all, headers=self.headers, meta={'league':league, 'player_id':player_id, 'counter':1})

        images = json_data.get('images','')
        for each_image in images:
            title =  each_image['title']
            desc  =  each_image['caption']
            keywords = [each['text'] for each in each_image['keywords']]
            id_ = each_image['id']
            events_related = each_image['event_ids']
            pic_url = self.get_download_url(id_)
            image_type =  ''
            if 'Headshot' in keywords:
                image_type = 'headshots'
            elif 'Motion' in keywords:
                image_type = 'actionshots'
            league = response.meta['league']
            pic_url = each_image['referral_destinations'][0]['uri']
            pic_url = self.get_download_url(id_)
            height  =   each_image['max_dimensions'].get('height', '')
            width   =   each_image['max_dimensions'].get('width', '')
            created_data = each_image.get('date_created', '0000-00-00')
            rd_image_values = (id_, pic_url, image_type, height, width, desc, created_data, created_data, league, image_type)
            print rd_image_values
            try:
                self.cursor.execute(IMAGE_QUERY, rd_image_values)
                query = "select entity_id from GETTY_IMAGES.getty_players_mapping where getty_player_id=%s and entity_type='player'"
                values = (response.meta['player_id'])
                self.cursor.execute(query, values)
                data = self.cursor.fetchone()
                if data:
                    sel_qry = 'select id from sports_images where url_sk=%s'
                    values = (id_)
                    self.cursor.execute(sel_qry, values)
                    image_data = self.cursor.fetchone()
                    if image_data:
                        image_id = image_data[0]
                        game_id  = data[0]
                        values = (game_id, 'player', image_id)
                        print values
                        self.cursor.execute(IMG_MAP_QUERY, values)
                        values = (image_id)
                        self.cursor.execute(RIGHT_QUERY, values)
            except:
                continue
 
    def get_bearer_key(self):
        url = 'https://api.gettyimages.com/oauth2/token'
        headers = {'origin': 'https://api.gettyimages.com',
                    'authority': 'api.gettyimages.com'}

        data = [('grant_type', 'client_credentials'),
                ('client_id', 'ha2c5vhscszmuueu8tn67edt'),
                ('client_secret', 'qTSQZpbe7sv8fg3NyfeXFFsMNnG4amKhzRmnq5jAFKTeQ')]

        r= requests.post(url, headers=headers, data=data)
        data = r.json()
        return data.get('access_token', '')

    def get_download_url(self, image_id):
        access_token =  self.get_bearer_key()
        self.headers.update({'authorization': 'Bearer %s'%access_token})
        print image_id
        params = [('auto_download', 'false')]
        image_url = 'https://api.gettyimages.com/v3/downloads/images/%s'%image_id
        req = requests.post(image_url, headers=self.headers, params=params)        
        download_url =  req.json().get('uri','')
        return download_url
