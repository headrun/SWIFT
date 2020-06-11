from scrapy.selector import Selector
from vtvspider import VTVSpider, extract_data, \
get_nodes, extract_list_data, get_utc_time, get_tzinfo
from scrapy.http import FormRequest, Request
import re
import requests
import MySQLdb
import json
import random
from getty_utils import *
import sys
reload(sys)
sys.setdefaultencoding('utf8')

WEIGHT_MAPP = {'One Person': 5, 'Portrait': 4, 'Looking at Camera': 3, 'Headshot': 2, 'Waist Up': 2, 'Photo Shoot': 1, 'Studio Shot': 1}

class GettysNewcrawler(VTVSpider):
    name = 'getty_new_playersimage'
    start_urls = []
    handle_httpstatus_list = [401,400,502]

    def __init__(self):
        self.conn = MySQLdb.connect(host = HOST, user = USER, passwd = passwd, db = DB1, charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.headers = {'api-Key': 'ha2c5vhscszmuueu8tn67edt'}

    def start_requests(self):
        bearer_key = self.get_bearer_key()
        self.headers.update({'authorization': 'Bearer %s' % bearer_key})
        self.cursor.execute(PLAYERS_QUERY)
        data =  self.cursor.fetchall()
        for each in data:
            player_id = str(each[0])
            league = each[1]
            url = 'https://api.gettyimages.com/v3/search/images?phrase=%s&keyword_ids=%s&page_size=100&page=%s' % (league, player_id, 1)
            page_no = ''.join(re.findall('size=100(.*)',url))
            yield Request(url, callback=self.parse_images, headers=self.headers, meta={'player_id':player_id, 'league':league,'page_no':page_no})

    def parse_images(self, response):
        json_data = json.loads(response.body)
        bearer_key = self.get_bearer_key()
        player_id = response.meta['player_id']
        league = response.meta['league']
        self.headers.update({'authorization': 'Bearer %s' % bearer_key})
        images = json_data.get('images', '')
        self.cursor.execute(EVENTS_QUERY % response.meta['player_id'])
        ids = ''
        for each_image in images:
            id_ = each_image['id']
            title = each_image['title']
            desc = each_image['caption']
            ids =  ids+id_+','

        images = 'https://api.gettyimages.com/v3/images?ids='+ids.strip(',').replace(',','%2C')+'&fields=caption%2Ccountry%2Cdetail_set%2Cevent_ids%2Cid%2Ckeywords%2Ctitle'
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
        elif response.status == 400 : 
            bearer_key = self.get_bearer_key()
            self.headers.update({'authorization': 'Bearer %s' % bearer_key})
            if not response.meta.get('counter', ''):
                yield Request(images, callback=self.parse_images_all, headers=self.headers, meta={'league':league, 'player_id':player_id, 'counter':1})
        images = json_data.get('images','')
        for each_image in images:
            title =  each_image['title']
            desc  =  each_image['caption']
            keywords = [each['text'] for each in each_image['keywords']]
            
            keywords_details = str("<>".join(keywords)).encode('utf8').strip()
            weight_count =0
            for list_ in keywords:
                weight_count += WEIGHT_MAPP.get(str(list_), 0)
            id_ = each_image['id']
            events_related = each_image['event_ids']
            pic_url = self.get_download_url(id_)
            image_type =  ''
            league = response.meta['league']
            pic_url = each_image['referral_destinations'][0]['uri']
            pic_url = self.get_download_url(id_)
            height  =   each_image['max_dimensions'].get('height', '')
            width   =   each_image['max_dimensions'].get('width', '')
            date_ = each_image.get('date_created')
            resolution = height*width
            created_data = each_image.get('date_created', '0000-00-00')
            import time
            try:
                epopch_time = int(time.mktime(time.strptime(date_,'%Y-%m-%dT%H:%M:%S')))
            except:
                epopch_time = 0
            created_data = each_image.get('date_created', '0000-00-00')

            no_people = [each['text'] for each in each_image['keywords'] if each['type'] == "NumberOfPeople"]

            if 'Vertical' in keywords:
                image_type = 'headshots'
            if 'Horizontal' in keywords:
                image_type = 'actionshots'
            if no_people:
                if "One Person" not in no_people and image_type == "headshots":
                    image_type = 'actionshots'
            if "Motion" in keywords:
                image_type = 'actionshots'
            if "Headshot" in keywords and "Horizontal" in keywords:
            	image_type="actionshots"  
            if "Scoring a goal" in keywords:
                image_type = 'actionshots'
            if "Heading the ball" in keywords:
                image_type='actionshots'
            if not image_type: continue

            rd_image_values = (id_, pic_url, image_type, height, width, desc, created_data, created_data, league, keywords_details, weight_count, image_type, pic_url, keywords_details, weight_count)
            try:
                self.cursor.execute(IMAGE_QUERY, rd_image_values)
                values = (response.meta['player_id'])
                self.cursor.execute(query % values)
                data = self.cursor.fetchone()
                if data:
                    values = (id_)
                    self.cursor.execute(sel_qry % values)
                    image_data = self.cursor.fetchone()
                    if image_data:
                        image_id = image_data[0]
                        game_id  = data[0]
                        values = (game_id, 'player', image_id)
                        values_ = (str(game_id), str(image_id),'1','0')
                        self.cursor.execute(insert_qrt_he, values_)
                        self.cursor.execute(IMG_MAP_QUERY, values)
                        values = (image_id)
                        self.cursor.execute(RIGHT_QUERY % values)
            except:
                continue
        """da_h1,da_a1 = '',''
        new2 = time.strptime("1900-01-01","%Y-%m-%d")
        newdate_ad = time.strptime("1900-01-01","%Y-%m-%d")
        import datetime
        if img_type1:
            if data1_:
                he_created = (data1_[6])
                if sel_qry_h:
                    self.cursor.execute(sel_qry_h % str(game_id))
                    da_h = self.cursor.fetchone()
                    if da_h:
                        da_h1 = da_h[0]
                        sel_imag_qry_he = "select image_created from sports_images where id=%s"%(da_h[1])
                        self.cursor.execute(sel_imag_qry_he)
                        image_created_he = self.cursor.fetchone()
                        image_id_head = str(image_created_he[0])
                        image_id_hea = image_id_head.split(' ')[0]
                        new2 = time.strptime(image_id_hea, "%Y-%m-%d")
                values_he = (data1_[0])
                hea_cre = str(datetime.datetime.strptime(he_created, "%Y-%m-%dT%H:%M:%S").date())
                self.cursor.execute(sel_qry_he % values_he)
                image_da_he = self.cursor.fetchone()
                if image_da_he:
                    image_id_he = image_da_he[0]
                    new1 = time.strptime(hea_cre, "%Y-%m-%d")
                    if new1 >= new2:
                        if da_h1:
                            vals_be_he = (da_h1, str(game_id))
                            self.cursor.execute(upadte_qry % vals_be_he)
                            self.conn.commit()
                        self.cursor.execute(upda_qry % image_id_he)
                        self.conn.commit()
        if img_type2:
            if data_:
                ac_created = (data_[6])
                if sel_qry_a:
                    self.cursor.execute(sel_qry_a % str(game_id))
                    da_a = self.cursor.fetchone()
                    if da_a:
                        da_a1 = da_a[0]
                        sel_imag_qry = "select image_created from sports_images where id=%s"%(da_a[1])
                        self.cursor.execute(sel_imag_qry)
                        image_created_ac = self.cursor.fetchone()
                        image_id_ac1 = str(image_created_ac[0])
                        img_id_ac1 = image_id_ac1.split(' ')[0]
                        newdate_ad = time.strptime(img_id_ac1, "%Y-%m-%d")
                ac_cre = str(datetime.datetime.strptime(ac_created, "%Y-%m-%dT%H:%M:%S").date())
                values_ac = (data_[0])
                self.cursor.execute(sel_qry_ac % values_ac)
                image_da_ac = self.cursor.fetchone()
                if image_da_ac:
                    image_id_ac = image_da_ac[0]
                    newdate_a = time.strptime(ac_cre, "%Y-%m-%d")
                    if newdate_a >= newdate_ad:
                        if da_a1:
                            val_be_ac = (da_a1, str(game_id))
                            self.cursor.execute(upadte_qry % val_be_ac)
                            self.conn.commit()
                        val_ac_user = (image_id_ac, str(game_id))
                        self.cursor.execute(upda_qry_ac, val_ac_user)
                        self.conn.commit()"""


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
        params = [('auto_download', 'false')]
        image_url = 'https://api.gettyimages.com/v3/downloads/images/%s'%image_id
        req = requests.post(image_url, headers=self.headers, params=params)
        download_url =  req.json().get('uri','')
        return download_url
