from scrapy.selector import Selector
from vtvspider import VTVSpider, extract_data, \
get_nodes, extract_list_data, get_utc_time, get_tzinfo
from scrapy.http import FormRequest, Request
import re
import requests
import MySQLdb
import json
import random

IMAGE_QUERY = 'insert into sports_images (url_sk, image_url, image_type, height, width, description, image_created, image_updated, league, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), image_type= %s, image_url=%s'

IMG_MAP_QUERY = 'insert into sports_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, 0, now(), now()) on duplicate key update modified_at=now()'

RIGHT_QUERY = 'insert into sports_image_rights_mapping(image_id, right_id, created_at, modified_at) values(%s, 1, now(), now()) on duplicate key update modified_at=now(), right_id = 1'

class GettyNewcrawler(VTVSpider):
    name = 'getty_new_playersimages'
    start_urls = []
    handle_httpstatus_list = [401]

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        #self.conn = MySQLdb.connect(host="10.28.216.45", user="veveo", passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.headers = {'api-Key': 'ha2c5vhscszmuueu8tn67edt'}

    def start_requests(self):
        bearer_key = self.get_bearer_key()
        self.headers.update({'authorization': 'Bearer %s' % bearer_key})
        PLAYERS_QUERY = "select getty_player_id, league from GETTY_IMAGES.getty_players where is_crawled=0 and league = 'Liga MX'"
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
        EVENTS_QUERY = "update GETTY_IMAGES.getty_players set is_crawled=1 where getty_player_id=%s limit 1"
        self.cursor.execute(EVENTS_QUERY % response.meta['player_id'])
        ids = ''
        for each_image in images:
            id_ = each_image['id']
            title = each_image['title']
            desc = each_image['caption']
            ids =  ids+id_+','

        #print ids
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
        img_type1 = []
        img_type2 = []
        for each_image in images:
            title =  each_image['title']
            desc  =  each_image['caption']
            keywords = [each['text'] for each in each_image['keywords']]
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
            if 'Vertical' in keywords:
                image_type = 'headshots'
                img_type1.append((id_, pic_url, image_type, height, width, desc, created_data, created_data, league, image_type,str(epopch_time),str(height*width)))
            elif 'Horizontal' in keywords:
                image_type = 'actionshots'
                img_type2.append((id_, pic_url, image_type, height, width, desc, created_data, created_data, league, image_type,str(epopch_time),str(height*width)))
            if not image_type: continue
        if img_type1:
            vals1 = []
            for epopch1 in img_type1:
                vals1.append(epopch1[-2])
            new_date1 = max(vals1)
            rec_vals1 = []
            for pix1 in img_type1:
                if new_date1 == pix1[-2]:
                    rec_vals1.append(pix1[-1])
            high_pixel1 = max(rec_vals1)
            data2 = []
            for v1 in img_type1:
                if new_date1 in v1[-2] and high_pixel1 in v1[-1]:
                    data2.append(v1)
                try:
                    data1_ = random.choice(data2)
                except:
                    continue
        if img_type2:
            vals = []
            for epopch in img_type2:
                vals.append(epopch[-2])
            new_date = max(vals)
            rec_vals = []
            for pix in img_type2:
                if new_date == pix[-2]:
                    rec_vals.append(pix[-1])
            high_pixel = max(rec_vals)
            data1 = []
            for v2 in img_type2:
                if new_date in v2[-2] and high_pixel in v2[-1]:
                    data1.append(v2)
                try:
                    data_ = random.choice(data1)
                except:
                    continue

        final_list = img_type1+img_type2
        for val in final_list:
            id_, pic_url, image_type, height, width, desc, created_data, created_data, league, image_type = val[:-2]
            rd_image_values = (id_, pic_url, image_type, height, width, desc, created_data, created_data, league, image_type,pic_url)
            print rd_image_values
            try:
                self.cursor.execute(IMAGE_QUERY, rd_image_values)
                query = "select entity_id from GETTY_IMAGES.getty_players_mapping where getty_player_id=%s and entity_type='player'"
                values = (response.meta['player_id'])
                self.cursor.execute(query % values)
                data = self.cursor.fetchone()
                if data:
                    sel_qry = 'select id from sports_images where url_sk=%s'
                    values = (id_)
                    self.cursor.execute(sel_qry % values)
                    image_data = self.cursor.fetchone()
                    if image_data:
                        image_id = image_data[0]
                        game_id  = data[0]
                        values = (game_id, 'player', image_id)
                        insert_qrt_he = "insert into sports_images_status(user_id,image_id,status,flag,created_at,modified_at) values(%s,%s,%s,%s,now(),now()) on duplicate key update modified_at=now()"
                        values_ = (str(game_id), str(image_id),'1','0')
                        self.cursor.execute(insert_qrt_he, values_)
                        self.cursor.execute(IMG_MAP_QUERY, values)
                        values = (image_id)
                        self.cursor.execute(RIGHT_QUERY % values)
            except:
                continue
        upadte_qry = "update sports_images_status set flag='0' where id=%s and user_id=%s"
        da_h1,da_a1 = '',''
        new2 = time.strptime("1900-01-01","%Y-%m-%d")
        newdate_ad = time.strptime("1900-01-01","%Y-%m-%d")
        import datetime
        if img_type1:
            if data1_:
                sel_qry_h = "select id,image_id from sports_images_status where flag='1' and user_id=%s and image_id in(select id from sports_images where image_type='headshots')"
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
                sel_qry_he = 'select id,image_created from sports_images where url_sk=%s and image_type="headshots"'
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
                        upda_qry = "update sports_images_status set flag='1' where image_id=%s"
                        self.cursor.execute(upda_qry % image_id_he)
                        self.conn.commit()
        if img_type2:
            if data_:
                sel_qry_a = "select id,image_id from sports_images_status where flag='1' and user_id = %s and image_id in(select id from sports_images where image_type='actionshots')"
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
                sel_qry_ac = 'select id,image_created from sports_images where url_sk=%s and image_type="actionshots"'
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
                        upda_qry_ac = "update sports_images_status set flag='1' where image_id=%s and user_id=%s"
                        val_ac_user = (image_id_ac, str(game_id))
                        self.cursor.execute(upda_qry_ac, val_ac_user)
                        self.conn.commit()


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
