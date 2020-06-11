from scrapy.selector import Selector
from vtvspider import VTVSpider, extract_data, \
get_nodes, extract_list_data, get_utc_time, get_tzinfo
from scrapy.http import FormRequest, Request
import requests
import MySQLdb
import json

RD_IMAGE_QUERY = 'insert into sports_radar_images (url_sk, image_url, image_type, height, width, description, image_created, image_updated, league, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s , now(), now()) on duplicate key update modified_at=now(), image_type= %s'

class GettyNewcrawler(VTVSpider):
    name = 'getty_newimages'
    start_urls = []
    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def start_requests(self):
        #competitions = ['premier league']
        competitions = ['ligue 1', 'premier league', 'bundesliga', 'la liga', 'uefa europa league', 'serie a italy', 'serie a', 'uefa champions league']
        for each in competitions:
            headers = {'Api-Key': 'ha2c5vhscszmuueu8tn67edt'}
            for i in range(1, 101):
                url = 'https://api.gettyimages.com/v3/search/images?phrase=%s&page=%s' % (each, i)
                #url = 'https://api.gettyimages.com/v3/search/images?fields=caption%2Cdetail_set%2Cevent_ids%2Cid%2Ckeywords%2Cpeople%2Cpreview%2Csummary_set%2Cthumb%2Ctitle&page_size=10&phrase='+each.replace(' ','%20')
                bearer_key = self.get_bearer_key()
                headers['Authorization'] = "Bearer %s"%bearer_key
                yield Request(url, callback=self.parse_images, headers=headers, meta={'headers':headers, 'league':each})

    def parse_images(self, response):  
        json_data = json.loads(response.body)
        images = json_data['images']    
        ids = ''
        for each_image in images:
            id_     = each_image['id']
            title   = each_image['title']
            desc    = each_image['caption']
            ids =  ids+id_+','
            #print id_, title, desc
        print ids
        images = 'https://api.gettyimages.com/v3/images?ids='+ids.strip(',').replace(',','%2C')+'&fields=caption%2Ccountry%2Cdetail_set%2Cevent_ids%2Cid%2Ckeywords%2Ctitle'
        print images
        #print len(ids.split(','))
        yield Request(images, self.parse_images_all, headers=response.meta['headers'], meta={'league':response.meta['league']})

    def parse_images_all(self, response):
        #import pdb;pdb.set_trace()
        json_data = json.loads(response.body)
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
            #some = self.get_download_url(id_)
            print rd_image_values  
            self.cursor.execute(RD_IMAGE_QUERY, rd_image_values) 
            #print title, desc, keywords, id_, events_related, image_url
        
    def get_bearer_key(self):
        url = 'https://api.gettyimages.com/oauth2/token'
        headers = {
                        'Origin': 'https://api.gettyimages.com',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'en-GB,en-US;q=0.8,en;q=0.6',
                        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Accept': '*/*',
                        'Referer': 'https://api.gettyimages.com/swagger/ui/index',
                        'X-Requested-With': 'XMLHttpRequest',
                        'Connection': 'keep-alive',
                  }
        data = [
                        ('grant_type', 'client_credentials'),
                        ('client_id', 'ha2c5vhscszmuueu8tn67edt'),
                        ('client_secret', 'qTSQZpbe7sv8fg3NyfeXFFsMNnG4amKhzRmnq5jAFKTeQ'),
                        ]
        r= requests.post(url, headers=headers, data=data)
        data = r.json()
        return data.get('access_token', '')

    def get_download_url(self, image_id):
        access_token =  self.get_bearer_key()
        headers = {
                'Origin': 'https://api.gettyimages.com',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-GB,en-US;q=0.8,en;q=0.6',
                'Authorization': 'Bearer %s'%access_token,
                'Api-Key': 'ha2c5vhscszmuueu8tn67edt',
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Referer': 'https://api.gettyimages.com/swagger/ui/index',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36',
                'Connection': 'keep-alive',
            }
        print image_id
        if "914180732" in image_id:
            import pdb;pdb.set_trace()
        params = [('auto_download', 'false')]
        image_url = 'https://api.gettyimages.com/v3/downloads/images/%s'%image_id
        req = requests.post(image_url, headers=headers, params=params)        
        download_url =  req.json().get('uri','')
        return download_url
