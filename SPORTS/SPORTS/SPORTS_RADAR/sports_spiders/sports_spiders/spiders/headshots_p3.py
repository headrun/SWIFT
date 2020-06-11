from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import re
import datetime
import MySQLdb

IMAGE_QUERY = 'insert into sports_images (url_sk, image_url, image_type, league, height, width, description, image_created, image_updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'

IMG_MAP_QUERY = 'insert into sports_images_mapping(entity_id, entity_type, image_id, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

SK_CHECK = "select entity_id from sports_source_keys where source=%s and entity_type=%s and source_key=%s"

UP_QRY = 'update sports_images set league =%s,description=%s, image_created=%s, image_updated=%s where url_sk=%s limit 1'

SP_IMG_RIGHT_ID = 'insert into sports_image_rights_mapping(image_id, right_id, created_at, modified_at) values(%s, %s, now(), now()) on duplicate key update modified_at=now()'

pattern1 = re.compile('.us/(.*)-images')

pattern2 = re.compile('reuters/(.*)/action')


class SoccerAPIImages(VTVSpider):
    name = "api_headshots"
    start_urls = []

    today = str(datetime.datetime.now().date())
    url = 'https://api.sportradar.us/%s-images-p%s/%s/headshots/players/%s/manifest.xml?api_key=%s'
    image_url = 'https://api.sportradar.us/%s-images-p%s/%s%s?api_key=%s'
    apis_dict = {'rugby': {'api-key': 'v5k8n3syyg4kexmyh8dyf2s5', 'version': '3', 'sport': 'rugby', 'provider': 'reuters'},\
                  'cricket': {'api-key': '2aumnpe8yfa2fyk469g464pz', 'version': '3', 'sport': 'cricket', 'provider': 'reuters'},\
                  'f1': {'api-key': 'gyx2zqve6y4323pjsgf737wk', 'version': '3', 'sport': 'f1', 'provider': 'reuters'},
              'tennis': {'api-key': 'vz8qysyeqp6pm5eazcjmhkws', 'version': '3', 'sport': 'tennis', 'provider': 'reuters'},}


    sport_dict = {'cricket': 6, 'tennis': 5, 'rugby union': 11, 'f1': 10}

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="root", passwd = '', db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.images_file = open('headshots_image_details', 'w+')
        self.pl_exists = open('headshots_pl_exists_images', 'w+') 
        self.player_images = []

        self.f=open('Players.txt','w')

    def start_requests(self):
        for image_year in ['2017', '2016', '2015', '2014', '2013', '2012', '2011']:
            for key, values in self.apis_dict.iteritems():
                api_key = values['api-key']
                version = values['version']
                sport = values['sport']
                provider = values['provider']
                req_url = self.url % (sport, version, provider, image_year, api_key)
                yield Request(req_url.replace('https', 'http'), self.parse,  \
                meta = {'game': key, 'version': version, \
                'api-key': api_key, 'provider': provider, 'sport': sport,
                'proxy': 'http://internal-sports-api-proxy-prod-0-1192592570.us-east-1.elb.amazonaws.com:8080/'},
                headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})

    def parse(self, response):
        sel = Selector(response)
      
        sel.remove_namespaces()
        game = response.meta['game']
        version = response.meta['version']
        api_key = response.meta['api-key']
        provider = response.meta['provider']
        sport = response.meta['sport']
        api_url = response.url
        if sport == 'rugby':sport = 'rugby union'
        if sport == 'f1': sport = 'auto racing'
        sport = self.sport_dict[sport]
        nodes = get_nodes(sel, '//assetlist/asset')
        for node in nodes:
          title_real = extract_data(node,'./title/text()')
          image_url  = extract_data(node,'./links/link/@href')
          title = '%'+title_real.replace(',','')+'%'
          query = "select id from sports_participants where sport_id=%s and participant_type = 'player' and title like %s" 
          value = (sport,title)
          player_id = ''
          self.cursor.execute(query, value)
          Data = self.cursor.fetchall()
          if len(Data) == 1:
            player_id = Data[0][0]
          if not Data and len(Data) !=1:
            value = (sport,'%'.join(title.split()))
          if sport == 5:
                value = (sport,'%'+' '.join(title_real.replace(',','').split()[::-1])+'%')
                self.cursor.execute(query, value)
                Data = self.cursor.fetchall()

          if len(Data) == 1:
              player_id = Data[0][0]
          else:
                if sport == 'auto racing': title = '%'+title_real.split()[0]+'%'
                if sport == 'cricket' or sport =='rugby union': title = '%'+title_real.split()[-1]+'%'
                value = (sport,title)
                self.cursor.execute(query, value)
                player_y_ = self.cursor.fetchall()
                #import pdb;pdb.set_trace()
                if len(player_y_) == 1:
                       player_id = player_y_[0][0]
                    
            
          player_sk = extract_data(node, './@player_id')
          '''import pdb;pdb.set_trace()
            player_id = self.get_player_id(player_sk)
          try:
                player_id = Data[0]
          except:
            continue'''
          
          print node,player_id, sport, version, provider, api_key,sel.response
          #player_id = self.getting_id(node,title_real,image_url,sport)
          if player_id:
                  self.populate_images(node, player_id, game, version, provider, api_key,api_url)
          else:
            self.f.write(title_real+'-'+image_url+'\n')
          self.cursor.close()
          self.conn.close()

        '''for i in player_y_: 
                 #import pdb;pdb.set_trace()
                 ratio = fuzz.token_sort_ratio(player_y_,title_real)
                 for i in player_y_:
                    lis.append(i[1])
                    process.extractOne(title_real, lis)
                    import pdb;pdb.set_trace()'''


    def populate_images(self, node, player_id, sport, version, provider, api_key, api_url):
        player_name = extract_data(node, './/title/text()')
        images = get_nodes(node, './links/link')
        player_sk = extract_data(node, './@player_id')
        image_created = extract_data(node, './@created')
        image_updated = extract_data(node, './@updated')
        description  = extract_data(node, './/description//text()').replace('amp;', '').strip()
      
        leauge = api_url.split('/')[3].split('-')[0].strip()
        if 'soccer' in api_url:
            leauge = api_url.split('/')[5].strip()
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
                values = (leauge,description, image_created, image_updated, image_sk)
                self.cursor.execute(UP_QRY, values)
            else:
                self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_images' and TABLE_SCHEMA='SPORTSDB'")
                count = self.cursor.fetchone()
            count = str(count[0])

            values = (image_sk, image, image_type, leauge, height, width, description, image_created, image_updated)
            self.cursor.execute(IMAGE_QUERY, values)

            right_values = (count, '2')
            self.cursor.execute(SP_IMG_RIGHT_ID, right_values)

            if player_id:
                values = (player_id, 'player', count)
            #import pdb;pdb.set_trace()
                self.cursor.execute(IMG_MAP_QUERY, values)
            else:
                self.images_file.write('%s<>%s<>%s<>%s\n' % (player_name, image_sk, sport, player_sk))
            self.cursor.close()
            self.conn.close()


    def get_player_details(self, name, player_sk, node, version, sport, api_key):
        pl_id = ''
        key_game = sport
        query = 'select id from sports_participants where sport_id=%s and title=%s'
        self.cursor.execute(query, (key_game, name))
        data = self.cursor.fetchall()
        data = [str(dt[0]) for dt in data]
        if data:
            rec = '<>'.join(data)
            record = '%s<>%s<>%s' % (rec, name, player_sk)
            self.pl_exists.write('%s\n' % record)
        self.cursor.close()
        self.conn.close()

    def get_player_id(self, sk):
        self.cursor.execute(SK_CHECK , ('radar', 'participant', sk))
        data = self.cursor.fetchone()
        if data:
            data = str(data[0])
        self.cursor.close()
        self.conn.close()

        return data

