import os
import re
import json
import datetime
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
from redis_utils import get_redis_data

IMAGE_QUERY = 'insert into sports_radar_images (url_sk, image_url, image_type, height, width, description, image_created, image_updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'

IMG_MAP_QUERY = 'insert into sports_radar_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, %s,now(), now()) on duplicate key update modified_at=now()'

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
        self.conn           = MySQLdb.connect(host   = "10.28.218.81",
                                              user   = "veveo",
                                              passwd = "veveo123",
                                              db     = "SPORTSDB",
                                              charset = 'utf8',
                                              use_unicode = True)
        self.cursor         = self.conn.cursor()
        self.images_file    = open('soccer_image_details', 'w+')
        self.pl_exists      = open('soccer_pl_exists_images', 'w+') 
        timestamp           = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
        debug_directory     = os.path.join(os.getcwd(), "debug_cases")
        self.missing_cases  = open("%s/%s" %(debug_directory, 'soccerway_radar_headshots_%s' %timestamp), "w+")
        self.populate_player_id()
        self.player_images  = []
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def populate_player_id(self):
        self.playerid_vs_teamid = {}
        tournaments = {'epl' : 35, 'bundesliga' : 33, 'la-liga' : 29, 'ligue-1' : 32, 'serie-a' : 579}

        query = "select player_id, team_id from sports_roster where status='active' and season in ('2016-17', '2015-16')"
        self.cursor.execute(query)
        players = self.cursor.fetchall()
        for player in players:
            player_id, team_id = player
            self.playerid_vs_teamid.setdefault(str(player_id), [])
            self.playerid_vs_teamid[str(player_id)].append(str(team_id))

        print "*"*20
        print len(self.playerid_vs_teamid)

    def start_requests(self):
        for image_year in ['2016', '2015']:
            for key, values in self.apis_dict.iteritems():
                api_key     = values['api-key']
                version     = values['version']
                sport       = values['sport']
                provider    = values['provider']
                req_url     = self.url % (sport, version, provider, key, image_year, api_key)
                yield Request(req_url, self.parse,  \
                                meta={'game': key, 'version': version, \
                                      'api-key': api_key, 'provider': provider, 'sport': sport})

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        game        = response.meta['game']
        version     = response.meta['version']
        api_key     = response.meta['api-key']
        provider    = response.meta['provider']
        sport       = response.meta['sport']
        nodes       = get_nodes(sel, '//assetlist/asset')

        for node in nodes:
            title       = extract_data(node, './/title/text()')
            description = extract_data(node, './/description/text()')
            player_sk   = extract_data(node, './@player_id')
            player_id   = self.get_player_id(player_sk)
            if player_id:
                self.populate_images(node, player_id, sport, version, provider, api_key)
            else:
                name = extract_data(node, './/title/text()')
                self.get_player_details(name, player_sk, node, version, sport, api_key)

    def populate_images(self, node, player_id, sport, version, provider, api_key):
        player_name     = extract_data(node, './/title/text()')
        images          = get_nodes(node, './links/link')
        player_sk       = extract_data(node, './@player_id')
        image_created   = extract_data(node, './@created')
        image_updated   = extract_data(node, './@updated')
        description     = extract_data(node, './/description//text()').replace('amp;', '').strip()

        for image_node in images:
            pl_image    = extract_data(image_node, './/@href')
            height      = extract_data(image_node, './@height')
            width       = extract_data(image_node, './@width')
            image       = self.image_url % (sport, version, provider, pl_image, api_key)
            if '/headshots/' in image:
                image_type = 'headshots'

            image_sk    = get_md5(image)
            query       = 'select id from sports_radar_images where url_sk=%s'
            self.cursor.execute(query, image_sk)
            count = self.cursor.fetchone()
            if count:
                count = str(count[0])
                values = (description, image_created, image_updated, image_sk)
                self.cursor.execute(UP_QRY, values)
                entity_qry = 'select entity_type from sports_radar_images_mapping where id in(select id from sports_radar_images where url_sk=%s)'
                self.cursor.execute(entity_qry,image_sk)
                entity_type = self.cursor.fetchone()
                UP_MAP_QRY = "update sports_radar_images_mapping set is_primary =%s where image_id in(select id from sports_radar_images where url_sk=%s) limit 1"
                #import pdb;pdb.set_trace()
                if entity_type[0]=='team':
                    values = (0,image_sk)
                    self.cursor.execute(UP_MAP_QRY, values)
                elif entity_type[0]=='player':
                    values=  (1,image_sk)
                    self.cursor.execute(UP_MAP_QRY, values)

            else:
                self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_radar_images' and TABLE_SCHEMA='SPORTSDB'")
                count = str(self.cursor.fetchone()[0])
            values = (image_sk, image, image_type, height, width, description, image_created, image_updated)

            self.cursor.execute(IMAGE_QUERY, values)
            if player_id:
                is_primary =1
                values = (player_id, 'player', count,is_primary)
                self.cursor.execute(IMG_MAP_QUERY, values)
        
                team_id = self.get_player_team(player_id)
                if team_id:
                    is_primary = 0
                    values = (team_id, 'team', count,is_primary)
                    self.cursor.execute(IMG_MAP_QUERY, values)
                    #query = "update sports_radar_images_mapping set entity_id = %s where image_id = %s  and entity_type = 'team' limit 1"
                    # query % (team_id, count)
                    #self.cursor.execute(query, (team_id, count))
            else:
                self.images_file.write('%s<>%s<>%s<>%s\n' % (player_name, image_sk, game, player_sk))

    def get_player_team(self, player_id):
        team_id = self.playerid_vs_teamid.get(player_id, '')
        if len(team_id) > 1:
            return team_id[0]
        elif len(team_id) == 1:
            return team_id[0]
        else:
            self.missing_cases.write("%s\n" %player_id)

    def get_player_details(self, name, player_sk, node, version, sport, api_key):
        query = 'select id from sports_participants where game=%s and title=%s'
        self.cursor.execute(query, (sport, name))
        data = self.cursor.fetchall()
        data = [str(dt[0]) for dt in data]
        if data:
            rec = '<>'.join(data)
            record = '%s<>%s<>%s' % (rec, name, player_sk)
            self.pl_exists.write('%s\n' % record)

    def get_player_id(self, sk):
        query = '%s:%s:%s' % (sk, 'participant', "radar")
        entity_id = get_redis_data(query, strict=True)
        if entity_id:
            return entity_id[0]
        else:
            return ''
    
    def spider_closed(self, spider):
        print self.name
        spider_stats = self.crawler.stats.get_stats()
        start_time   = spider_stats.get('start_time')
        finish_time = spider_stats.get('finish_time')
        spider_stats['start_time'] = str(start_time)
        spider_stats['finish_time'] = str(finish_time)

        query = "insert into WEBSOURCEDB.crawler_summary(crawler, start_datetime, end_datetime, type, count, aux_info, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now())"
        values = (self.name, start_time, finish_time, '', '', json.dumps(spider_stats))
        self.cursor.execute(query,values)
