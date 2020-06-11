from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import re
import datetime
import MySQLdb
import collections
import StringUtil
from StringUtil import cleanString, generateSubstrings
import md5
from datetime import datetime
from datetime import timedelta
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
from scrapy import signals
import json


IMAGE_QUERY = 'insert into sports_images (url_sk, image_url, image_type, league, height, width, description, image_created, image_updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'

IMG_MAP_QUERY = 'insert into sports_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), is_primary=%s'

SK_CHECK = "select entity_id from sports_source_keys where source=%s and entity_type=%s and source_key=%s"

UP_QRY = 'update sports_images set description=%s, image_created=%s, image_updated=%s where url_sk=%s limit 1'

UP_IMG_QRY = 'update sports_images_mapping set is_primary=%s where entity_id=%s and image_id=%s and entity_type=%s limit 1'

SP_IMG_RIGHT_ID = 'insert into sports_image_rights_mapping(image_id, right_id, created_at, modified_at) values(%s, %s, now(), now()) on duplicate key update modified_at=now()'

RE_TEAM_PATTERN1 = re.compile("- (.*?) v (.*?) -", re.I)
RE_TEAM_PATTERN2 = re.compile("- (.*?) vs (.*?) -", re.I)

class SoccerAPIActionImages(VTVSpider):
    name = "soccer_action_images_1"
    start_urls = []

    today = str(datetime.now().date())
    url = 'https://api.sportradar.us/%s-images-p%s/%s/%s/actionshots/events/%s/manifest.xml?api_key=%s'

    image_url = 'https://api.sportradar.us/soccer-images-p3/reuters%s?api_key=%s'

    apis_dict = {
                 'bundesliga': {'api-key': 'pr253kju82fm9964ypgk6zg2', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'},
                 #'epl': {'api-key': 'bnspjq4ngenjrs5dh4z373mn', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'},
                 'la-liga': {'api-key': '6ecu2yybawrzgpm49srmwmww', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'}, 
                 'serie-a': {'api-key': 'crsdhq4ckunjgeqpax5q487q', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'},
                 'ligue-1': {'api-key': 'tmxeze4tfmffdxrnp2sz28xh', 'version': '3', 'sport': 'soccer', 'provider': 'reuters'}
                }

    def create_cursor(self):
        self.conn = MySQLdb.connect(host="10.28.218.81",
                                     user="veveo",
                                     passwd="veveo123",
                                     db="SPORTSDB",
                                     charset='utf8',
                                     use_unicode=True)
            
        self.cursor = self.conn.cursor()


    def __init__(self):
        self.create_cursor()
        self.images_file        = open('soccer_image_details', 'w+')
        self.without_game_id    = open('without_game_id', 'w+')
        self.pl_exists          = open('soccer_pl_exists_images', 'w+') 
        timestamp               = datetime.now().strftime("%Y%m%dT%H%M%S")
        self.missing_cases      = open('la_liga_missing_cases_%s' %timestamp, "w+")
        self.player_images      = []
        self.word_frequency     = {}
        self.f                  = open("wordfrequency.txt",'w')
        self.fteam              = open("teamnames.txt",'a')
        self.participants_dict  = {}
        self.teams_dict         = {}
        self.participants_dict_whole = {}
        self.teams_dict_whole   ={}
        self.get_participants_info()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def get_participants_info(self):
        tournaments = {'epl' : 35, 'bundesliga' : 33, 'la-liga' : 29, 'ligue-1' : 32, 'serie-a' : 579}
        for key, value in tournaments.iteritems():
            query = 'select participant_id from sports_tournaments_participants where tournament_id = %s'
            self.cursor.execute(query, (value))
            records = self.cursor.fetchall()

            for record in records:
                query = "select player_id from sports_roster where team_id  =%s"
                self.cursor.execute(query, (record[0]))
                players = self.cursor.fetchall()

                query = 'select id, title, aka from sports_participants where id =%s'
                self.cursor.execute(query, (record[0]))
                teams_data = self.cursor.fetchall()

                if teams_data:
                    id, title, aka = teams_data[0]
                    self.teams_dict.setdefault(key, {})
                    cleaned_title = cleanString(title)
                    for r  in ['a f c', "afc", "f c", "fc", "as"]:
                        cleaned_title = cleaned_title.replace(r, "").strip()
                    self.teams_dict[key][cleaned_title.strip()] = id
                    self.teams_dict_whole[cleaned_title.strip()] = id       
 
                for player in players:
                    query = 'select id, title, aka from sports_participants where id =%s'
                    self.cursor.execute(query, (player[0]))
                    rec = self.cursor.fetchall()

                    if rec:
                        id, title, aka = rec[0]
                        self.participants_dict.setdefault(key, {})
                        self.participants_dict[key][title.lower()] = id
                        self.participants_dict_whole[title.lower()] = id
        
            query = "select id, title from sports_participants where participant_type='team'"
            self.cursor.execute(query)
            records = self.cursor.fetchall()

            for record in records:
                pid, title  = record
                title = cleanString(title)
                self.teams_dict_whole[title.strip()] = pid
            

    def start_requests(self):
        now = datetime.now()
        for i in range(0, 3):
            _date = (now - timedelta(days=i)).strftime('%Y/%m/%d')
            _game_date = (now - timedelta(days=i)).strftime('%Y-%m-%d')
            for key, values in self.apis_dict.iteritems():
                api_key     = values['api-key']
                version     = values['version']
                sport       = values['sport']
                provider    = values['provider']
                req_url     = self.url % (sport, version, provider, key, _date, api_key)
                yield Request(req_url, self.parse,  \
                                meta={'game': key, 'version': version, \
                                'api-key': api_key, 'provider': provider,
                                'sport': sport, 'gamedate': _game_date})

    def team_information(self, description):
        teams = RE_TEAM_PATTERN1.findall(description)
        if teams:
            participant_1, participant_2 = teams[0]
            return participant_1, participant_2
        elif RE_TEAM_PATTERN2.findall(description):
            participant_1, participant_2 = RE_TEAM_PATTERN2.findall(description)[0]
            return participant_1, participant_2
        else:
            return '', ''

    def clean_titles(self, title):
        for r  in ['a f c', "afc", "f c", "fc"]:
            title = title.lower().replace(r, "").strip()
        return title

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        game 	  = response.meta['sport']
        version   = response.meta['version']
        api_key   = response.meta['api-key']
        provider  = response.meta['provider']
        sport 	  = response.meta['sport']
        game_date = response.meta['gamedate']
        nodes     = get_nodes(sel, '//assetlist/asset')

        for node in nodes:
            title       = extract_data(node, './title/text()')
            description = extract_data(node, './/description//text()').replace('amp;', '').replace('\n', '').strip()
            teamnames   = node.xpath('.//refs/ref[@type="organization"]/@name').extract()
            image_link  = node.xpath('.//links/link')
            image_created = extract_data(node, './@created')
            image_updated = extract_data(node, './@updated')

            par1, par2  = self.team_information(description)
            par1        = self.clean_titles(par1)
            par2        = self.clean_titles(par2)
            desc        = cleanString(description) 
            substrings  = generateSubstrings(desc, 1, 2)

            req_hash = self.participants_dict.get(response.meta['game'], '')
            if not req_hash: continue

            #for teams
            teams_list =  []
            for i in [par1, par2]:
                i = i.lower()
                if self.teams_dict[response.meta['game']].has_key(i):
                    team_id = self.teams_dict[response.meta['game']][i]
                    teams_list.append(team_id)

            #for players
            players_list = []
            is_primary = True
            for string in substrings[1]:
                if req_hash.has_key(string):
                    player_id = req_hash[string]
                elif self.participants_dict_whole.has_key(string):
                    player_id = self.participants_dict_whole[string]
                elif self.teams_dict_whole.has_key(string):
                    team_id = self.teams_dict_whole[string]
                    teams_list.append(team_id)
                    continue
                else:
                    continue

                if is_primary:
                    pri = 1
                else:
                    pri  = 0
                is_primary = False
                players_list.append((player_id, pri))

            for img in image_link:
                height   = "".join(img.xpath('./@height').extract())
                width    = ''.join(img.xpath('./@width').extract())
                pl_image = "".join(img.xpath('./@href').extract())
                image    = self.image_url %  (pl_image, response.meta['api-key'])

                url_sk = md5.md5(image).hexdigest()
                values = (url_sk, image, "actionshots", response.meta['game'], height, width, description, image_created, image_updated)
                self.cursor.execute(IMAGE_QUERY, values)

                query = 'select id from sports_images where url_sk =%s'
                self.cursor.execute(query, (url_sk))
                image_id = self.cursor.fetchall()

                if image_id:
                    print image_id
                    print players_list
                    print teams_list
                    right_values = (image_id[0][0], '2')
                    self.cursor.execute(SP_IMG_RIGHT_ID, right_values)
                    for player in players_list:
                        player_id, pri = player
                        values = (player_id, "player", image_id[0][0], pri, pri)
                        self.cursor.execute(IMG_MAP_QUERY, values)
                
                    for team in teams_list:
                        values = (team, "team", image_id[0][0], 0, 0)
                        self.cursor.execute(IMG_MAP_QUERY, values)
        
                if not teams_list and not players_list:
                    message = 'Image : %s\nDescription : %s\nResponseUrl : %s\n' %(image, description, response.url)
                    self.missing_cases.write("%s" %message)
                    self.missing_cases.write("%s\n" %("*"*50))

    def spider_closed(self):
        spider_stats = self.crawler.stats.get_stats()
        start_time   = spider_stats.get('start_time')
        finish_time = spider_stats.get('finish_time')
        spider_stats['start_time'] = str(start_time)
        spider_stats['finish_time'] = str(finish_time)

        query = "insert into WEBSOURCEDB.crawler_summary(crawler, start_datetime, end_datetime, type, count, aux_info, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now())"
        values = (self.name, start_time, finish_time, '','', json.dumps(spider_stats))
        self.cursor.execute(query,values)
