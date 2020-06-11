# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, \
extract_list_data, get_md5
import json
import datetime
import MySQLdb
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector

IMAGE_QUERY = 'insert into sports_images (url_sk, image_url, image_type, league, height, width, description, image_created, image_updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

IMG_MAP_QUERY = 'insert into sports_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

IMG_TAG_MAP_QUERY = 'insert into sports_radar_tags(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

UP_QRY = 'update sports_images set description=%s, image_created=%s, image_updated=%s where url_sk=%s limit 1'
UP_IMG_QRY = 'update sports_images_mapping set is_primary=%s where entity_id=%s and image_id=%s and entity_type=%s limit 1'
SP_IMG_RIGHT_ID = 'insert into sports_image_rights_mapping(image_id, right_id, created_at, modified_at) values(%s, %s, now(), now()) on duplicate key update modified_at=now()'

class RadarV3Actionshots(VTVSpider):
    name = "radaraction_images"
    start_urls = []
    url = "https://api.sportradar.us/%s-images-p%s/usat/actionshots/events/%s/manifest.xml?api_key=%s"
    image_url = 'https://api.sportradar.us/%s-images-p%s/usat%s?api_key=%s'

    apis_dict = {'nfl'      : {'api-key': 'km3tpjcxhug3wffejmyefbgs', 'version': '3'},
                 'mlb'      : {'api-key': 'ew5dcutm74bwgpyz4pfxkr2n', 'version': '3'},
                 'nhl'      : {'api-key': 'rbddgmtzenhe2tad9xr4xvvg', 'version': '3'},
                 'nba'      : {'api-key': 'a2xe3vm4zh47z8nxd6pswegm', 'version': '3'}}

    game_dict = { 'nba': '2', 'nhl': '3', 'mlb': '1', \
                  'golf': '8', 'nfl': '4', 'ncaamb': '2', \
                  'mls': '7', 'nascar': '10', 'ncaafb': '4'}

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.without_game_id = open('without_game_id', 'w+')
        self.without_pl_id   = open('without_pl_id', 'w+')
        self.pl_exists       = open('samename_pl_names', 'w+')
        self.pl_no_player    = open('playees_not_present', 'w+')


    def start_requests(self):
        for key, values in self.apis_dict.iteritems():
            api_key = values['api-key']
            version = values['version']
            now = datetime.datetime.now()

            for i in range(0, 2):
                game_date = (now - datetime.timedelta(days=i)).strftime('%Y/%m/%d')
                req_url = self.url % (key, version, game_date, api_key)
                yield Request(req_url, self.parse, meta={'game': key, 'version': version, \
                                                         'api-key': api_key,
                                                         'game_date': game_date})

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        game      = response.meta['game']
        league    = response.meta['game']
        version   = response.meta['version']
        api_key   = response.meta['api-key']
        nodes     = get_nodes(sel, '//assetlist/asset')
        game_date = response.meta['game_date']
        for node in nodes:
            teams = extract_list_data(node, './/refs//ref[@type="organization"]//@name')
            persons = extract_list_data(node, './/refs//ref[@type="profile"]//@name')
            _id = extract_data(node, './@id')  
            title = extract_data(node, './/title//text()')
            created = extract_data(node, './@created')
            game_id = self.parse_participants(title, game_date, game, created, teams)
            if game_id:
                pl_id = ''
                team_id = ''
                is_primary = '0'
                self.populate_images(node, game_id, game, version, api_key, pl_id, team_id, is_primary, league)
            else:
                self.without_game_id.write('%s\t%s\n' %(title, response.url))
            if persons:
                self.get_player_details(persons, game, node, version, api_key, league)
            if teams:
                self.get_team_details(teams, game, node, version, api_key, league)
    
    def get_team_details(self, teams, game, node, version, api_key, league):
        pl_id = ''
        game_id = ''
        team_id = ''
        for team in teams:
            team = team
            is_primary = '0'
            query = 'select id from sports_participants where sport_id=%s and title=%s and participant_type="team"'
            game_ = self.game_dict[game]
            self.cursor.execute(query, (game_, team))
            data = self.cursor.fetchall()
            data = [str(dt[0]) for dt in data]
            if data and len(data) == 1:
                team_id = data[0]
                self.populate_images(node, game_id, game, version, api_key, pl_id, team_id, is_primary, league)
            else:
                print team

    def get_player_details(self, persons, game, node, version, api_key, league):
        pl_id = ''
        game_id = ''
        team_id = ''
        for person_ in persons:
            person = person_.split(',')[-1].strip() + " " + person_.split(',')[0].strip()
            if "Alex Ovechkin" in person:
                person = "Alexander Ovechkin"
                persons[0] = "Alexander Ovechkin"
            if "Otto Porter Jr" in  person:
                person = "Otto Porter"
                persons[0] = "Otto Porter"

            query = 'select id from sports_participants where sport_id=%s and title=%s'
            game_ = self.game_dict[game]
            self.cursor.execute(query, (game_, person))
            data = self.cursor.fetchall()
            data = [str(dt[0]) for dt in data]
            if  person_ in persons[0]:
                is_primary = '1'
            else:
                is_primary = '0'
            if data and len(data) == 1:
                pl_id = data[0]
                self.populate_images(node, game_id, game, version, api_key, pl_id, team_id, is_primary, league)
            elif data:
                rec = '<>'.join(data)
                record = '%s<>%s<>%s' % (rec, person, game_)
                self.pl_exists.write('%s\n' % record)
            else:
                self.pl_no_player.write('%s\t%s\n' % (person, game))


    def parse_participants(self, title, game_date, game, created, teams):
        if " at " in title:
            participants = title.split(' at ')
        else:
            participants = title.split(' vs ')

        game = self.game_dict[game]
        game_list = []
        for par in participants:
            par = par.split(':')[-1].strip()
            if "Brigham Young" in par:
                par = "BYU Cougars"

            if "season-" in par:
                par = par.split('-')[-1].strip()

            for team in teams:
                if par in team:
                    par = team
            par = par.replace('Los Angeles Galaxy', 'LA Galaxy'). \
                        replace('Santos Laguna', 'Club Santos Laguna')

            pa_qury = 'select id from sports_participants where title=%s and sport_id =%s'
            values = (par, game)
            self.cursor.execute(pa_qury, values)
            data = self.cursor.fetchone()


            if data:
                pa_id = data[0]
                game_query = 'select game_id from sports_games_participants where participant_id =%s and game_id in (select id from sports_games where sport_id = %s and game_datetime like %s and game_datetime not like %s and status !="Hole")'
                game_date_ = '%' + game_date + '%'
                game_date_next = game_date + " 0"
                game_date_next = '%' + game_date_next + '%'
                values = (pa_id, game, game_date_, game_date_next)
                self.cursor.execute(game_query, values)
                data = self.cursor.fetchone()
                if not data:
                    game_date = created.split('T')[0].strip()
                    game_querys = 'select game_id from sports_games_participants where participant_id =%s and game_id in (select id from sports_games where sport_id = %s and game_datetime like %s and game_datetime like %s and status !="Hole")'
                    game_date_ = '%' + game_date + '%'
                    game_date_next = game_date + " 0"
                    game_date_next = '%' + game_date_next + '%'
                    values = (pa_id, game, game_date_, game_date_next)
                    self.cursor.execute(game_querys, values)
                    data = self.cursor.fetchone()
                if data:
                    data = str(data[0])
                    game_list.append(data)
                else:
                    data = ''
        if len(game_list) == 2:
            if game_list[0] == game_list[1]:
                data = game_list[0]
            else:
                data = ''
        return data


    def populate_images(self, node, game_id, game, version, api_key, pl_id, team_id, is_primary, league):
        images = get_nodes(node, './links/link')
        image_created = extract_data(node, './@created')
        image_updated = extract_data(node, './@updated')
        description  = extract_data(node, './/description//text()'). \
        replace('amp;', '').replace('\n', '').strip()

        for image_node in images:
            pl_image = extract_data(image_node, './/@href')
            height = extract_data(image_node, './@height')
            width  = extract_data(image_node, './@width')
            image = self.image_url % (game, version, pl_image, api_key)
            image_type = 'actionshots'
            image_sk = get_md5(image)
            query = 'select id from sports_images where url_sk=%s'
            self.cursor.execute(query, image_sk)
            count = self.cursor.fetchone()
            if count:
                count = str(count[0])
                values = (description, image_created, image_updated, image_sk)
                self.cursor.execute(UP_QRY, values)
                if pl_id and count:
                    query = 'select id from sports_images_mapping where image_id=%s and entity_type="player" and entity_id=%s'
                    pl_values = (count, pl_id)
                    self.cursor.execute(query, pl_values)
                    im_id = self.cursor.fetchone()
                    if im_id:
                        im_id = str(im_id[0])
                        im_values = (is_primary, pl_id, count, 'player')
                        self.cursor.execute(UP_IMG_QRY, im_values)


            else:
                self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_images' and TABLE_SCHEMA='SPORTSDB'")

                count = str(self.cursor.fetchone()[0])
            values = (image_sk, image, image_type,league, height, width, description, image_created, image_updated)
            self.cursor.execute(IMAGE_QUERY, values)

            right_values = (count, '2')
            self.cursor.execute(SP_IMG_RIGHT_ID, right_values)


            if game_id:
                values = (game_id, 'game', count, is_primary)
                self.cursor.execute(IMG_MAP_QUERY, values)
            if pl_id:
                if is_primary == '1':
                    values = (pl_id, 'player', count, is_primary)
                    self.cursor.execute(IMG_MAP_QUERY, values)
                tag_values = (pl_id, 'player', count, is_primary)
                self.cursor.execute(IMG_TAG_MAP_QUERY, tag_values)

            if team_id:
                values = (team_id, 'team', count, is_primary)
                self.cursor.execute(IMG_MAP_QUERY, values)
                tag_values = (team_id, 'team', count, is_primary)
                self.cursor.execute(IMG_TAG_MAP_QUERY, tag_values)

