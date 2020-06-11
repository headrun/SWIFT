from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import re
import datetime
import MySQLdb
import unwantedword
import nltk

IMAGE_QUERY = 'insert into sports_radar_images (url_sk, image_url, image_type, league, height, width, description, image_created, image_updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'

IMG_MAP_QUERY = 'insert into sports_radar_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

SK_CHECK = "select entity_id from sports_source_keys where source=%s and entity_type=%s and source_key=%s"

UP_QRY = 'update sports_radar_images set league =%s, description=%s, image_created=%s, image_updated=%s where url_sk=%s limit 1'

pattern1 = re.compile('.us/(.*)-images')

pattern2 = re.compile('reuters/(.*)/action')

class SoccerAPIImages(VTVSpider):
    name = "soccer_all_images"
    start_urls = []

    today = str(datetime.datetime.now().date())
    url = 'https://api.sportradar.us/%s-images-p%s/%s/%s/actionshots/events/%s/manifest.xml?api_key=%s'
    url = 'https://api.sportradar.us/%s-images-p%s/%s/actionshots/events/%s/manifest.xml?api_key=%s'
    image_url = "https://api.sportradar.us/%s-images-p3/reuters/actionshots/events/%s/%s/%s?api_key=%s"
    apis_dict = {
		'tennis': {'api-key': 'qaadnucggkf9kzu8zuhmvw2q', 'version': '3', 'sport': 'tennis', 'provider': 'reuters'},
		'f1': {'api-key': 'x7qwcj8mkz76xv4hu624murn', 'version': '3', 'sport': 'f1', 'provider': 'reuters'},
                'cricket': {'api-key': 'jwezpgcbt3t7vhjzez8myhun', 'version': '3', 'sport': 'cricket', 'provider': 'reuters'},
		'rugby': {'api-key': 'xhfhhvnegaygrufqcx79zveb', 'version': '3', 'sport': 'rugby', 'provider': 'reuters'}
                }

    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user="root",passwd = '', db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.images_file = open('soccer_image_details', 'w+')
	self.without_game_id = open('without_game_id', 'w+')
        self.pl_exists = open('soccer_pl_exists_images', 'w+') 
        self.player_images = []
	self.f=open('Players_not_in_db.txt','w')

    def start_requests(self):
	day_list = [datetime.datetime.now().date()]
	now = datetime.datetime.now()
	for i in range(0, 30):
            _date = (now - datetime.timedelta(days=i)).strftime('%Y/%m/%d')
            _game_date = (now - datetime.timedelta(days=i)).strftime('%Y/%m/%d')
            for key, values in self.apis_dict.iteritems():
                api_key = values['api-key']
                version = values['version']
                sport = values['sport']
                provider = values['provider']
                req_url = self.url % (sport, version, provider, _date, api_key)
                yield Request(req_url, self.parse,  \
                meta={'game': key, 'version': version, \
                'api-key': api_key, 'provider': provider, 'sport': sport, 'gamedate': _game_date})

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        game = response.meta['sport']
	if game == 'rugby' : key = 'rugby union'
        if game == 'tennis': key = 'tennis'
        if game == 'f1' : key = 'auto racing'
	if game == 'cricket' : key = 'cricket' 
        version = response.meta['version']
        api_key = response.meta['api-key']
        provider = response.meta['provider']
        sport = response.meta['sport']
	game_date = response.meta['gamedate']
	api_url = response.url
        nodes = get_nodes(sel, '//assetlist/asset')
        for node in nodes:
	    title_real = extract_data(node, './title/text()')
	    title = extract_data(node, './description/text()')
	    countries = node.xpath('.//refs/ref[@type="organization"]/@name').extract()
	    image_link =node.xpath('.//links/link/@href').extract()
	    print title_real
	    print title

	    liste1 = self.get_plid(title,key)
	    liste2 = self.get_plid(title_real,key)
	    liste1.extend(liste2)
	    if liste1 == []:
		self.f.write(title.encode('utf-8') + image_link[0]+'\n')
	    for i in set(liste1):
		game_id = ''
		team_id = ''
      		is_primary = '0'
   		self.populate_images(node, game_id, game, version, api_key, i, team_id, is_primary,game_date,api_url)
		

	    print list(set(liste1))
	    print '*'*50       


    def get_plid(self,title,key):
	prob_title = []
	pl_list = []
        for i in title.split():
            if i not in unwantedword.unwanted_words:
                prob_title.append(i)
        if len(prob_title) == 1:
            query="select id from sports_participants where participant_type='player' and title = '%s'" %(prob_title[0])
            print query
            #self.cursor.execute(query)
        else:
            player_bigrams = list(nltk.bigrams(prob_title))
        for player in player_bigrams:
            pl_id = 0
            player = " ".join(player)
            player = '%'+player+'%'
            query ="select id,title from sports_participants where participant_type='player' and title like %s and game=%s"
	    values = (player.replace(',','').replace('\'',''), key)
            self.cursor.execute(query,values)
            result = self.cursor.fetchall()
	    if not result:
		#query = "select id,title from sports_participants where id = (select participant_id from sports_players where display_title like %s)"
                query ="select id,title from sports_participants where participant_type='player' and aka like %s and game=%s"
		values = (player.replace(',','').replace('\'',''),key)
		self.cursor.execute(query,values)
		result = self.cursor.fetchall()

            if len(result)==1:
                pl_id = result[0][0]
		pl_list.append(pl_id)
        return pl_list

    def parse_participants(self, title, game_date, game):
	participants = title
	if 'Football Soccer' in title: title = title.split('Football Soccer')[-1].strip('- ')
	if '-' in title: title = title.split('-')[0].strip()
	if '(' in title: title = title.split('(')[0].strip()
        if " v " in title:
            participants = title.split(' v ')
	

        game_list = []
        for par in participants:
            if "At. Madrid" in par: par = "Atletico Madrid"
	    if "B. Munich" in par: par = "FC Bayern Munich"
	    if "Borussia" in par: par = "Borussia Monchengladbach"
	    if "Liverpool" in par: par = "Liverpool F.C."
	    if 'Villarreal' in par: par = "Villarreal CF"
	    if 'Chelsea' in par: par = "Chelsea F.C."
	    if 'Man City' in par: par = "Manchester City F.C."
	    if 'Southampton' in par: par = "Southampton F.C."
	    if 'Tottenham Hotspur' in par: par = "Tottenham Hotspur F.C."
	    if 'Real Madrid' in par: par = "Real Madrid C.F."
	    if 'Manchester United' in par: par = "Manchester United F.C."
	    if 'Manchester City' in par: par = "Manchester City F.C."
	    if 'Shakhtar' in par: par = 'FC Shakhtar Donetsk'	
	    if 'FC Sevilla' in par: par = 'Sevilla FC'
	    if 'AS Roma' in par: par = 'A.S. Roma'

            pa_qury = 'select id from sports_participants where title=%s and game =%s'
            values = (par, game)
            self.cursor.execute(pa_qury, values)
            data = self.cursor.fetchone()
            if not data:
                pa_query = 'select id from sports_participants where title like %s and game=%s and participant_type="team"'
		par = '%'+ par + '%'
                values = (par, game)
                self.cursor.execute(pa_query, values)
                data = self.cursor.fetchone()

            if data:
                pa_id = data[0]
                game_query = 'select game_id from sports_games_participants where participant_id =%s and game_id in (select id from sports_games where game = %s and game_datetime like %s and status !="Hole")'
                game_date_ = '%' + game_date + '%'
                values = (pa_id, game, game_date_)
                self.cursor.execute(game_query, values)
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


    def populate_images(self, node, game_id, game, version, api_key, pl_id, team_id, is_primary,game_date,api_url):
        images = get_nodes(node, './links/link')
        image_created = extract_data(node, './@created')
        image_updated = extract_data(node, './@updated')
        description  = extract_data(node, './/description//text()'). \
        replace('amp;', '').replace('\n', '').strip()
	asset_id = extract_data(node,'./@id')
	leauge = api_url.split('/')[3].split('-')[0].strip()
        if 'soccer' in api_url:
            leauge = api_url.split('/')[5].strip()

        for image_node in images:
            pl_image = extract_data(image_node, './@href')
	    pl_image = pl_image.split('/')[-1]
            height = extract_data(image_node, './@height')
            width  = extract_data(image_node, './@width')
	    image = self.image_url %  (game,game_date,asset_id,pl_image,api_key)
            image_type = 'actionshots'
            image_sk = get_md5(image)
            query = 'select id from sports_radar_images where url_sk=%s'
            self.cursor.execute(query, image_sk)
            count = self.cursor.fetchone()
            if count:
                count = str(count[0])
                values = (leauge,description, image_created, image_updated, image_sk)
                self.cursor.execute(UP_QRY, values)
                if pl_id and count:
                    query = 'select id from sports_radar_images_mapping where image_id=%s and entity_type="player" and entity_id=%s'
                    pl_values = (count, pl_id)
                    self.cursor.execute(query, pl_values)
                    im_id = self.cursor.fetchone()
                    if im_id:
                        im_id = str(im_id[0])
                        im_values = (leauge ,is_primary, pl_id, count, 'player')
			import pdb;pdb.set_trace()
                        self.cursor.execute(UP_QRY, im_values)
            else:
                self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_radar_images' and TABLE_SCHEMA='SPORTSDB'")

                count = str(self.cursor.fetchone()[0])
            values = (image_sk, image, image_type,leauge, height, width, description, image_created, image_updated)
	    #import pdb;pdb.set_trace()
            self.cursor.execute(IMAGE_QUERY, values)

            if game_id:
                values = (game_id, 'game', count, is_primary)
                self.cursor.execute(IMG_MAP_QUERY, values)

