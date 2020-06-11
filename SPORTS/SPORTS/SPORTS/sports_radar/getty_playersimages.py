import datetime
from vtvspider import VTVSpider, extract_data, extract_list_data
from vtvspider import get_nodes
from scrapy.selector import Selector
from scrapy.http import Request, FormRequest
import MySQLdb

IMG_MAP_QUERY = 'insert into sports_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, 0, now(), now()) on duplicate key update modified_at=now()'

IMAGE_QUERY = 'insert into sports_images (url_sk, image_url, image_type, height, width, description, source, image_created, image_updated, league, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s , now(), now()) on duplicate key update modified_at=now(), image_type= %s'

SP_IMG_RIGHT_ID = 'insert into sports_image_rights_mapping(image_id, right_id, created_at, modified_at) values(%s, %s, now(), now()) on duplicate key update modified_at=now()'

RD_IMAGE_QUERY = 'insert into sports_radar_images (url_sk, image_url, image_type, height, width, description, image_created, image_updated, league, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s , now(), now()) on duplicate key update modified_at=now(), image_type= %s'



class GettyPlayersImages(VTVSpider):
    name = "getty_playersimages"

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def start_requests(self):
        url = 'https://www.gettyimages.in/sign-in'
        _data_dict = {'new_session[username]': 'ksmetanka', 'new_session[password]': 'images123'}
        yield FormRequest(url, callback=self.parse_login, formdata=_data_dict)

    def parse_login(self, response):
        sel = Selector(response)
        user_name = extract_data(sel, '//span[@id="user-display-name-text"]/text()')
        PLAYERS_QUERY = "select getty_player_id, league from GETTY_IMAGES.getty_players where is_crawled=0"
        self.cursor.execute(PLAYERS_QUERY)
        data =  self.cursor.fetchall()
        for each in data:
            event = str(each[0])
            league = each[1]
            #url = 'http://www.gettyimages.in/photos/%s?editorialproducts=sport&family=editorial&events=%s&phrase=%s&sort=best&excludenudity=true#license'
            #url = 'http://www.gettyimages.in/photos/%s?editorialproducts=sport&excludenudity=false&family=editorial&numberofpeople=one&specificpeople=%s&page=1&phrase=%s&sort=best#license'
            url = 'http://www.gettyimages.in/photos/%s?editorialproducts=sport&excludenudity=false&family=editorial&numberofpeople=one&specificpeople=%s&phrase=%s&sort=best#license'
            values = (league.replace(' ','-'), event, league)
            url = url%values
            yield Request(url, callback=self.parse_next, meta = {'event_id': event, 'league': each[1]})

    def parse_next(self, response):
        sel = Selector(response)
        EVENTS_QUERY = "update GETTY_IMAGES.getty_players set is_crawled=1 where getty_player_id=%s limit 1"
        values = (response.meta['event_id'])
        league = response.meta['league']
        self.cursor.execute(EVENTS_QUERY, values)
        user_name = extract_data(sel, '//span[@id="user-display-name-text"]/text()')
        pic_nodes = get_nodes(sel, '//a[@class="search-result-asset-link"]')
        for node in pic_nodes:
            pic_lic_id = extract_data(node, './@data-asset-id')
            pic_url = extract_data(node, './img/@src')
            lic_url = 'http://www.gettyimages.in/license/' + pic_lic_id
            yield Request(lic_url, callback=self.parse_image, meta = {'league': league, 'event_id':response.meta['event_id'], 'pic_url':pic_url})
        cur_page = extract_data(sel, '//input[@name="page"]/@data-current-page')
        max_page = extract_data(sel, '//span[@class="last-page"]/text()')      
        if cur_page and max_page and int(cur_page)<=int(max_page):
            url = response.url+ '&page=%s'%(int(cur_page)+1)
            yield Request(url, self.parse_next, meta = {'event_id': response.meta['event_id'], 'league': response.meta['league']})

    def parse_image(self, response):
        sel = Selector(response)
        user_name = extract_data(sel, '//span[@id="user-display-name-text"]/text()')
        #pic_url = extract_data(sel, '//figure[@class="unzoomed"]/img/@src')
        pic_url = response.meta['pic_url']
        url_sk = response.url.split('/')[-1]
        desc = extract_data(sel, '//div[@class="details-wrapper"]//section[@class="description"]//text()')
        image_title = extract_data(sel, '//div[@class="details-wrapper"]//section[@class="title"]//text()')
        created_data = extract_list_data(sel, '//div[@class="date-created"]//text()')[-1].strip()
        game_note = image_title.replace(' v ', ' vs. ').replace('AS ', '').strip().split(' - ')[0].strip().replace('Saint Etienne', 'Saint-Etienne')
        url_sk = ''.join(pic_url.split('&')[1:3]).replace('m=','').strip()
        #url_sk = "".join(pic_url.split('m')[-2:])
        height = pic_url.split('=')[-4].split('x')[0]
        width = pic_url.split('=')[-4].split('x')[-1].replace('&w', '').strip()
        created_data = str(datetime.datetime.strptime(created_data, '%d %B, %Y')).split(' ')[0]
        keywords = extract_data(sel, '//meta[@itemprop="keywords"]/@content')
        if 'Headshot' in keywords.split(','):
            image_type = 'headshots'
        elif 'Motion' in keywords.split(',') or 'Running' in keywords.split(','):
            image_type = 'actionshots'
        else:
            image_type = ''
        league = response.meta['league'].lower().replace(' ', '-').replace('premier-league', 'epl')
        image_values = (url_sk, pic_url, image_type, height, width, desc, 'Getty', created_data, created_data, league, image_type)
        game_note = '%' + game_note + '%'
        game_datetime = '%'+created_data+ '%'
        game_values = (game_datetime, game_note)
        #self.cursor.execute(IMAGE_QUERY, image_values)
        query = "select entity_id from GETTY_IMAGES.getty_players_mapping where getty_player_id=%s and entity_type='player'"
        values = (response.meta['event_id'])
        self.cursor.execute(query, values)
        data = self.cursor.fetchone()

        rd_image_values = (url_sk, pic_url, image_type, height, width, desc, created_data, created_data, league, image_type)
        self.cursor.execute(RD_IMAGE_QUERY, rd_image_values)

        sel_qry = 'select id from sports_radar_images where url_sk=%s'
        values = (url_sk)
        self.cursor.execute(sel_qry, values)
        image_data = self.cursor.fetchone()
        if image_data:
            image_id = image_data[0]
            right_values = (image_id, '1')
            self.cursor.execute(SP_IMG_RIGHT_ID, right_values)
        
        if data:
            sel_qry = 'select id from sports_radar_images where url_sk=%s'
            values = (url_sk)
            self.cursor.execute(sel_qry, values)
            image_data = self.cursor.fetchone()
            if image_data:
                image_id = image_data[0]
                game_id  = data[0]
                values = (game_id, 'player', image_id)
                self.cursor.execute(IMG_MAP_QUERY, values)
