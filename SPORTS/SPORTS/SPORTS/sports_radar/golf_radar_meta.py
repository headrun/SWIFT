from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import MySQLdb
from scrapy.selector import Selector

INSRT_QRY = 'insert into sports_tournaments(id, title, sport_id, alias, reference_url, season_start, season_end, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

INT_LOC = 'insert into sports_locations(country, state, city, zipcode, time_zone, created_at, modified_at) values (%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

INT_STADIUM = 'insert into sports_stadiums(id, title, capacity, surface, type, address, location_id, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), surface = %s, type = %s'

CHECK_LOC = 'select id from sports_locations where city=%s and state=%s and country=%s'



class GolfRadarMeta(VTVSpider):
    name       = "golf_radar_meta"
    start_urls = ["https://api.sportradar.us/golf-p2/schedule/pga/2017/tournaments/schedule.json?api_key=36c62wf2dmztgvfe2nt3qr9v"]

    def __init__(self):
        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSRADARDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def parse(self, response):
        data = eval(response.body)
        tournament = data['tournaments']
        for tou in tournament:
            id_        = tou['id']
            name       = tou['name']
            start_date = tou['start_date']
            end_date   = tou['end_date']
           
            address = capacity = city = state =  country = zip_code= time_zone = surface = type_ =''
            stadium_id   = tou['venue']['id']
            stadium_name = tou['venue']['name']
            address      =  tou.get('venue', '').get('address')
            capacity     =  tou.get('venue', '').get('capacity')
            city         =  tou.get('venue', '').get('city')
            state        =  tou.get('venue', '').get('state')
            country      = tou.get('venue', '').get('country')
            zip_code     = tou.get('venue', '').get('zipcode')
            time_zone    = tou.get('venue', '').get('time_zone', '')
            surface      = tou.get('venue', '').get('surface', '')
            type_        = tou.get('venue', '').get('type', '')
                
            if id_:
                values = (id_, name, '8', '', response.url, start_date, end_date)
                self.cursor.execute(INSRT_QRY, values)


            loc_check_values = (city, state, country)
            self.cursor.execute(CHECK_LOC, loc_check_values)
            data_ = self.cursor.fetchone()
            if data_:
                location_id = data_[0]
            else:
                loc_values = (country, state, city, zip_code, time_zone)
                self.cursor.execute(INT_LOC, loc_values)
                loc_check_values = (city, state, country)
                self.cursor.execute(CHECK_LOC, loc_check_values)
                data_ = self.cursor.fetchone()
                if data_:
                    location_id = data_[0]

            if stadium_id:
                stadium_values = (stadium_id, stadium_name, '', surface, type_, '', location_id, surface, type_)
                self.cursor.execute(INT_STADIUM, stadium_values)
     
             
