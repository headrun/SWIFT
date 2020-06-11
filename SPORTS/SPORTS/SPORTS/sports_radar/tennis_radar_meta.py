from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import MySQLdb
from scrapy.selector import Selector

TOU_QRY = 'insert into sports_tournaments(id, title, sport_id, alias, gender, reference_url, season_start, season_end, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

TOU_EVENT_QRY = 'insert into sports_tournaments_events(tournament_id, event_id, sequence_num, status, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
    
TOU_SEASON_QRY = 'insert into sports_seasons(id, title, tournament_id, season_start, season_end, year, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

INT_ENT_QRY = 'insert into sports_entities(entity_id, entity_type, result_type, result_value, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'


class TenniRadarMeta(VTVSpider):
    name       = "tennis_radars_meta"
    start_urls = ["http://api.sportradar.us/tennis-t2/en/tournaments.xml?api_key=yybj7e5ma9j5ebvrucn9bpye"]

    def __init__(self):
        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSRADARDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        data = get_nodes(sel, '//tournament')
        for rd_data in data:
            id_         = extract_data(rd_data, './@id')
            title       = extract_data(rd_data, './@name')
            type_       = extract_data(rd_data, './@type') 
            parent_id   = extract_data(rd_data, './@parent_id')
            gender      = extract_data(rd_data, './@gender'). \
            replace('women', 'female').replace('men', 'male'). \
            replace('mixed', '').strip()
            sport_id    = extract_data(rd_data, './sport/@id')
            sport_name  = extract_data(rd_data, './sport/@name')
            cat_id      = extract_data(rd_data, './category/@id')
            cat_name    = extract_data(rd_data, './category/@name')
            cur_season  = extract_data(rd_data, './current_season/@id')
            cur_name    = extract_data(rd_data, './current_season/@name')
            start_date  = extract_data(rd_data, './current_season/@start_date')
            end_date    = extract_data(rd_data, './current_season/@end_date')
            year        = extract_data(rd_data, './current_season/@year')
        
           
            if parent_id:
                    ref_url = "http://api.sportradar.us/tennis-t2/en/tournaments/%s/schedule.xml?api_key=yybj7e5ma9j5ebvrucn9bpye" %parent_id
                    yield Request(ref_url, callback=self.parse_next, meta = {'start_date': start_date, 'end_date': end_date}, dont_filter=True)
 
            if id_:
                tou_values = (id_, title, '5', '', gender, response.url, start_date, end_date)
                self.cursor.execute(TOU_QRY, tou_values) 
               
                if parent_id:
                    event_values = (parent_id, id_, '', '') 
                     
                    self.cursor.execute(TOU_EVENT_QRY, event_values)

                if cur_season:
                    season_values = (cur_season, cur_name, id_, start_date, end_date, year)                     
                    self.cursor.execute(TOU_SEASON_QRY, season_values)

                if cat_id:
                    cat_values = (id_, 'tournament', 'category_id', cat_id)
                    cat_nm_values = (id_, 'tournament', 'category_name', cat_name)
                    self.cursor.execute(INT_ENT_QRY, cat_values)
                    self.cursor.execute(INT_ENT_QRY, cat_nm_values)
                    
                if type_:
                    ty_values = (id_, 'tournament', 'type', type_)
                    self.cursor.execute(INT_ENT_QRY, ty_values)


    def parse_next(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        id_   = extract_data(sel, '//tournament/@id')
        title = extract_data(sel, '//tournament/@name')  
        
        if id_:
            tou_values = (id_, title, '5', '', '', response.url, response.meta['start_date'], response.meta['end_date'])
            self.cursor.execute(TOU_QRY, tou_values)

