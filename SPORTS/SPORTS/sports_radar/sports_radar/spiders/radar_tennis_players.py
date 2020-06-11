from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import MySQLdb
from scrapy.selector import Selector


INT_PL = 'insert into sports_players(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, country, country_code, main_role, roles, height, weight, birth_date, birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), birth_date=%s'

PL_LINK = 'http://api.sportradar.us/tennis-t2/en/players/%s/profile.xml?api_key=yybj7e5ma9j5ebvrucn9bpye'

class RadarTennisPlayers(VTVSpider):
    name = 'tennisplayers_metadata'
    start_urls = [ "https://api.sportradar.us/tennis-t2/en/double_teams/rankings.xml?api_key=yybj7e5ma9j5ebvrucn9bpye",
                    "https://api.sportradar.us/tennis-t2/en/players/race_rankings.xml?api_key=yybj7e5ma9j5ebvrucn9bpye",
                    "https://api.sportradar.us/tennis-t2/en/players/rankings.xml?api_key=yybj7e5ma9j5ebvrucn9bpye",
                    "https://api.sportradar.us/tennis-t2/en/double_teams/race_rankings.xml?api_key=yybj7e5ma9j5ebvrucn9bpye"]

    def __init__(self):
        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSRADARDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        pl_data = get_nodes(sel, '//ranking//player_ranking')
        for data in pl_data:
            pl_id = extract_data(data, './player/@id')
            pl_name      = extract_data(data, './player/@name')
            pl_abb       = extract_data(data, './player/@abbreviation')
            pl_nat       = extract_data(data, './player/@nationality')
            pl_coun_code = extract_data(data, './player/@country_code')
            pl_link = PL_LINK % (pl_id)
            
            pl_name = pl_name.split(',')[-1].strip() + " " + pl_name.split(',')[0].strip()
            pl_values = (pl_id, pl_name, '5', '', '', pl_abb, pl_link, '', pl_nat, pl_coun_code, '', '', '', '', '', '', '', '', '', '', '')
            #self.cursor.execute(INT_PL, pl_values)
            yield Request(pl_link.replace('https', 'http'), callback=self.parse_next, meta={'proxy':'http://internal-sports-api-proxy-prod-0-1192592570.us-east-1.elb.amazonaws.com:8080/'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"}, dont_filter=True)            

    def parse_next(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        pl_data = get_nodes(sel, '//player_profile//player')
        for data in pl_data:
            pl_id        = extract_data(data, './@id')
            pl_name      = extract_data(data, './@name')
            pl_abb       = extract_data(data, './@abbreviation')
            pl_nat       = extract_data(data, './@nationality')
            pl_coun_code = extract_data(data, './@country_code')
            date_of_birth = extract_data(data, './@date_of_birth')
            pro_year     = extract_data(data, './@pro_year')
            handedness   = extract_data(data, './@handedness')

            if pro_year:
                pro_year  = str(pro_year) + "-01-01"
            if handedness:
                handedness = handedness.title() + "-handed"
            pl_name = pl_name.split(',')[-1].strip() + " " + pl_name.split(',')[0].strip()
            pl_values = (pl_id, pl_name, '5', '', '', pl_abb, response.url, pro_year, pl_nat, pl_coun_code, handedness, '', '', '', date_of_birth, '', '', '', '', '', '', date_of_birth)
            self.cursor.execute(INT_PL, pl_values)

        
