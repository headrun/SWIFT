from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import MySQLdb
from scrapy.selector import Selector


INT_TEAMS = 'insert into sports_teams(id, title, sport_id, market, abbr, country, country_code, tournament_id, formed, stadium_id, reference_url, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id= %s, reference_url = %s'

TOU_SEASON_QRY = 'insert into sports_seasons(id, title, tournament_id, season_start, season_end, year, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

INT_PL = 'insert into sports_players(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, country, country_code, main_role, roles, height, weight, birth_date,birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_ROSTER = 'insert into sports_roster(team_id, player_id, player_role, player_number, status, season, created_at, modified_at) values(%s, %s, %s, \
%s, %s, %s, now(), now())on duplicate key update modified_at = now(), season = %s'

INSRT_QRY = 'insert into sports_tournaments(id, title, sport_id, alias, reference_url, season_start, season_end, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id= %s'

#INT_GRP_QRY = 'insert into sports_tournaments_groups(id, group_name, sport_id, alias, tournament_id, group_type, season_start, season_end, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_GRP_QRY = 'insert into sports_tournaments_groups(id, group_name, sport_id, alias, tournament_id, group_type, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_ENT = 'insert into sports_entities(entity_id, entity_type, result_type, result_value, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

#TEAM_URL = 'https://api.sportradar.us/rugby-p1/teams/%s/profile.xml?api_key=%s'
TEAM_URL = 'https://api.sportradar.us/ncaafb-rt1/teams/%s/roster.xml?api_key=%s'



class NcaafbMeta(VTVSpider):
    name       = "ncaafb_radar_meta"
    start_urls =['https://api.sportradar.us/ncaafb-rt1/teams/FBS/hierarchy.xml?api_key=2u6h2nxweuzvwtnrbwjej7aw','https://api.sportradar.us/ncaafb-rt1/teams/FCS/hierarchy.xml?api_key=2u6h2nxweuzvwtnrbwjej7aw','https://api.sportradar.us/ncaafb-rt1/teams/D2/hierarchy.xml?api_key=2u6h2nxweuzvwtnrbwjej7aw','https://api.sportradar.us/ncaafb-rt1/teams/D3/hierarchy.xml?api_key=2u6h2nxweuzvwtnrbwjej7aw','https://api.sportradar.us/ncaafb-rt1/teams/NAIA/hierarchy.xml?api_key=2u6h2nxweuzvwtnrbwjej7aw','https://api.sportradar.us/ncaafb-rt1/teams/USCAA/hierarchy.xml?api_key=2u6h2nxweuzvwtnrbwjej7aw']

    def __init__(self):
        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSRADARDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        self.cursor.close()
        self.conn.close()

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        api = '2u6h2nxweuzvwtnrbwjej7aw'
        tournament_id = 'ncaafb'
        season_year = ''
        conference_nodes = get_nodes(sel,'//conference')
        for conf_node in conference_nodes:
            conf_id = extract_data(conf_node,'./@id')
            conf_name = extract_data(conf_node,'./@name')
            sports_id='4'
            con_grp_type = 'Conference'

            values = (conf_id, conf_name, sports_id,'', 'ncaafb', con_grp_type, sports_id)
            self.cursor.execute(INT_GRP_QRY, values)
        
            team_nodes = get_nodes(conf_node, './team')
            for node in team_nodes:
                team_id      = extract_data(node, './@id')
                team_name    = extract_data(node,'./@name')
                team_market   = extract_data(node,'./@market')
                team_coverage = extract_data(node,'./@coverage')
                team_url     =  TEAM_URL % (team_id, api)
                yield Request(team_url.replace('https', 'http'), self.parse_team, meta = {'tou_id': tournament_id, 'season':season_year ,'sports_id':sports_id, 'proxy': 'http://internal-sports-api-proxy-prod-0-1192592570.us-east-1.elb.amazonaws.com:8080/'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"}) 

            subdivision_nodes = get_nodes(conf_node, '//subdivision')
            for subd_node in subdivision_nodes:
                subdivision_id          = extract_data(subd_node, './@id')
                subdivision_name        = extract_data(subd_node, './@name')
                #INSRT_QRY = 'insert into sports_tournaments(id, title, sport_id, alias, reference_url, season_start, season_end, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id= %s'
                if subdivision_id:
                    #values = (tournament_id,tournament_name,sports_id,'',response.url,season_start,season_end,sports_id)
                    #self.cursor.execute(INSRT_QRY,values)
                    division_name = conf_name + ' ' + subdivision_name
                    div_grp_type = 'Division'
                    values = (subdivision_id, division_name, sports_id,'', 'ncaafb', div_grp_type, sports_id)
                    self.cursor.execute(INT_GRP_QRY, values)


                    team_nodes = get_nodes(subd_node, './/team')
                    for node in team_nodes:
                        team_id      = extract_data(node, './@id')      
                        team_name    = extract_data(node,'./@name')
                        team_market   = extract_data(node,'./@market')
                        team_coverage = extract_data(node,'./@coverage')
                        team_url     =  TEAM_URL % (team_id, api)
                        yield Request(team_url.replace('https', 'http'), self.parse_team, meta = {'tou_id': tournament_id, 'season':season_year ,'sports_id':sports_id, 'proxy': 'http://internal-sports-api-proxy-prod-0-1192592570.us-east-1.elb.amazonaws.com:8080/'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})
            
    
    def parse_team(self, response):
        sel = Selector(response)
        sel.remove_namespaces() 
        #INT_TEAMS = 'insert into sports_teams(id, title, sport_id, market, abbr, country, country_code, tournament_id, formed, stadium_id, reference_url, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id= %s, reference_url = %s'

        #INT_ROSTER = 'insert into sports_roster(team_id, player_id, player_role, player_number, status, season, created_at, modified_at) values(%s, %s, %s, \
        #%s, %s, %s, now(), now())on duplicate key update modified_at = now()'
               
        sport_id = response.meta['sports_id']
        tournament_id  = response.meta['tou_id']
        team_id            = extract_data(sel, '//team/@id')
        #if 'NAV' in team_id:
            #import pdb;pdb.set_trace()
        team_name          = extract_data(sel, '//team/@name')
        team_market      = extract_data(sel, '//team/@market')

        if team_id:
            values = (team_id,team_name,sport_id,team_market,'','','',tournament_id,'','',response.url,sport_id,response.url)
            self.cursor.execute(INT_TEAMS,values)


            nodes           = get_nodes(sel, '//team/player')
            for each in nodes:
                pl_id              = extract_data(each, './@id') 
                pl_first_name      = extract_data(each, './@name_first')
                pl_last_name       = extract_data(each, './@name_last')
                pl_name            = extract_data(each,'./@name_full')
                pl_abbr            = extract_data(each,'./@name_abbr')
                
                #pl_country         = extract_data(each, './@country')
                #pl_country_code    = extract_data(each, './@country_code')
                birth_date         = extract_data(each, './@birthdate')
                birth_place        = extract_data(each,'./@birth_place')
                #country            = birth_place.split(',')[-1].strip()
                country            = ''
                height             = extract_data(each, './@height')
                weight             = extract_data(each, './@weight')
                position           = extract_data(each,'./@position')
                jersey_number      = extract_data(each,'./@jersey_number')
                status             = extract_data(each,'./@status')
                experience         = extract_data(each,'./@experience')
                college = high_school  = experience = updated        =salary    =   '' 
                #INT_PL = 'insert into sports_players_bkp(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, country, country_code, main_role, roles, height, weight, birth_date,birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

            
            
                pl_values = (pl_id, pl_name, sport_id, pl_first_name, pl_last_name, \
                             pl_abbr, response.url, '', country, '',   position, \
                             '', height, weight, birth_date, birth_place, \
                            college, high_school, experience, salary, updated, sport_id)
                self.cursor.execute(INT_PL, pl_values)

                   
                roster_values = (team_id, pl_id, position, jersey_number, status, response.meta['season'], response.meta['season'])
                self.cursor.execute(INT_ROSTER, roster_values)


