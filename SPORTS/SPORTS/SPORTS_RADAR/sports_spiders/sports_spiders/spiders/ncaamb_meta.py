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

CHECK_LOC = 'select id from sports_locations where city=%s and state=%s and country=%s'


INT_GRP_QRY = 'insert into sports_tournaments_groups(id, group_name, sport_id, alias, tournament_id, group_type, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_ENT = 'insert into sports_entities(entity_id, entity_type, result_type, result_value, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

INT_LOC = 'insert into sports_locations(country, state, city, zipcode, time_zone, created_at, modified_at) values (%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

INT_STADIUM = 'insert into sports_stadiums(id, title, capacity, address, surface, type, location_id, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

TEAM_URL = 'https://api.sportradar.us/ncaamb-p3/teams/%s/profile.xml?api_key=%s'


class NcaamdMeta(VTVSpider):
    name       = "ncaamb_radar_meta"
    start_urls =['https://api.sportradar.us/ncaamb-p3/league/hierarchy.xml?api_key=znq3h3jd57kuf9qmjc9pr3mp']


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
        api = 'znq3h3jd57kuf9qmjc9pr3mp'
        sports_id = '2'
        tournament_id = 'ncaamb'
        season_year = ''

        league_nodes = get_nodes(sel,'//league')
        for l_node in league_nodes:
            league_name = extract_data(l_node,'./@name')
            league_id = extract_data(l_node,'./@id')
            league_alias =  extract_data(l_node,'./@alias')

            division_nodes = get_nodes(l_node,'./division')
            for div_node in division_nodes:
                division_id = extract_data(div_node,'./@id')
                division_name = extract_data(div_node,'./@name')
                division_alias = extract_data(div_node,'./@alias')
        
                div_grp_type = 'Division'
                values = (division_id, division_name, sports_id,division_alias, '', div_grp_type, sports_id)
                self.cursor.execute(INT_GRP_QRY, values)
                
                conference_nodes = get_nodes(div_node,'./conference')
                for conf_node in conference_nodes:
                    conf_id = extract_data(conf_node,'./@id')
                    conf_name = extract_data(conf_node,'./@name')
                    conf_alias = extract_data(conf_node,'./@alias')

                    con_grp_type = 'Conference'
                    values = (conf_id, conf_name, sports_id,conf_alias, '', con_grp_type, sports_id)
                    self.cursor.execute(INT_GRP_QRY, values) 
                    
                    team_nodes = get_nodes(conf_node, './team')
                    for node in team_nodes:
                        team_id      = extract_data(node, './@id')
                        team_name    = extract_data(node,'./@name')
                        team_market   = extract_data(node,'./@market')
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
        team_name          = extract_data(sel, '//team/@name')
        team_market        = extract_data(sel, '//team/@market')
        team_alias         = extract_data(sel,'//team/@alias')
        team_subdivision   = extract_data(sel,'//team/@subdivision')

        league_id          = extract_data(sel,'//hierarchy/league/@id')
        league_name        = extract_data(sel,'//hierarchy/league/@name')
        league_alias       = extract_data(sel,'//hierarchy/league/@alias')

        division_id       = extract_data(sel,'//hierarchy/division/@id')
        division_name     = extract_data(sel,'//hierarchy/division/@name')
        division_alias    = extract_data(sel,'//hierarchy/division/@alias')

        conference_id      = extract_data(sel,'//hierarchy/conference/@id')
        conference_name   = extract_data(sel,'//hierarchy/conference/@name')
        conference_alias   = extract_data(sel,'//hierarchy/conference/@alias') 

        venue_id = extract_data(sel,'//team/venue/@id')
        venue_name = extract_data(sel,'//team/venue/@name')
        venue_capacity = extract_data(sel,'//team/venue/@capacity')
        venue_address = extract_data(sel,'//team/venue/@address')
        venue_city = extract_data(sel,'//team/venue/@city')
        venue_country = extract_data(sel,'//team/venue/@country')
        venue_state = extract_data(sel,'//team/venue/@state')
        venue_zip  = extract_data(sel,'//team/venue/@zip')
 

        if team_id:
            values = (team_id,team_name,sport_id,team_market,team_alias,'','',tournament_id,'','',response.url,sport_id,response.url)
            self.cursor.execute(INT_TEAMS,values)
            
            loc_check_values = (venue_city, venue_state, venue_country)
            self.cursor.execute(CHECK_LOC, loc_check_values)
            data_ = self.cursor.fetchone()
            if data_:
                location_id = data_[0]
            else:
                loc_values = (venue_country, venue_state, venue_city, venue_zip,'')
                self.cursor.execute(INT_LOC, loc_values)
                loc_check_values = (venue_city, venue_state, venue_country)
                self.cursor.execute(CHECK_LOC, loc_check_values)
                data_ = self.cursor.fetchone()
                if data_:
                    location_id = data_[0]

            if venue_id:
                stadium_values = (venue_id, venue_name, venue_capacity, '', '', venue_address, location_id)
                self.cursor.execute(INT_STADIUM, stadium_values)
            

            coach_nodes = get_nodes(sel,'//coaches/coach')
            for c_node in coach_nodes:
                coach_id = extract_data(c_node,'./@id')
                coach_name = extract_data(c_node,'./@full_name')
                coach_first_name = extract_data(c_node,'./@first_name')
                coach_last_name = extract_data(c_node,'./@last_name')
                coach_position = extract_data(c_node,'./@postion')
                coach_exp = extract_data(c_node,'./@experience')

                 #INT_PL = 'insert into sports_players_bkp(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, country, country_code, main_role, roles, height, weight, birth_date,birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'


                pl_values = (coach_id, coach_name, sport_id, coach_first_name, coach_last_name, \
                             '', response.url, '', '', '',   coach_position, \
                             '', '','', '', '', \
                             '', '', coach_exp, '', '', sport_id)
                self.cursor.execute(INT_PL, pl_values)


                roster_values = (team_id, coach_id,coach_position, '','', response.meta['season'], response.meta['season'])
                self.cursor.execute(INT_ROSTER, roster_values) 




            player_nodes           = get_nodes(sel, '//players/player')
            for each in player_nodes:
                pl_id              = extract_data(each, './@id') 
                pl_first_name      = extract_data(each, './@first_name')
                pl_last_name       = extract_data(each, './@last_name')
                pl_name            = extract_data(each,'./@full_name')
                pl_abbr            = extract_data(each,'./@abbr_name')
                birth_date         = ''
                birth_place        = extract_data(each,'./@birth_place')
                country            = ''
                height             = extract_data(each, './@height')
                weight             = extract_data(each, './@weight')
                position           = extract_data(each,'./@position')
                primary_position   = extract_data(each,'./@primary_position')
                jersey_number      = extract_data(each,'./@jersey_number')
                status             = extract_data(each,'./@status')
                experience         = extract_data(each,'./@experience')
                updated            = extract_data(each,'./@updated')
                college = high_school  =salary    =   '' 
                #INT_PL = 'insert into sports_players_bkp(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, country, country_code, main_role, roles, height, weight, birth_date,birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'
            
                pl_values = (pl_id, pl_name, sport_id, pl_first_name, pl_last_name, \
                             pl_abbr, response.url, '', country, '',   position, \
                             '', height, weight, birth_date, birth_place, \
                            college, high_school, experience, salary, updated, sport_id)
                self.cursor.execute(INT_PL, pl_values)

                   
                roster_values = (team_id, pl_id, position, jersey_number, status, response.meta['season'], response.meta['season'])
                self.cursor.execute(INT_ROSTER, roster_values)


