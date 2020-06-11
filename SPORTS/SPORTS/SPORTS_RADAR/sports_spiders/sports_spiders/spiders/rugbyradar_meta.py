from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import MySQLdb
from scrapy.selector import Selector


INT_TEAMS = 'insert into sports_teams(id, title, sport_id, market, abbr, country, country_code, tournament_id, formed, stadium_id, reference_url, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id= %s, reference_url = %s'

TOU_SEASON_QRY = 'insert into sports_seasons(id, title, tournament_id, season_start, season_end, year, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

INT_PL = 'insert into sports_players(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, country, country_code, main_role, roles, height, weight, birth_date,birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_ROSTER = 'insert into sports_roster(team_id, player_id, player_role, player_number, status, season, created_at, modified_at) values(%s, %s, %s, \
%s, %s, %s, now(), now())on duplicate key update modified_at = now(), season = %s'

INSRT_QRY = 'insert into sports_tournaments(id, title, sport_id, alias, reference_url, season_start, season_end, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id= %s'

INT_GRP_QRY = 'insert into sports_tournaments_groups(id, group_name, sport_id, alias, tournament_id, group_type, season_start, season_end, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_ENT = 'insert into sports_entities(entity_id, entity_type, result_type, result_value, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

TEAM_URL = 'https://api.sportradar.us/rugby-p1/teams/%s/profile.xml?api_key=%s'



class RugbyRadarMeta(VTVSpider):
    name       = "rugby_radar_meta"
    start_urls = ['https://api.sportradar.us/rugby-p1/teams/2016/hierarchy.xml?api_key=8zp2sjv9ra28tct2rzfqh73a']


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
        api = '8zp2sjv9ra28tct2rzfqh73a'

        league_nodes = get_nodes(sel,'//league')
        for lea_node in league_nodes:
            league_id = extract_data(lea_node,'./@id')
            league_name = extract_data(lea_node,'./@name')
        
            sports_id=''
            sport_id_dict = {'Rugby Union Sevens':'167','Rugby League':'12','Rugby Union':'11'}
            sports_id = sport_id_dict.get(league_name, '')
            
 
    
            tou_nodes = get_nodes(lea_node, './tournament')
            for tou_node in tou_nodes:
                tournament_id          = extract_data(tou_node, './@id')
                tournament_name        = extract_data(tou_node, './@name')
                season_start           = extract_data(tou_node,'./season/@start')
                season_end             = extract_data(tou_node,'./season/@end')
                #INSRT_QRY = 'insert into sports_tournaments(id, title, sport_id, alias, reference_url, season_start, season_end, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id= %s'
                if tournament_id:
                    values = (tournament_id,tournament_name,sports_id,'',response.url,season_start,season_end,sports_id)
                    self.cursor.execute(INSRT_QRY,values)

           
                season_nodes = get_nodes(tou_node, './/season')
                for seas_node in season_nodes:
                    season_id        = extract_data(seas_node, './@id')
                    season_name      = extract_data(seas_node, './@name')
                    season_year      = extract_data(seas_node, './@year')
                    season_start     = extract_data(seas_node, './@start')
                    season_end       = extract_data(seas_node, './@end')

                    #TOU_SEASON_QRY = 'insert into sports_seasons(id, title, tournament_id, season_start, season_end, year, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

                    if season_id:
                        values = (season_id,season_name, tournament_id, season_start,season_end,season_year)

                    team_nodes = get_nodes(seas_node, './/team')
                    for node in team_nodes:
                        team_id      = extract_data(node, './@id')      
                        #team_name    = extract_data(node,'./@name')
                        #team_alias   = extract_data(node,'./@alias')
                        team_url     =  TEAM_URL % (team_id, api)
                        yield Request(team_url, self.parse_team, meta = {'tou_id': tournament_id, 'season':season_year ,'sports_id':sports_id})


    def parse_team(self, response):
        sel = Selector(response)
        sel.remove_namespaces() 
        '''        
        INT_TEAMS = 'insert into sports_teams(id, title, sport_id, market, abbr, country, country_code, tournament_id, formed, stadium_id, reference_url, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id= %s, reference_url = %s'

        INT_ROSTER = 'insert into sports_roster(team_id, player_id, player_role, player_number, status, season, created_at, modified_at) values(%s, %s, %s, \
        %s, %s, %s, now(), now())on duplicate key update modified_at = now()'
               
        '''
        sport_id = response.meta['sports_id']
        tournament_id  = response.meta['tou_id']
        team_id            = extract_data(sel, '//team/@id')
        team_name          = extract_data(sel, '//team/@name')
        team_callsign      = extract_data(sel, '//team/@alias')

        if team_id:
            values = (team_id,team_name,sport_id,'','','',team_callsign,tournament_id,'','',response.url,sport_id,response.url)
            self.cursor.execute(INT_TEAMS,values)


        nodes           = get_nodes(sel, '//roster/player')
        for each in nodes:
            pl_id              = extract_data(each, './@id') 
            pl_first_name      = extract_data(each, './@first_name')
            pl_last_name       = extract_data(each, './@last_name')
            pl_name            = pl_first_name+' '+pl_last_name
            pl_country         = extract_data(each, './@country')
            pl_country_code    = extract_data(each, './@country_code')
            birth_date         = extract_data(each, './@birth_date')
            height_in          = extract_data(each, './@height_in')
            weight_lb          = extract_data(each, './@weight_lb')
            height_cm          = extract_data(each, './@height_cm')
            weight_kg          = extract_data(each, './@weight_kg') 
            status             =   ''
            pl_abbr_name       =  pl_pr_position =pl_position   =   college =  pl_jrsy_no = \
            high_school        = experience         =   salary = \
            birth_place        =   ''
            updated            =   '' 
            #INT_PL = 'insert into sports_players_bkp(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, country, country_code, main_role, roles, height, weight, birth_date,birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

        
        
            pl_values = (pl_id, pl_name, sport_id, pl_first_name, pl_last_name, \
                         '', response.url, '', pl_country, pl_country_code,pl_position, \
                        pl_pr_position, height_in, weight_lb, birth_date, birth_place, \
                        college, high_school, experience, salary, updated, sport_id)
            self.cursor.execute(INT_PL, pl_values)

               
            roster_values = (team_id, pl_id, pl_position, pl_jrsy_no, status, response.meta['season'], response.meta['season'])
            self.cursor.execute(INT_ROSTER, roster_values)


