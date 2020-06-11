from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import MySQLdb
from scrapy.selector import Selector


INT_TEAMS = 'insert into sports_teams(id, title, sport_id, market, abbr, country, country_code, tournament_id, formed, stadium_id, reference_url, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id= %s, reference_url = %s'


INT_PL = 'insert into sports_players_bkp(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, country, country_code, main_role, roles, height, weight, birth_date,birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_ROSTER = 'insert into sports_roster(team_id, player_id, player_role, player_number, status, season, created_at, modified_at) values(%s, %s, %s, \
%s, %s, %s, now(), now())on duplicate key update modified_at = now()'

INSRT_QRY = 'insert into sports_tournaments(id, title, sport_id, alias, reference_url, season_start, season_end, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id= %s'

INT_GRP_QRY = 'insert into sports_tournaments_groups(id, group_name, sport_id, alias, tournament_id, group_type, season_start, season_end, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_ENT = 'insert into sports_entities(entity_id, entity_type, result_type, result_value, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

TEAM_URL =  'https://api.sportradar.us/soccer-p2/%s/teams/%s/profile.xml?api_key=%s'

class SoccerRadarMeta(VTVSpider):
    name       = "soccer_radar_meta"
    start_urls = ["https://api.sportradar.us/soccer-p2/eu/teams/hierarchy.xml?api_key=72vxpdus7zy22a93kcnhjqct", \
                "https://api.sportradar.us/soccer-p2/na/teams/hierarchy.xml?api_key=kpwbhzaxkpxhfgp3zdaqe2xw"]

    def __init__(self):
        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSRADARDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()    

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        sport_id = '7'
        if "eu/teams/" in response.url:
            so_type  = "eu"
            api      = "72vxpdus7zy22a93kcnhjqct"
        else:
            so_type  = "na"
            api      = "kpwbhzaxkpxhfgp3zdaqe2xw"
        

        tou_nodes = get_nodes(sel, '//tournament_group')
        for tou_node in tou_nodes:
            tou_grp_id          = extract_data(tou_node, './@id')
            tou_grp_name        = extract_data(tou_node, './@name')
            tou_grp_start       = extract_data(tou_node, './@season_start')
            tou_grp_end         = extract_data(tou_node, './@season_end')
            tou_season          = extract_data(tou_node, './@season')
            tou_reference_id    = extract_data(tou_node, './@reference_id')

            if tou_grp_id:
                values = (tou_grp_id, tou_grp_name, sport_id, '', response.url, tou_grp_start, tou_grp_end, sport_id)
                self.cursor.execute(INSRT_QRY, values)
                
                values = (tou_grp_id, 'tournament', 'reference_id', tou_reference_id)
                self.cursor.execute(INT_ENT, values)

                values = (tou_grp_id, 'tournament', 'season', tou_season)
                self.cursor.execute(INT_ENT, values)

             
           
            group_nodes = get_nodes(tou_node, './/tournament')
            for group_node in group_nodes:
                tournament_id        = extract_data(group_node, './@id')
                tournament_name      = extract_data(group_node, './@name')
                tournament_start     = extract_data(group_node, './@season_start')
                tournament_end       = extract_data(group_node, './@season_end')
                tournament_season    = extract_data(group_node, './@season')
                tournament_type      = extract_data(group_node, './@type')
                tournament_season_id = extract_data(group_node, './@season_id')
                type_group_id        = extract_data(group_node, './@type_group_id') 
                reference_id         = extract_data(group_node, './@reference_id')


                if type_group_id and "Group" in tournament_name:
                    values = (tournament_id, tournament_name, sport_id, '', tou_grp_id, "Group", tournament_start, tournament_end, sport_id)
                    self.cursor.execute(INT_GRP_QRY, values)
                    type_ = "group"

  
                else:
                    values = (tournament_id, tournament_name, sport_id, '', response.url, tournament_start, tournament_end, sport_id)
                    self.cursor.execute(INSRT_QRY, values)
                    type_ = "tournament"

                if type_group_id:
                    values = (tournament_id, type_, 'type_group_id', type_group_id)
                    self.cursor.execute(INT_ENT, values)


            
                values = (tournament_id, type_, 'reference_id', reference_id)
                self.cursor.execute(INT_ENT, values)

                values = (tournament_id, type_, 'season', tournament_season)
                self.cursor.execute(INT_ENT, values)

                values = (tournament_id, type_, 'season_id', tournament_season_id)
                self.cursor.execute(INT_ENT, values)

                values = (tournament_id, type_, 'type', tournament_type)
                self.cursor.execute(INT_ENT, values)
 


                team_nodes     = get_nodes(group_node, './/team')
                for node in team_nodes:
                    team_id             = extract_data(node, './@id')      
            
                    team_url            =  TEAM_URL % (so_type, team_id, api)
                    yield Request(team_url, self.parse_team, meta = {'tou_id': tou_grp_id, 'season': tou_season})

    def parse_team(self, response):
        sel = Selector(response)
        sel.remove_namespaces() 
        sport_id = '7'
        team_id            = extract_data(sel, '//team/@id')
        team_name          = extract_data(sel, '//team/@name')
        team_fullname      = extract_data(sel, '//team/@full_name')
        team_callsign      = extract_data(sel, '//team/@alias')
        team_country       = extract_data(sel, '//team/@country')
        team_country_code  = extract_data(sel, '//team/@country_code')
        team_reference_id  = extract_data(sel, '//team/@reference_id')
        team_type          = extract_data(sel, '//team/@type')

        values = (team_id, 'team', 'reference_id', team_reference_id)
        self.cursor.execute(INT_ENT, values)

        values = (team_id, 'team', 'type', team_type)
        self.cursor.execute(INT_ENT, values)


        manager_id          = extract_data(sel, '//manager/@id') 
        manager_firstname   = extract_data(sel, '//manager/@first_name')
        manager_lastname    = extract_data(sel, '//manager/@last_name')
        manager_countrycode = extract_data(sel, '//manager/@country_code')
        manager_country     = extract_data(sel, '//manager/@country')
        manager_birthdate   = extract_data(sel, '//manager/@birthdate')
        manager_referenceid = extract_data(sel, '//manager/@reference_id')
        manager_fullname    = manager_firstname + " " + manager_lastname

        values = (team_id, 'player', 'reference_id', manager_referenceid)
        self.cursor.execute(INT_ENT, values)



        status             =   ''
        pl_abbr_name       =  pl_pr_position    =   college = \
        high_school        =  experience         =   salary = \
        birth_place        =   ''
        updated            =   ''

        pl_values = (manager_id, manager_fullname, sport_id, manager_firstname, manager_lastname, \
                     pl_abbr_name, response.url, '', manager_country, manager_countrycode, \
                    'Manager', '', '', '', manager_birthdate, birth_place, \
                    college, high_school, experience, salary, updated, sport_id)
        self.cursor.execute(INT_PL, pl_values)

        roster_values = (team_id, manager_id, 'Manager', '', '', response.meta['season'])
        self.cursor.execute(INT_ROSTER, roster_values)



        if team_id:
            team_values = (team_id, team_fullname, sport_id, team_name, team_callsign, team_country, team_country_code,response.meta['tou_id'], '', '', response.url, sport_id, response.url)
            self.cursor.execute(INT_TEAMS, team_values)

        nodes           = get_nodes(sel, '//roster/player')
        for each in nodes:
            pl_id              = extract_data(each, './@id') 
            pl_first_name      = extract_data(each, './@first_name')
            pl_last_name       = extract_data(each, './@last_name')
            pl_name            = pl_first_name+' '+pl_last_name
            pl_country         = extract_data(each, './@country')
            pl_country_code    = extract_data(each, './@country_code')
            preferred_foot     = extract_data(each, './@preferred_foot')
            birth_date         = extract_data(each, './@birthdate')
            height_in          = extract_data(each, './@height_in')
            weight_lb          = extract_data(each, './@weight_lb')
            height_cm          = extract_data(each, './@height_cm')
            weight_kg          = extract_data(each, './@weight_kg') 
            reference_id       = extract_data(each, './@reference_id')
            pl_jrsy_no         = extract_data(each, './@jersey_number')
            pl_position        = extract_data(each, './@position')
            status             =   ''
            pl_abbr_name       =  pl_pr_position    =   college = \
            high_school        = experience         =   salary = \
            birth_place        =   ''
            updated            =   '' 
            pl_values = (pl_id, pl_name, sport_id, pl_first_name, pl_last_name, \
                         pl_abbr_name, response.url, '', pl_country, pl_country_code,pl_position, \
                        pl_pr_position, height_in, weight_lb, birth_date, birth_place, \
                        college, high_school, experience, salary, updated, sport_id)
            self.cursor.execute(INT_PL, pl_values)

            roster_values = (team_id, pl_id, pl_position, pl_jrsy_no, status, response.meta['season'])
            self.cursor.execute(INT_ROSTER, roster_values)



            values = (pl_id, 'player', 'reference_id', reference_id)
            self.cursor.execute(INT_ENT, values)

            values = (pl_id, 'player', 'preferred_foot', preferred_foot)
            self.cursor.execute(INT_ENT, values)


            values = (pl_id, 'player', 'height_cm', height_cm)
            self.cursor.execute(INT_ENT, values)

            values = (pl_id, 'player', 'weight_kg', weight_kg)
            self.cursor.execute(INT_ENT, values)

