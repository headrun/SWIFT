from scrapy.http import Request
from vtvspider import VTVSpider
import json
import MySQLdb


INSRT_QRY = 'insert into sports_tournaments(id, title, sport_id, alias, reference_url, created_at, modified_at) values (%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_GRP_QRY = 'insert into sports_tournaments_groups(id, group_name, sport_id, alias, tournament_id, group_type, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_TEAMS = 'insert into sports_teams(id, title, sport_id, market, abbr, tournament_id, formed, stadium_id, reference_url, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s, reference_url = %s'

INT_LOC = 'insert into sports_locations(country, state, city, zipcode, time_zone, created_at, modified_at) values (%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

INT_STADIUM = 'insert into sports_stadiums(id, title, capacity, surface, type, address, location_id, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), surface = %s, type = %s'

CHECK_LOC = 'select id from sports_locations where city=%s and state=%s and country=%s'

INT_PL = 'insert into sports_players(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, main_role, roles, height, weight, birth_date, birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), debut=%s, sport_id=%s' 

INT_ROSTER = 'insert into sports_roster(team_id, player_id, player_role, player_number, status, season, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, now(), now())on duplicate key update modified_at = now()'

INT_DRAFT = 'insert into sports_players_draft(team_id, player_id, year, round, pick, created_at, modified_at) values(%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

TEAM_LINK = 'https://api.sportradar.us/%s/teams/%s/profile.json?api_key=%s'

INT_ENT = 'insert into sports_entities(entity_id, entity_type, result_type, result_value, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

class SportsRadarLeagues(VTVSpider):
    name = 'radarleague_metadata'
    start_urls = [  'https://api.sportradar.us/mlb-p5/league/hierarchy.json?api_key=e7gv4ntqzhyrrs5fn4qje6vr',
                    'https://api.sportradar.us/nba-p3/league/hierarchy.json?api_key=4gjmrrsbpsuryecdsdazxyuj',
                    'https://api.sportradar.us/nhl-p3/league/hierarchy.json?api_key=fc4ndqfgxbeb7kzzzvkx6wtx']

    def __init__(self):
        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSRADARDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def parse(self, response):
        data  = json.loads(response.body)
        id_   = data['league']['id']
        name  = data['league']['name']
        alias = data['league']['alias']
        league_info = response.url.split('/')[3]
        api_key = response.url.split('/')[-1].replace('hierarchy.json?api_key=', '').strip()

        if "nba-p3" in response.url:
            sport_id = '2'
        if "nhl-p3" in response.url:
            sport_id = '3'
        if "nba-p3" in response.url or "nhl-p3" in response.url:
            conference = data['conferences']
            group_type = "Conference"
        if "mlb-p5" in response.url:
            conference = data['leagues']
            group_type = "League"
            sport_id = '1'

        if id_:
           values = (id_, name, sport_id, alias, response.url, sport_id)
           self.cursor.execute(INSRT_QRY, values)
 
        for conf in conference:
            con_id = conf['id']
            con_name = conf['name']
            con_alias = conf['alias']
            values = (con_id, con_name, sport_id, con_alias, id_, group_type, sport_id)
            self.cursor.execute(INT_GRP_QRY, values)

            division = conf['divisions']
            for div in division:
                div_id = div['id'] 
                div_name = div['name']
                div_alias = div['alias']
                group_type = "Division"
                if "nba-p3" in response.url or "nhl-p3" in response.url:
                    div_name = alias + " " + div_name
                if "mlb-p5" in response.url:
                    div_name = con_name + " " + div_name
                values = (div_id, div_name, sport_id, div_alias, id_, group_type, sport_id)
                self.cursor.execute(INT_GRP_QRY, values)

                teams = div['teams']
                for team in teams:
                    team_id = team['id']
                    team_url = TEAM_LINK % (league_info, team_id, api_key)
                    yield Request(team_url.replace('https', 'http'), callback=self.parse_teams, meta = {'tou_id': id_, 'sport_id': sport_id, 'proxy':'headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"}'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})

    def parse_teams(self, response):
        data    = json.loads(response.body)
        tou_id = response.meta['tou_id']
        team_id = data['id'] 
        name    = data['name']
        abbr    = data.get('abbr', '')
        sport_id = response.meta['sport_id']
        if not abbr:
            abbr = data.get('alias', '')

        market       = data['market']
        name = market + " " + name
        founded      = data.get('founded', '')
        stadium_id   = data['venue']['id']
        stadium_name = data['venue']['name']
        address      = data['venue']['address']
        capacity     = data['venue']['capacity']
        city         = data['venue']['city']
        state        = data['venue']['state']
        country      = data['venue']['country']
        zip_code     = data['venue']['zip']
        time_zone    = data.get('venue', '').get('time_zone', '')
        surface      = data.get('venue', '').get('surface', '')
        type_        = data.get('venue', '').get('type', '')

        if founded:
            founded = str(founded) + "-01-01 00:00:00"
        else:
            founded = "0000-00-00 00:00:00"
        
        #Teams Informantion

        if team_id:
            team_values = (team_id, name, sport_id, market, abbr, tou_id, founded, stadium_id, response.url, sport_id, response.url)
            self.cursor.execute(INT_TEAMS, team_values)

        #Location Information       

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
            stadium_values = (stadium_id, stadium_name, capacity, surface, type_, address, location_id, surface, type_)
            self.cursor.execute(INT_STADIUM, stadium_values)

        #Coach infomation

        if "mlb-p5" in response.url:
            coaches = data.get('staff', '')
        else:
            coaches = data.get('coaches', '')

        for coach in coaches:
            pl_id         = coach['id']
            pl_name       = coach['full_name']
            pl_first_name = coach['first_name']
            pl_last_name  = coach['last_name']
            pl_pr_pos     = coach['position']
            experience    = coach.get('experience', '')

            pl_abbr_name = pl_position =  pl_height = pl_weight = \
            birthdate = birth_place =  college = high_school =  \
            salary = updated = pl_jrsy_no = status = high_school = experience  = salary =''

            pl_values = (pl_id, pl_name, sport_id, pl_first_name, pl_last_name, \
                        pl_abbr_name, response.url, '', pl_pr_pos, pl_position, \
                        pl_height, pl_weight, birthdate, birth_place, college, \
                        high_school, experience, salary, updated, '', sport_id)
            self.cursor.execute(INT_PL, pl_values)

            roster_values = (team_id, pl_id, pl_pr_pos, pl_jrsy_no, status, '')
            self.cursor.execute(INT_ROSTER, roster_values)

        #Players Information

        players = data['players']
        for player in players:
            pl_id         = player['id']
            pl_name       = player['full_name']
            pl_first_name = player['first_name']
            pl_last_name  = player['last_name']
            pl_abbr_name  = player.get('abbr_name', '')
            if not pl_abbr_name:
                pl_abbr_name = player.get('preferred_name', '')

            pl_height     = player.get('height', '')
            pl_weight     = player.get('weight', '')
            pl_position   = player['position']
            pl_pr_pos     = player['primary_position']
            pl_jrsy_no    = player.get('jersey_number', '')
            college       = player.get('college', '')
            birth_place   = player.get('birth_place', '')
            birthdate     = player.get('birthdate', '')
            updated       = player.get('updated', '')
            handedness    = player.get('handedness', '')
            experience    = player.get('experience', '')
            high_school   = player.get('high_school', '')
            status        = player['status']
            salary        = player.get('salary', '')
            mlbam_id      = player.get('mlbam_id', '') 
            throw_hand    = player.get('throw_hand', '')
            bat_hand      = player.get('bat_hand', '')
            pro_debut     = player.get('pro_debut', '')

            if handedness:
                values = (pl_id, 'player', 'handedness', handedness)
                self.cursor.execute(INT_ENT, values)
            if mlbam_id:
                values = (pl_id, 'player', 'mlbam_id', mlbam_id)
                self.cursor.execute(INT_ENT, values)
                values = (pl_id, 'player', 'throw_hand', throw_hand)
                self.cursor.execute(INT_ENT, values)
                values = (pl_id, 'player', 'bat_hand', bat_hand)
                self.cursor.execute(INT_ENT, values)


            if not birth_place:
                try:
                    birth_place = player.get('birthcity', '') + ", " +  \
                player.get('birthstate', '') + ", " + player.get('birthcountry', '')
                except:
                    birth_place = player.get('birthstate', '') + ", " + player.get('birthcountry', '')

            pl_values = (pl_id, pl_name, sport_id, pl_first_name, pl_last_name, \
                         pl_abbr_name, response.url, pro_debut, pl_pr_pos, \
                        pl_position, pl_height, pl_weight, birthdate, birth_place, \
                        college, high_school, experience, salary, updated, pro_debut, sport_id)
            self.cursor.execute(INT_PL, pl_values)

            roster_values = (team_id, pl_id, pl_pr_pos, pl_jrsy_no, status, '')
            self.cursor.execute(INT_ROSTER, roster_values)

            #Darft information
            player_draft = player.get('draft', '')
            if player_draft:
                draft_team_id = player.get('draft', '').get('team_id', '')
                draft_year    = player.get('draft', '').get('year', '') 
                draft_round   = player.get('draft', '').get('round', '')
                draft_pick    = player.get('draft', '').get('pick', '')

                if draft_team_id:
                    draft_values = (draft_team_id, pl_id, draft_year, draft_round, draft_pick)
                    self.cursor.execute(INT_DRAFT, draft_values) 

            
