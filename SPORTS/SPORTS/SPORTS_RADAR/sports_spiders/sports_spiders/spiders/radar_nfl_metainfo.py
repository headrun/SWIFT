from scrapy.http import Request
from vtvspider import VTVSpider, log
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import json
import MySQLdb

TEAM_DICT = {'MIN': '33405046-04ee-4058-a950-d606f8c30852', 'MIA': '4809ecb0-abd3-451d-9c4a-92a90b83ca06', 'CAR': 'f14bf5cc-9a82-4a38-bc15-d39f75ed5314', 'ATL': 'e6aa13a4-0055-48a9-bc41-be28dc106929', 'DET': 'c5a59daa-53a7-4de0-851f-fb12be893e9e', 'CIN': 'ad4ae08f-d808-42d5-a1e6-e9bc4e34d123', 'NYJ': '5fee86ae-74ab-4bdd-8416-42a9dd9964f3', 'DEN': 'ce92bd47-93d5-4fe9-ada4-0fc681e6caa0', 'BAL': 'ebd87119-b331-4469-9ea6-d51fe3ce2f1c', 'NYG': '04aa1c9d-66da-489d-b16a-1dee3f2eec4d', 'OAK': '1c1cec48-6352-4556-b789-35304c1a6ae1', 'TEN': 'd26a1ca5-722d-4274-8f97-c92e49c96315', 'LA': '2eff2a03-54d4-46ba-890e-2bc3925548f3', 'DAL': 'e627eec7-bbae-4fa4-8e73-8e1d6bc5c060', 'NE': '97354895-8c77-4fd4-a860-32e62ea7382a', 'SEA': '3d08af9e-c767-4f88-a7dc-b920c6d2b4a8', 'CHI': '7b112545-38e6-483c-a55c-96cf6ee49cb8', 'PIT': 'cb2f9f1f-ac67-424e-9e72-1475cb0ed398', 'NFC': '892cf7f3-46ab-4a45-8824-8afbd84924a9', 'CLE': 'd5a2eb42-8065-4174-ab79-0a6fa820e35e', 'TB': '4254d319-1bc7-4f81-b4ab-b5e6f3402b69', 'HO': '82d2d380-3834-4938-835f-aec541e5ece7', 'GB': 'a20471b4-a8d9-40c7-95ad-90cc30e46932', 'WAS': '22052ff7-c065-42ee-bc8f-c4691c50e624', 'JAC': 'f7ddd7fa-0bae-4f90-bc8e-669e4d6cf2de', 'KC': '6680d28d-d4d2-49f6-aace-5292d3ec02c2', 'PHI': '386bdbf9-9eea-4869-bb9a-274b0bc66e80', 'BUF': '768c92aa-75ff-4a43-bcc0-f2798c2e1724', 'NO': '0d855753-ea21-4953-89f9-0e20aff9eb73', 'LAC': '1f6dcffb-9823-43cd-9ff4-e7a8466749b5', 'IND': '82cf9565-6eb9-4f01-bdbd-5aa0d472fcd9', 'ARI': 'de760528-1dc0-416a-a978-b510d20692ff', 'SF': 'f0e724b0-4cbf-495a-be47-013907608da9', 'SD': '9dbb9060-ba0f-4920-829e-16d4d9246b5d'}

INSRT_QRY = 'insert into sports_tournaments(id, title, sport_id, alias, reference_url, created_at, modified_at) values (%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_GRP_QRY = 'insert into sports_tournaments_groups(id, group_name, sport_id, alias, tournament_id, group_type, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_TEAMS = 'insert into sports_teams(id, title, sport_id, market, abbr, tournament_id, formed, stadium_id, reference_url, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s, reference_url = %s'

INT_LOC = 'insert into sports_locations(country, state, city, zipcode, time_zone, created_at, modified_at) values (%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

INT_STADIUM = 'insert into sports_stadiums(id, title, capacity, address, surface, type, location_id, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

CHECK_LOC = 'select id from sports_locations where city=%s and state=%s and country=%s'

INT_PL = 'insert into sports_players(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, main_role, roles, height, weight, birth_date, birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_ROSTER = 'insert into sports_roster(team_id, player_id, player_role, player_number, status, season, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, now(), now())on duplicate key update modified_at = now(), season = %s'

INT_DRAFT = 'insert into sports_players_draft(team_id, player_id, year, round, pick, created_at, modified_at) values(%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'



class NFLRadarMetainfo(VTVSpider):
    name = 'nflradar_api_meta_info'
    start_urls = ['https://api.sportradar.us/nfl-rt1/teams/hierarchy.json?api_key=f593kvk9569c8yztm8h576nu']

    team_api = 'http://api.sportradar.us/nfl-rt1/teams/%s/roster.json?api_key=f593kvk9569c8yztm8h576nu'
    
    def __init__(self):
        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSRADARDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        self.cursor.close()
        self.conn.close()

    @log
    def parse(self, response):
        data = json.loads(response.body)
        id_   = data['league']
        name  = data['league']
        alias = ''
        sport_id = '4'
        if id_:
           values = (id_, name, sport_id, alias, response.url, sport_id)
           self.cursor.execute(INSRT_QRY, values)
    
        conferences = data['conferences']
        for conference in conferences:
            con_id     = conference['id']
            con_name   = conference['name']
            con_alias  = ''
            
            group_type = "Conference"
            values     = (con_id, con_name, sport_id, con_alias, id_, group_type, sport_id)
            self.cursor.execute(INT_GRP_QRY, values)

            divisions = conference['divisions']
            for division in divisions:
                group_type = "Division"
                div_id     = division['id']
                div_name   = division['name']
                div_alias  = ''
                values = (div_id, div_name, sport_id, div_alias, id_, group_type, sport_id)
                self.cursor.execute(INT_GRP_QRY, values)

                teams = division['teams']
                for team in teams:
                    team_id      = team['id']
                    name         = team['name']
                    abbr         = team.get('alias', '')
                    market       = team['market']
                    name         = market + " " + name
                    founded      = team.get('founded', '')
                    stadium_id   = team['venue']['id']
                    stadium_name = team['venue']['name']
                    address      = team['venue']['address']
                    capacity     = team['venue']['capacity']
                    city         = team['venue']['city']
                    state        = team['venue']['state']
                    country      = team['venue']['country']
                    zip_code     = team['venue']['zip']
                    time_zone    = team.get('venue', '').get('time_zone', '')
                    surface      = team.get('venue', '').get('surface', '')
                    type_        = team.get('venue', '').get('type', '')

                    if founded:
                        founded = str(founded) + "-01-01 00:00:00"
                    else:
                        founded = "0000-00-00 00:00:00"
                    
                    if team_id:
                        #team_id = TEAM_DICT[team_id]
                        team_values = (team_id, name, sport_id, market, abbr, id_, founded, stadium_id, response.url, sport_id,  response.url)
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
                        stadium_values = (stadium_id, stadium_name, capacity, surface, type_, address, location_id)
                        self.cursor.execute(INT_STADIUM, stadium_values)

          
                    team_id = team['id'].lower()
                    req_url = self.team_api % team_id
                    yield Request(req_url, self.parse_next, meta = {'tou_id': id_})
 


    def parse_next(self, response):
        self.schema = {}
        data = json.loads(response.body)
        players = data['players']
        coaches = data.get('coaches', '')
        team_id = data['id']
        sport_id = '4'
        #team_id = TEAM_DICT[team_id]
        for coach in coaches:
            pl_id         = coach['id']
            pl_name       = coach['name_full']
            pl_first_name = coach['name_first']
            pl_last_name  = coach['name_last']
            pl_abbr_name  = coach['name_abbr']
            pl_pr_pos     = coach['position']
            experience    = coach.get('experience', '')
            salary        = coach.get('salary', '')
            status        = coach.get('status', '')


            pl_position =  pl_height = pl_weight = \
            birthdate = birth_place =  college = high_school =  \
            updated = pl_jrsy_no = ''

            pl_values = (pl_id, pl_name, sport_id, pl_first_name, pl_last_name, \
                        pl_abbr_name, response.url, '', pl_pr_pos, pl_position, \
                        pl_height, pl_weight, birthdate, birth_place, college, \
                        high_school, experience, salary, updated, sport_id)
            self.cursor.execute(INT_PL, pl_values)

            roster_values = (team_id, pl_id, pl_pr_pos, pl_jrsy_no, status, '', '')
            self.cursor.execute(INT_ROSTER, roster_values)

        
        for player in players:
            pl_id         = player.get('id', '')
            pl_name       = player.get('name_full', '')
            pl_abbr_name  = player.get('name_abbr', '')
            pl_last_name  = player.get('name_last', '')
            pl_first_name = player.get('name_first', '')
            pl_pr_pos     = player.get('position', '')
            birthdate    = player.get('birthdate', '')
            pl_weight     = player.get('weight', '')
            pl_height      = player.get('height', '')
            birth_place   = player.get('birth_place', '')
            pl_jrsy_no    = player.get('jersey_number', '')
            updated       = player.get('updated', '')
            experience    = player.get('experience', '')
            high_school   = player.get('high_school', '')
            status        = player['status']
            salary        = player.get('salary', '')
            college        = player.get('college', '')           
 
            draft_pick    = player.get('draft_pick', '')
            draft_team_id = player.get('draft_team', '')
            draft_round   = player.get('draft_round', '')
            draft_year    = player.get('draft_year', '')
            pl_position = ''

            #draft_team_id = TEAM_DICT[draft_team_id]
            if draft_team_id:
                draft_values = (draft_team_id, pl_id, draft_year, draft_round, draft_pick)
                self.cursor.execute(INT_DRAFT, draft_values)
            
            pl_values = (pl_id, pl_name, sport_id, pl_first_name, pl_last_name, \
                         pl_abbr_name, response.url, '', pl_pr_pos, \
                        pl_position, pl_height, pl_weight, birthdate, birth_place, \
                        college, high_school, experience, salary, updated, sport_id)
            self.cursor.execute(INT_PL, pl_values)

            roster_values = (team_id, pl_id, pl_pr_pos, pl_jrsy_no, status, '', '')
            self.cursor.execute(INT_ROSTER, roster_values)

