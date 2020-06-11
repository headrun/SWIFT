from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data, get_md5
import MySQLdb
from scrapy.selector import Selector
from sports_spiders.items import SportsSetupItem

TOU_QRY = 'insert into sports_tournaments(id, title, sport_id, alias, gender, reference_url, season_start, season_end, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

TOU_SEASON_QRY = 'insert into sports_seasons(id, title, tournament_id, season_start, season_end, year, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

INT_ENT_QRY = 'insert into sports_entities(entity_id, entity_type, result_type, result_value, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

INT_TEAMS = 'insert into sports_teams(id, title, sport_id, market, abbr, country, country_code, tournament_id, formed, stadium_id, reference_url, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id= %s, reference_url = %s'

INT_PL = 'insert into sports_players(id, title, sport_id, first_name, last_name, abbr_name, reference_url, debut, country, country_code, main_role, roles, height, weight, birth_date,birth_place, college, high_school, experience, salary, updated, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now(), sport_id = %s'

INT_ROSTER = 'insert into sports_roster(team_id, player_id, player_role, player_number, status, season, created_at, modified_at) values(%s, %s, %s, \
%s, %s, %s, now(), now())on duplicate key update modified_at = now(), season = %s'

TOU_LINK = 'https://api.sportradar.us/soccer-p3/%s/en/tournaments/%s/schedule.xml?api_key=%s'
TEAM_LINK = 'http://api.sportradar.us/soccer-p3/%s/na/teams/%s/profile.xml?api_key=%s'

class SoccerV3RadarMeta(VTVSpider):
    name       = "soccer_radar_v3"
    start_urls = ["https://api.sportradar.us/soccer-p3/eu/en/tournaments.xml?api_key=xzuefb5xj5294amryqu828gk",
                "https://api.sportradar.us/soccer-p3/am/en/tournaments.xml?api_key=eah2zymdzsdne76xm5uc9rvp"]

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
        tou_nodes = get_nodes(sel, '//tournaments//tournament')
        for tou_node in tou_nodes:
            tou_id          = extract_data(tou_node, './@id')
            tou_name        = extract_data(tou_node, './@name')
            tou_start       = extract_data(tou_node, './current_season/@start_date')
            tou_end         = extract_data(tou_node, './current_season/@end_date')
            tou_sea_id      = extract_data(tou_node, './current_season/@id')
            tou_sea_name    = extract_data(tou_node, './current_season/@name')
            tou_year        = extract_data(tou_node, './current_season/@year')
            cat_id          = extract_data(tou_node, './category/@id')
            cat_name        = extract_data(tou_node, './category/@name')
            if "xzuefb5xj5294amryqu828gk" in response.url:
                tou_link = TOU_LINK % ('eu', tou_id , 'xzuefb5xj5294amryqu828gk')
            else:
                tou_link = TOU_LINK % ('am', tou_id, 'eah2zymdzsdne76xm5uc9rvp')

            if tou_id:
                tou_values = (tou_id, tou_name, '7', '', '', response.url, tou_start, tou_end)
                self.cursor.execute(TOU_QRY, tou_values)


                if tou_sea_id:
                    season_values = (tou_sea_id, tou_sea_name, tou_id, tou_start, tou_end, tou_year)
                    self.cursor.execute(TOU_SEASON_QRY, season_values)

                if cat_id:
                    cat_values = (tou_id, 'tournament', 'category_id', cat_id)
                    cat_nm_values = (tou_id, 'tournament', 'category_name', cat_name)
                    self.cursor.execute(INT_ENT_QRY, cat_values)
                    self.cursor.execute(INT_ENT_QRY, cat_nm_values)

            yield Request(tou_link.replace('https', 'http'), callback=self.parse_next, meta={'tou_id': tou_id, 'season':tou_year, 'proxy': 'http://internal-sports-api-proxy-prod-0-1192592570.us-east-1.elb.amazonaws.com:8080/'}, headers = {"X-SPORTSAPI-EXT-PROXY-CLIENT": "Sports_KG"})

    def parse_next(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        record = SportsSetupItem()
        competitors_list = []
        game_nodes = get_nodes(sel, '//sport_events//sport_event')
        for game_node in game_nodes:
            game_id       = extract_data(game_node, './@id')
            game_datetime = extract_data(game_node, './@scheduled')   
            game_status   = extract_data(game_node, './@status')
            tou_id        = extract_data(game_node, './tournament/@id')
            competitors   = get_nodes(game_node, './competitors/team')
            for com in competitors:
                qualifier = extract_data(com, './@qualifier')
                id_       = extract_data(com, './@id')
                
                competitors_list.append(id_)
                if qualifier == "home":
                    home_id = id_
                if qualifier == "away":
                    away_id = id_

            stadium_id = ''
            record['game_datetime']     = game_datetime
            record['sport_id']          = '7'
            record['game_status']       = game_status
            record['participant_type']  = "team"
            record['tournament']        = tou_id
            record['game']              = 'soccer'
            record['reference_url']     = response.url
            record['result']            = {}
            record['source_key']        = game_id
            record['rich_data']         = {'location': {'satdium': stadium_id}}
            record['participants'] = { home_id: ('1',''), away_id: ('0','')}
            yield record

        for comp_id in competitors_list:
            if "xzuefb5xj5294amryqu828gk" in response.url:
                team_link = TEAM_LINK % ('eu', comp_id, 'xzuefb5xj5294amryqu828gk')
            else:
                team_link = TEAM_LINK % ('am', comp_id, 'eah2zymdzsdne76xm5uc9rvp')
            yield Request(team_link, callback=self.parse_team, meta={'tou_id':tou_id, 'season':response.meta['season']})

    def parse_team(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        node = get_nodes(sel, '//team_profile//team')[0]
        team_id = extract_data(node, './@id')
        team_name = extract_data(node, './@name')
        team_country = extract_data(node, './@country')
        team_country_code = extract_data(node, './@country_code')
        team_abbr = extract_data(node, './@abbreviation')
        sport_id = 7

        values = (team_id, team_name, sport_id, '', team_abbr, team_country, team_country_code, response.meta['tou_id'], '', '', response.url, sport_id, response.url)
        self.cursor.execute(INT_TEAMS, values)

        players_nodes = get_nodes(sel, '//players//player')
        for pl_node in players_nodes:
            pl_id = extract_data(pl_node, './@id')
            pl_role = extract_data(pl_node, './@type')
            pl_nation = extract_data(pl_node, './@nationality')
            pl_country_code = extract_data(pl_node, './@country_code')
            pl_dob = extract_data(pl_node, './@date_of_birth')
            pl_height = extract_data(pl_node, './@height')
            pl_weight = extract_data(pl_node, './@weight')
            pl_jersey_number = extract_data(pl_node, './@jersey_number')
            pl_gender = extract_data(pl_node, './@gender')
            pl_prefered_foor = extract_data(pl_node, './@preferred_foot')
            pl_name = extract_data(pl_node, './@name')

            if ',' in pl_name:
                pl_n = pl_name.split(',')
                pl_n.reverse()
                pl_name = ' '.join(pl_n).strip()

            pl_values = (pl_id, pl_name, sport_id, '', '', '', response.url, '', pl_nation, pl_country_code, pl_role, '', pl_height, pl_weight, pl_dob, '', '', '', '', '', '', sport_id)
            self.cursor.execute(INT_PL, pl_values)

            roster_values = (team_id, pl_id, pl_role, pl_jersey_number, '', response.meta['season'], response.meta['season'])
            self.cursor.execute(INT_ROSTER, roster_values) 
