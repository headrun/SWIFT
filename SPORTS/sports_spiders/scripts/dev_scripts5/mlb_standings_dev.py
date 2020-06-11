import datetime
import time
from vtvspider import VTVSpider
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import HtmlXPathSelector
from vtvspider_dev import VTVSpider, extract_data, extract_list_data, \
                              get_nodes, get_utc_time
divisions = {'National League East' : 'NL East', 'National League Central' : 'NL Central', 'National League West' : 'NL West', 'American League East' : 'AL East', 'American League West' : 'AL West', 'American League Central' : 'AL Central'}

conference = {'National League East': 'National League', 'National League Central': 'National League', 'National League West': 'National League', 'American League West' : 'American League', 'American League Central' : 'American League', 'American League East' :'American League'}
class MlbStandings(VTVSpider):
    name = "mlb_standings_dev"
    start_urls = []

    def start_requests(self):
        next_week_days = []
        urls = []
        top_url = "http://mlb.mlb.com/lookup/json/named.standings_schedule_date.bam?season=2014&schedule_game_date.game_date='%s' \
                    &sit_code='h0'&league_id=103&league_id=104&all_star_sw='N'&version=2"
        for i in range(1, 2):
            next_week_days.append((datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y/%m/%d'))
        for wday in next_week_days:
            url = top_url % (wday)
            yield Request(url, callback = self.parse_standings, meta= {})

    def parse_standings(self, response):
        record = SportsSetupItem()
        url = response.url
        data = eval(response.body)
        all_data = data.get('standings_schedule_date', {}).get('standings_all_date_rptr', {}).get('standings_all_date', [])
        info_dict = {}
        req_dict = {}
        old_division = ''
        for i in all_data:
            record['result'] = {}
            _id = i.get('league_id', '')
            season = i.get('queryResults', {}).get ('created', '')
            season = season.split('-')
            season = season[0]
            record['season'] = season
            rows = i.get('queryResults', {}).get('row', [])
            for row in rows:
                division = row.get('division', '')
                if old_division != division:
                    count = 0
                count += 1
                old_division = division
                group = divisions.get(division, '')
                team = row.get('team_abbrev', '')
                wins = row.get('w', '')
                loses = row.get('l', '')
                pct = row.get('pct', '')
                gb = row.get('gb', '')
                elim = row.get('elim', '')
                gb_wildcard = row.get('gb_wildcard', '')
                last_ten = row.get('last_ten', '')
                streak = row.get('streak', '')
                home = row.get('home', '')
                away = row.get('away', '')
                record['tournament']    = group
                record['source']        = "MLB"
                record['participant_type'] = 'team'
                record['affiliation'] = 'mlb'
                record['result_type']   = 'tournament_standings'
                #record['result'] = {team: {'W': wins, 'L': loses, 'PCT' : pct, 'GB' : gb, 'L10' : last_ten, 'STRK' : streak, 'HOME' : home, 'ROAD' : away, 'RANK': count}
                #yield record
                info_dict[team] = {'W': wins, 'L': loses, 'PCT' : pct, 'GB' : gb, 'L10' : last_ten, 'STRK' : streak, 'HOME' : home, 'ROAD' : away, 'RANK': ''}
                req_dict[wins] = team
                rank = 0
                for values in  sorted(req_dict.keys(), reverse=True):
                    rank +=  1
                    title = req_dict[values]
                    record_ = info_dict[title]
                    record_['RANK'] = rank
                record['result'] = record_
                #yield record
