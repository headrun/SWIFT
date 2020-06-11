from vtvspider_new import VTVSpider, get_nodes, \
             extract_data, extract_list_data, get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem

STADIUM_CITY_DICT = {'Eden Park':['Eden Park', 'Oceania', 'New Zealand', 'Auckland', 'Kingsland'], \
                    'Cbus Super Stadium': ['Cbus Super Stadium', 'Australia', 'Australia', 'Queensland', 'Gold Coast'], \
                    'Suncorp Stadium': ['Suncorp Stadium', 'Australia', 'Australia', 'Queensland', 'Brisbane'], \
                    'ANZ Stadium': ['ANZ Stadium', '', 'Australia', 'New South Wales', 'Sydney'], \
                    'Belmore Sports Ground': ['Belmore Sports Ground', '', 'Australia', 'New South Wales', 'Sydney'], \
                    'GIO Stadium': ['GIO Stadium', '', 'Australia', '', 'Canberra'], \
                    'Brookvale Oval': ['Brookvale Oval', '', 'Australia', 'New South Wales', 'Sydney'], \
                    'AAMI Park': ['AAMI Park', 'Australia', 'Australia', 'Victoria', 'Melbourne'], \
                    'Hunter Stadium': ['Newcastle International Sports Centre', '', 'Australia', 'New South Wales', 'Newcastle'], \
                    'Mt Smart Stadium': ['Mt Smart Stadium', '', 'New Zealand', '', 'Auckland'], \
                    'Smiles Stadium': ['Willows Sports Complex', 'Australia', 'Australia', 'Queensland', 'Townsville'], \
                    '1300 Smiles Stadium': ['Willows Sports Complex', 'Australia', 'Australia', 'Queensland', 'Townsville'], \
                    'Pirtek Stadium': ['Parramatta Stadium', '', 'Australia', 'New South Wales', 'Sydney'], \
                    'Pepper Stadium': ['Penrith Stadium', 'Australia', 'Australia', 'New South Wales', 'Sydney'], \
                    'Jubilee Oval': ['Jubilee Oval', '', 'Australia', 'New South Wales', 'Sydney'],\
                    'Allianz Stadium': ['Allianz Stadium', '', 'Australia', 'New South Wales', 'Sydney'], \
                    'Campbelltown Stadium': ['Campbelltown Stadium', '', 'Australia', 'New South Wales', 'Newcastle'], \
                    'Leichhardt Oval': ['Leichhardt Oval', '', 'Australia', 'New South Wales', 'Newcastle'], \
                    'WIN Stadium': ['WIN Stadium', 'Australia', 'Australia', 'Queensland', 'Brisbane'], \
                    'Remondis Stadium': ['Remondis Stadium', '', 'Australia', 'New South Wales', 'Sydney'], \
                    'TIO Stadium': ['Marrara Stadium', '', 'Australia', 'Northern Territory', 'Darwin'], \
                    'Barlow Park': ['Barlow Park', '', 'Australia', 'Queensland', 'Cairns'], \
                    'Sydney Cricket Ground': ['Sydney Cricket Ground', '', 'Australia', 'New South Wales', 'Sydney'], \
                    'Carrington Park': ['Carrington Park', '', 'Australia', 'New South Wales', 'Bathurst'], \
                    'Langtree Park': ['Langtree Park', '', 'England', '', 'Merseyside'], \
                    'DW Stadium': ['DW Stadium', '', 'England', '', 'Wigan'], \
                    'Halliwell Jones Stadium': ['Halliwell Jones Stadium', '', 'England', '', 'Warrington'], \
                    'Central Coast Stadium': ['Central Coast Stadium', '', 'Australia', 'New South Wales', 'Gosford'], \
                    'nib Stadium': ['NIB Stadium', '', 'Australia', '', 'Perth'], \
                    'Melbourne Cricket Ground': ['Melbourne Cricket Ground', 'Australia', 'Australia', 'Victoria', 'Melbourne']}

ROUND_URL = "http://www.nrl.com/DrawResults/TelstraPremiership/Draw/tabid/11180/s/42/r/%s/sc/cWOFGg40w000/default.aspx"
ROUND_URL = "http://www.nrl.com/DrawResults/TelstraPremiership/Draw/tabid/11180/s/43/s/%s/sc/cPgMHaCF0g20/default.aspx"

class NRLRugby(VTVSpider):
    name = "nrl_spider"
    start_urls = ['http://www.nrl.com/DrawResults/TelstraPremiership/Draw/tabid/11180/Default.aspx']

    def parse(self, response):
        hxs = Selector(response)
        round_ids = extract_list_data(hxs, '//label[contains(@class, " drawCentreFilters__filter__option series_1")]//@value')
        for round_id in round_ids[:1]:
            url = ROUND_URL % round_id
            yield Request(url, callback=self.parse_round_results)

    def parse_round_results(self, response):
        record = SportsSetupItem()
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="drawCentreGame"]// \
                    div[contains(@class, "drawGame drawGame")]')
        for node in nodes:
            home_sk = extract_data(node, './/div[@class="drawGame__name drawGame__name--home"]//strong/text()')
            away_sk = extract_data(node, './/div[@class="drawGame__name drawGame__name--away"]//strong/text()')
            game_time = extract_data(node, './/@o_date')
            game_sk = extract_data(node, './/@matchcode')
            game_id = game_time + '-' + game_sk
            game_tag = extract_data(node, './/div[@class="drawGame__hashTag"]//text()')
            if not game_tag:
                continue
            home_scores = extract_data(node, './/div[contains(@class, "drawGame__score--home--score drawGame__score--home--post")]//strong/text()')
            away_scores = extract_data(node,'.//div[contains(@class, "drawGame__score--away--score drawGame__score--away--post")]//strong/text()')
            stadium  = extract_data(node, './/h2[@class="drawGame__timeLocation"]//@venuename')
            if not stadium:
                stadium  = extract_data(node, './/span[@class="venue "]//text()')
            game_note = extract_data(node, './/span[@class="round"]//text()').replace('R', 'Round ')
            if game_note:
                game_notes = game_note + " " + home_sk + " " + 'vs' + " " +away_sk
            else:
                game_notes = ""
            game_datetime = extract_data(node, './/h2//span//@utc'). \
                                replace('T', ' ').replace('Z', '')
            print game_datetime
            match_status = extract_list_data(node, './/div[contains(@class, "drawGame__links__set1")]/a/@buttontype')[-1]
            if "0000-00-00 00:00:00" in game_datetime:
                continue
            if not game_datetime:
                continue
            if "2015-08" not in game_datetime:
                continue
            import pdb;pdb.set_trace()
            total_score = home_scores + "-" + away_scores
            final_stadium = STADIUM_CITY_DICT.get(stadium, [])
            if final_stadium:
                stadium  = final_stadium[0]
                continent = final_stadium[1]
                country   = final_stadium[2]
                state     = final_stadium[3]
                city      = final_stadium[4]
            else:
                stadium   = ''
                continent = ''
                country   = ''
                state     = ''
                city      =  ''
            tz_info = get_tzinfo(city = city, game_datetime = game_datetime)
            record['tz_info'] = tz_info
            if not tz_info:
                tz_info = get_tzinfo(city = city, country = country, game_datetime = game_datetime)
                record['tz_info'] = tz_info
                if not tz_info:
                    if "England" in country:
                        tz_info = get_tzinfo(country = "United Kingdom", game_datetime = game_datetime)
                        record['tz_info'] = tz_info
            record['rich_data'] = {'location': {'city': city, 'country': country, \
                                       'continent': continent, 'state': state, \
                                       'stadium': stadium}, 'game_note': game_notes}

            if home_scores:
                status = "completed"
                record['game_status'] = "completed"
                if status == "completed":

                    if int(away_scores) > int(home_scores):
                        winner = away_sk
                    elif int(home_scores) > int(away_scores):
                        winner = home_sk
                    else:
                        winner = ''
                    record['result'] = {'0': {'winner': winner, \
                                              'score': total_score}, \
                                              home_sk: {"final": home_scores}, \
                                              away_sk: {"final": away_scores}}
            elif not home_scores:
                record['game_status'] = "scheduled"
                record['result'] = {}
            record['game'] = "rugby league"
            record['affiliation'] = "nrl"
            record['source'] = "nrl_rugby"
            record['tournament'] = "National Rugby League"
            record['source_key'] = game_id
            record['game_datetime'] = game_datetime
            record['reference_url'] = response.url
            record['participants'] = {home_sk: (1, ''), away_sk: (0, '')}
            record['time_unknown'] = 0
            import pdb;pdb.set_trace()
            yield record


