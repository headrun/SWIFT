import re
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, extract_data, \
get_nodes, get_utc_time, get_tzinfo


class ITMCupRugbySpider(VTVSpider):
    name = "itmcup_spider"
    allowed_domains = []
    start_urls = ['http://www.itmcup.co.nz/Fixtures']

    def parse(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        tou_details = extract_data(sel, '//div[@class="subHead"]//h2//text()')
        tou_name    = tou_details.replace('2015 ', '')
        season      = tou_details.replace(tou_name, '').strip()
        season = re.findall('\d+', season)[0]
        nodes = get_nodes(sel, '//table[@class="responsive"]//tr')
        for node in nodes:
            date_  = extract_data(node, './/td[1]//text()')
            if not date_ or 'Week' in date_ or 'ITM Cup' in date_ :
                continue
            team_details = extract_data(node, './/td[2]//text()')
            location   = extract_data(node, './/td[3]//text()'). \
            replace('PalmerstonN', 'Palmerston')
            time_      = extract_data(node, './/td[4]//text()')
            results    = extract_data(node, './/td[5]//text()')
            if  "0-0" in results:
                status = "scheduled"
            else:
                status = "completed"
            if "TBA" in location:
                location = ''
            home_sks = team_details.split(' v ')[0]
            if "-" in home_sks:
                home_sk = home_sks.split(' - ')[-1].lower()
                game_note = home_sks.split(' - ')[0]
            else:
                home_sk = home_sks.lower()
                game_note = ''
            home_sk = home_sk.replace(' ', '_'). \
            encode('utf-8').strip()
            away_sk = team_details.split(' v ')[-1].lower()
            away_sk = away_sk.replace(' ', '_'). \
            encode('utf-8').strip()
            home_scores = results.split('-')[0]
            away_scores = results.split('-')[1]
            game_date = date_ + " " +season + " "+ time_
            game_datetime = get_utc_time(game_date, '%d %b %Y %I:%M %p', 'Pacific/Auckland')
            game_sk = game_datetime.split(' ')[0].replace('-', '_') + \
                    "_" + home_sk.replace(' ', '_') + "_" + away_sk.replace(' ', '_')

            if "final" in team_details or "Final" in team_details:
                game_note = team_details
                home_sk = "tbd1"
                away_sk = "tbd2"
                game_sk = game_datetime.split(' ')[0].replace('-', '_') + \
                    "_" + home_sk.replace(' ', '_') + "_" + away_sk.replace(' ', '_')+ "_" + game_note

            if location in ["Albany", "Pukekohe"]:
                location = "Auckland"
            if location == "Palmerston":
                location = "Dunedin"
            if location == '':
                country = ''
                location = ''
            else:
                country = 'New Zealand'
                location = location
            tz_info = get_tzinfo(city = location, \
                            game_datetime = game_datetime)
            record['tz_info'] = tz_info
            if not tz_info:
                tz_info = get_tzinfo(city = location, country = country, \
                            game_datetime = game_datetime)
                record['tz_info'] = tz_info

            record['affiliation'] = 'irb'
            record['game_datetime'] = game_datetime
            record['game'] = 'rugby union'
            record['source'] = 'itmcup_rugby'
            record['game_status'] = 'scheduled'
            record['event'] = ''
            record['tournament'] = tou_name
            record['participant_type'] = "team"
            record['source_key'] = game_sk
            record['time_unknown'] = '0'
            record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
            record['rich_data'] = {'location': {'city': location, \
                                   'country': country}, \
                                   'game_note': game_note}
            record['reference_url'] = response.url
            record['result'] = {}
            record['game_status'] = status
            if record['game_status'] == "scheduled":
                yield record
            else:
                if int(home_scores) > int(away_scores):
                    winner = home_sk
                elif int(home_scores) < int(away_scores):
                    winner = away_sk
                else:
                    winner = ''

                totla_score = home_scores + "-" + away_scores + " FT"
                record['result'] = {'0': {'score': totla_score, \
                            'winner': winner}, \
                            home_sk: {'final': home_scores},
                            away_sk: {'final': away_scores}}
                import pdb;pdb.set_trace()
                yield record


