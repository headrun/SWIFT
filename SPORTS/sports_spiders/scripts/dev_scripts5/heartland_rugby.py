from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, extract_data, \
get_nodes, extract_list_data, get_utc_time, get_tzinfo
import re

class HeartlandRugby(VTVSpider):
    name = "heartland_rugby"
    allowed_domains = []
    start_urls = ['http://rugbyheartland.co.nz/wp/2015-heartland-results-fixtures/']

    def parse(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        nodes = get_nodes(sel, '//div[@class="entry-content"]//table//tr')
        season = "".join(re.findall('\d+', response.url))
        for node in nodes:
            date_ = extract_data(node, './/td[1]//text()')
            if "-" not in date_:
                continue
            teams    = extract_data(node, './/td[2]//text()'). \
            encode('utf8').replace('\xc2\xa0', ' ')
            location = extract_data(node, './/td[3]//text()')
            time_    = extract_data(node, './/td[4]//text()'). \
            replace('o0', '0').replace('.', ":")
            results  = extract_data(node, './/td[5]//text()'). \
            encode('utf8').replace(' \xe2\x80\x93 ', '-').replace('.', '')
            if "0" in results:
                status = "scheduled"
            else:
                status = "completed"
            if status == "completed" and not results:
                results  = extract_data(node, './/td[4]//text()'). \
                encode('utf8').replace(' \xe2\x80\x93 ', '-')

            if "TBA" in location:
                location = ''
            home_sk = teams.split(' v ')[0].strip()
            home_sk = home_sk.replace(' ', '_'). \
            encode('utf-8').strip().lower()
            away_sk = teams.split(' v ')[-1].strip().lower()
            away_sk = away_sk.replace(' ', '_'). \
            encode('utf-8').strip()
            if "." in home_sk:
                home_sk = home_sk.split('.v.')[0].lower().strip()
                home_sk = home_sk.replace('.', '_').encode('utf-8') \
                            .strip().lower()
                away_sk = away_sk.split('.v.')[-1].lower().strip()
                away_sk = away_sk.replace('.', '_').encode('utf-8') \
                        .strip().lower()
            if status == "completed":
                home_scores = results.split('-')[0]
                away_scores = results.split('-')[1]
            if ":" not in time_:
                time_ = "2:30pm"

            if ":" in time_:

                game_date = date_ + " " + season + " "+ time_
                game_datetime = get_utc_time(game_date, '%d-%b %Y %I:%M%p', 'Pacific/Auckland')
                game_sk = game_datetime.split(' ')[0].replace('-', '_') + \
                        "_" + home_sk.replace(' ', '_') + "_" + away_sk.replace(' ', '_')
            else:
                game_date = date_ + " " + season
                game_datetime = get_utc_time(game_date, '%d-%b %Y', 'Pacific/Auckland')
                game_sk = game_datetime.split(' ')[0].replace('-', '_') + \
                    "_" + home_sk.replace(' ', '_') + "_" + away_sk.replace(' ', '_')

            if location == '':
                country = ''
                location = ''
            else:
                country = 'New Zealand'
                location = location
            if location == "Wanganui":
                location = "Whanganui"
            tz_info = get_tzinfo(city = location, \
                            game_datetime = game_datetime)
            if not tz_info:
                tz_info = get_tzinfo(city = location, country = country, \
                            game_datetime = game_datetime)
            if not tz_info:
                tz_info = get_tzinfo(country = country, game_datetime = game_datetime)

            record['tz_info'] = tz_info

            record['affiliation'] = 'irb'
            record['game_datetime'] = game_datetime
            record['game'] = 'rugby union'
            record['source'] = 'heartland_rugby'
            record['game_status'] = 'scheduled'
            record['event'] = ''
            record['tournament'] = "Heartland Championship"
            record['participant_type'] = "team"
            record['source_key'] = game_sk
            record['time_unknown'] = '0'
            record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
            record['rich_data'] = {'location': {'city': location, \
                                   'country': country}}
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
                yield record
        final_nodes = get_nodes(sel, '//table[@class="responsive"]//tr')
        for final_node in final_nodes:
            game_date = extract_data(final_node, './/td[1]//em//text()').replace('.', '-')
            game_note = extract_list_data(final_node, './/td[2]//h3//strong//text()')

            if not game_note:
                game_note = extract_list_data(final_node, './/td[2]//em//text()')
            if not game_note:
                game_note = extract_list_data(final_node, './/td[2]//h2//text()')
            if game_note:
                game_note = game_note[0].replace('#', '')
            else:
                game_note = ''
            location = extract_data(final_node, './/td[3]//text()')
            if not game_date or "TBC" in location:
                continue

            time_    = extract_data(final_node, './/td[4]//text()').replace('.', ":")
            print time_
            results  = extract_data(final_node, './/td[5]//text()'). \
                        encode('utf8').replace(' \xe2\x80\x93 ', '-')
            if not results:
                results = extract_data(final_node, './/td[4]//text()').encode('utf8').replace(' \xe2\x80\x93 ', '-')
            print results
            teams    = extract_data(final_node, './/td//p//strong//text()'). \
            encode('utf8').replace('\xc2\xa0', ' ').replace('1', '').replace('2', '')
            print teams

            if "0-0" in results:
                status = "scheduled"
            else:
                status = "completed"
            if "TBA" in location:
                location = ''
            if " v " in teams:
                home_sk = teams.split(' v ')[0].strip()
                home_sk = home_sk.split('#')[-1].strip()
                home_sk = home_sk.replace(' ', '_'). \
                encode('utf-8').strip().lower()
                away_sk = teams.split(' v ')[-1].strip().lower()
                away_sk = away_sk.replace(' ', '_'). \
                encode('utf-8').strip()

            elif " vs " in teams:
                home_sk = teams.split(' vs ')[0].strip()
                home_sk = home_sk.split('#')[-1].strip()
                home_sk = home_sk.replace(' ', '_'). \
                encode('utf-8').strip().lower()
                away_sk = teams.split(' vs ')[-1].strip().lower()
                away_sk = away_sk.replace(' ', '_'). \
                encode('utf-8').strip() 
            if "." in home_sk:
                home_sk = home_sk.split('.v.')[0].lower().strip()
                home_sk = home_sk.split('#')[-1].strip()
                home_sk = home_sk.replace('.', '_') \
                        .encode('utf-8').strip().lower()
                away_sk = away_sk.split('.v.')[-1] \
                            .lower().strip()
                away_sk = away_sk.replace('.', '_') \
                .encode('utf-8').strip().lower()

            home_scores = results.split('-')[0]
            away_scores = results.split('-')[1]

            if ":" in time_:

                game_date = game_date + " " + season + " "+ time_
                game_datetime = get_utc_time(game_date, '%d-%b %Y %I:%M%p', 'Pacific/Auckland')
                game_sk = game_datetime.split(' ')[0].replace('-', '_') + \
                        "_" + home_sk.replace(' ', '_') + "_" +  \
                        away_sk.replace(' ', '_')
            else:
                game_date = game_date + " " + season
                game_datetime = get_utc_time(game_date, '%d-%b %Y', 'Pacific/Auckland')
                game_sk = game_datetime.split(' ')[0].replace('-', '_') + \
                    "_" + home_sk.replace(' ', '_') + "_" +  \
                    away_sk.replace(' ', '_')

            if location == '':
                country = ''
                location = ''
            else:
                country = 'New Zealand'
                location = location
            if location == "Wanganui":
                location = "Whanganui"
            tz_info = get_tzinfo(city = location, \
                            game_datetime = game_datetime)
            record['tz_info'] = tz_info
            if not tz_info:
                tz_info = get_tzinfo(city = location, country = country, \
                            game_datetime = game_datetime)
                record['tz_info'] = tz_info
            if not tz_info:
                tz_info = get_tzinfo(country = country, \
                            game_datetime = game_datetime)
                record['tz_info'] = tz_info


            record['affiliation'] = 'irb'
            record['game_datetime'] = game_datetime
            record['game'] = 'rugby union'
            record['source'] = 'heartland_rugby'
            record['game_status'] = 'scheduled'
            record['event'] = ''
            record['tournament'] = "Heartland Championship"
            record['participant_type'] = "team"
            record['source_key'] = game_sk
            record['time_unknown'] = '0'
            record['participants'] = { home_sk: ('1',''), away_sk: ('0','')}
            record['rich_data'] = {'location': {'city': location, \
                                   'country': country}, 'game_note': game_note}
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
                yield record

