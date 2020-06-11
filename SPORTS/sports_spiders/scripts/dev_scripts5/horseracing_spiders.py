import datetime
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_new import VTVSpider, extract_data, get_nodes, \
get_utc_time, get_tzinfo, extract_list_data

true    = True
false   = False
null    = ''



class HoreseRacingSpide(VTVSpider):
    name            = "horseracing_gamess"
    allowed_domains = []
    start_urls      = ['http://www.horseracingnation.com/race/2016_Randwick_Guineas', \
                        'http://www.horseracingnation.com/race/2016_Stewards_Cup', \
                        'http://www.horseracingnation.com/race/2016_Florida_Derby', \
                        'http://www.horseracingnation.com/race/2016_Hong_Kong_Gold_Cup', \
                        'http://www.horseracingnation.com/race/2016_Rosehill_Guineas', \
                        'http://www.horseracingnation.com/race/2016_Breeders_Stakes', \
                        'http://www.horseracingnation.com/race/2016_Australian_Derby', \
                   'http://www.horseracingnation.com/race/2016_Champions_Chater_Cup', \
                   'http://www.horseracingnation.com/race/2016_Epsom_Derby', \
                   'http://www.horseracingnation.com/race/2016_English_2000_Guineas', \
                   'http://www.horseracingnation.com/race/2016_Champions_and_Chater_Cup', \
                   'http://www.horseracingnation.com/race/2016_Kings_Stand_Stakes',
                   'http://www.horseracingnation.com/race/2016_Queen_Anne_Stakes',
                    'http://www.horseracingnation.com/race/2016_St_Jamess_Palace_Stakes',
                    'http://www.horseracingnation.com/race/2016_Ascot_Gold_Cup',
                    'http://www.horseracingnation.com/race/2016_Prince_Of_Waless_Stakes',
                    'http://www.horseracingnation.com/race/2016_Diamond_Jubilee_Stakes',
                    'http://www.horseracingnation.com/race/2016_Coronation_Stakes', \
                    'http://www.horseracingnation.com/race/2016_Coventry_Stakes', \
                    'http://www.horseracingnation.com/race/2016_Queen_Mary_Stakes', \
                    'http://www.horseracingnation.com/race/2016_Duke_of_Cambridge_Stakes', \
                    'http://www.horseracingnation.com/race/2016_Ribblesdale_Stakes', \
                    'http://www.horseracingnation.com/race/2016_Norfolk_Stakes', \
                    'http://www.horseracingnation.com/race/2016_King_Edward_VII_Stakes', \
                    'http://www.horseracingnation.com/race/2016_Hardwicke_Stakes', \
                    'http://www.horseracingnation.com/race/2016_Jersey_Stakes', \
                    'http://www.horseracingnation.com/race/2016_Tercentenary_Stakes', \
                    'http://www.horseracingnation.com/race/2016_Queens_Vase', \
                    'http://www.horseracingnation.com/race/2016_Albany_Stakes', \
                    'http://www.horseracingnation.com/race/2016_English_St_Leger', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Distaff', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Juvenile', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Turf', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Sprint', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Dirt_Mile', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Turf_Sprint', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Filly_and_Mare_Sprint', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Juvenile_Turf', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Juvenile_Fillies', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Juvenile_Fillies_Turf', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Mile', \
                    'http://www.horseracingnation.com/race/2016_Breeders_Cup_Filly_and_Mare_Turf',
                    'http://www.horseracingnation.com/race/2016_Kentucky_Derby', \
                  'http://www.horseracingnation.com/race/2016_Belmont_Stakes', \
                  'http://www.horseracingnation.com/race/2016_Preakness_Stakes']

    def parse(self, response):
        hxs = Selector(response)
        record          = SportsSetupItem()
        tou_name = extract_data(hxs, '//h1[@class="headline"]/text()').strip() \
                .split('(')[0].strip().split('2016')[-1].strip()

        if tou_name == '':
            return

        game_note = ''
        if "Stewards' Cup" in tou_name:
            tou_name = "Hong Kong Stewards' Cup"
        if "Prince of Wales Stakes" in tou_name:
            tou_name = "Prince of Wales Stakes"
        if "English 2000 Guineas" in tou_name:
            tou_name = "2,000 Guineas Stakes"
        if "Champions & Chater Cup" in tou_name:
            tou_name = "Hong Kong Champions & Chater Cup"
        if "Albany Stakes" in tou_name:
            tou_name = "Albany Stakes (Great Britain)"
        if "Norfolk Stakes" in tou_name:
            tou_name = "Norfolk Stakes (Great Britain)"
        if "English St. Leger" in tou_name:
            tou_name = "St. Leger Stakes"
        if ("Breeders Cup" in tou_name) or ("Breeders' Cup" in tou_name):
            game_note = tou_name.split('Cup')[-1].strip()
            tou_name = "Breeder's Cup"

        tou_date = extract_data(hxs,'//div[@class="row"]/div[@class="title"][contains(text(),"Date/Track:")]\
                               /following-sibling::div[@class="value"]/text()').replace(',','')
        tou_time = extract_data(hxs,'//div[@class="row"]/div[@class="title"][contains(text(),"Post Time:")]\
                                /following-sibling::div[@class="value"]/text()')
        if tou_name:
            if tou_time:
                _date = tou_date+ " "+tou_time
                _date = _date.replace('ET', '').replace('PT', '').strip()
                pattern = "%m/%d/%Y %I:%M %p"
                _datetime = get_utc_time(_date, pattern, 'US/Eastern')
                time_unknown = '0'
            else:
                _date = tou_date
                pattern = "%m/%d/%Y"
                if "Hong Kong" in tou_name:
                    _datetime = get_utc_time(_date, pattern, 'Asia/Shanghai')
                elif tou_name in ['Randwick Guineas', 'Rosehill Guineas', 'Australian Derby']:
                    _datetime = get_utc_time(_date, pattern, 'Australia/Sydney')
                elif tou_name in ["Queen's Plate", 'Prince of Wales Stakes', "Breeders' Stakes"]:
                    _datetime = get_utc_time(_date, pattern, 'America/Toronto')
                elif tou_name in ["Breeder's Cup", "Kentucky Derby", "Preakness Stakes", "Belmont Stakes"]:
                    _datetime = get_utc_time(_date, pattern, 'US/Eastern')
                else:
                    _datetime = get_utc_time(_date, pattern, 'GMT')
                time_unknown = '1'
            today_date = str(datetime.datetime.utcnow())
            if today_date <  _datetime:
                status = "scheduled"
            else:
                status = "completed"
            winner = extract_data(hxs, '//table/tr//td[@class="rank"][1][contains(text(), "st")]/following-sibling::td/span[@class="horse-name-link"]/a/@href')
            winner = winner.split('/')[-1]

            game_sk = response.url.split('/')[-1]
            stad_link = extract_data(hxs, '//div[@class="row"]/div[@class="title"][contains(text(),"Date/Track:")]\
                                     /following-sibling::div[@class="value"]/a/@href')
            record['participants'] = {}
            record['result'] = {}
            if  winner:
                record['result'] = {'0': {'winner': winner}}

            nodes = get_nodes(hxs, '//table[@id="ctl00_MainContent_uxRaceDetail_uxRankings_uxRankMain_uxGridView2"]/tr')
            rank_ = extract_list_data(hxs, '//td[@class="rank"][not(contains(text(), "-"))]/text()')

            if today_date > _datetime and "1st " in rank_[0]:
                 status = "completed"
            else:
                status = "scheduled"

            for node in nodes:
                rank = extract_data(node, './td[@class="rank"][not(contains(text(), "-"))]/text()').strip()
                if "st" in rank:
                    rank = rank.replace('st', '')
                elif "nd" in rank:
                    rank = rank.replace('nd', '')
                elif "rd" in rank:
                    rank = rank.replace('rd', '')
                elif "th" in rank:
                    rank = rank.replace('th', '')
                pl_sk = extract_data(node, './td/span[@class="horse-name-link"]/a/@href')
                title = extract_data(node, './a/text()')
                rating = extract_data(node, './td[@class="rank hidesmall"]//text()')
                pl_sk = pl_sk.split('/')[-1]
                if pl_sk == '':
                    continue
                record['participants'].update({pl_sk: (0, title)})
                if status == "completed":
                    record['result'].update({pl_sk: {'rank': rank, 'rating': rating}})

            ref = response.url
            record['affiliation'] = "ntra"
            record['game'] = "horse racing"
            record['game_datetime'] = _datetime
            record['game_status'] = status
            record['participant_type'] = "player"
            record['tournament'] = tou_name
            record['reference_url'] = ref
            record['source'] = 'horseracing_nation'
            record['source_key'] = game_sk
            record['time_unknown'] = time_unknown
            if stad_link:
                yield Request(stad_link, callback=self.parse_location_details, \
                            meta = {'type': self.spider_type, 'record': record, \
                            'tou_name': tou_name, '_datetime': _datetime, \
                            'game_sk': game_sk, 'game_note': game_note}, \
                            dont_filter=True)

    def parse_location_details(self, response):
        hxs       = Selector(response)
        loc_dict  = {'NY': 'New York', 'KY': 'Kentucky', \
                        'MD':'Maryland', 'FL': 'Florida'}
        cou_dict  = {'CAN': 'Canada', 'GB': 'Great Britain'}
        record    = response.meta['record']
        tou_name  = response.meta['tou_name']
        _datetime = response.meta['_datetime']
        stadium   = extract_data(hxs, '//h1[@class="headline"]//text()').strip()
        loc_info  = extract_data(hxs, '//div[@class="row"]/div[contains(text(), "Location:")]\
                                /following-sibling::div[@class="col_val"]//text()').strip()
        loc_info  = loc_info.split(' ')
        city      = loc_info[0].strip().replace(',', '')
        country_   = loc_info[-1].strip()
        country   = cou_dict.get(country_, '')
        state_     = loc_info[1].strip().replace(',', '')
        state     = loc_dict.get(state_, '')
        game_note = response.meta['game_note']
        if not state:
            state = state_
        if not country:
            country = country_

        if "Doncaster-GB" in stadium:
            stadium = "Doncaster Racecourse"
            city = "Doncaster"
            country = "England"
            state = ''
        if "Sha Tin-Hong Kong" in stadium:
            city = "Hong Kong"
            country = "China"
            state = ''
        if "-AUS" in stadium:
            city    = "Sydney"
            state   = "New South Wales"
            country = "Australia"
        if "Randwick-AUS" in stadium:
            stadium = "Randwick Racecourse"
        if "Rosehill" in stadium:
            stadium = "Rosehill Gardens Racecourse"
        if "Woodbine" in stadium:
            stadium =  "Woodbine Racetrack"
        if "Ascot-GB" in stadium:
            stadium = "Ascot Racecourse"
        if "Newmarket-GB" in stadium:
            stadium = "Newmarket Racecourse"
            country = "England"
        if "Sha Tin-Hong Kong" in stadium:
            stadium = "Sha Tin Sports Ground"
        if city == "Rexdale":
            city = "Toronto"
        if "Epsom-GB" in stadium:
            country = "England"
            city = "Epsom"
            stadium = "Epsom Downs Racecourse"
        if tou_name in ["St. James's Palace Stakes", 'Queen Anne Stakes', \
                        'Ascot Gold Cup', 'Coronation Stakes', \
                        "Prince of Wales's Stakes", 'Coventry Stakes', \
                        'Queen Mary Stakes', 'Duke of Cambridge Stakes', \
                        'Ribblesdale Stakes', 'Norfolk Stakes (Great Britain)', \
                        'King Edward VII Stakes', 'Hardwicke Stakes', \
                        'Jersey Stakes', 'Tercentenary Stakes', \
                        "Queen's Vase", 'Albany Stakes (Great Britain)']:
            record['tournament'] = "Royal Ascot"
            record['event'] = tou_name
        if tou_name == "Breeder's Cup":
            record['event'] = "Breeders' Cup " + response.meta['game_note'].replace('and', '&').replace("Fillies'", "Fillies").strip()
            game_note = ''


        location  = {'stadium': stadium, 'city': city, \
                        'country': country, 'state': state}


        record['rich_data'] = {}
        record['rich_data'].update({'channel': '', 'location': location})
        record['tz_info']      = get_tzinfo(city = city, game_datetime = _datetime)
        if country == "Great Britain" or country == "England":
            record['tz_info'] = get_tzinfo(country = "United Kingdom", \
                            game_datetime = _datetime)
        if not record['tz_info']:
            record['tz_info'] = get_tzinfo(city = city, country = country, \
                                    game_datetime = _datetime)
        record['rich_data'].update({'channel':'', 'location': location, \
                    'game_note': game_note})
        yield record
