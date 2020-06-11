from vtvspider_new import VTVSpider, get_nodes, extract_data, extract_list_data, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
import datetime

RECORD = SportsSetupItem()

class AustraliaOpenGolf(VTVSpider):
    name = "golf_australia"
    start_urls = ['http://www.pga.org.au/tourns/pga-tour/schedule']

    tou_name = "Australian Open (golf)"
    RECORD['game'] = 'golf'
    RECORD['source'] = 'pga_golf'
    RECORD['affiliation'] = 'pga'
    RECORD['participant_type'] = 'player'
    RECORD['tournament'] = tou_name
    RECORD['season'] = "2016"
    RECORD['event'] = ''
    RECORD['time_unknown'] = "1"

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//table[@class="data reverse-styles"]/tbody/tr')

        for node in nodes:
            tou_name = extract_data(node, './td/a/text()')

            if "Australian Open" not in tou_name:
                continue

            tou_st_date, tou_end_date = extract_data(node, './/td//small/text()').split(' - ')
            game_datetime = get_utc_time(tou_st_date.strip(), "%d/%m/%y", "Australia/Sydney")
            tou_venue = ','.join(data.strip() for data in extract_data(node, './td[2]/text()').split(',')).split(',')
            tou_link = extract_data(node, './td/a/@href')
            today_date = str(datetime.datetime.utcnow().date())
            st_date = str(game_datetime.split(' ')[0])
            #en_date = str(tou_end_date.replace('/', '-'))
            en_date = str(datetime.datetime.strptime(tou_end_date, '%d/%m/%y'))
            if st_date <= today_date and today_date <= en_date:
                status = "ongoing"
            elif en_date < today_date:
                status = "completed"
            else:
                status = "scheduled"
            source_key = self.tou_name.replace(' ', '_') + "_" + \
                         tou_st_date.replace('/', '-') + "_" + \
                         tou_end_date.replace('/', '-')
            tz_info = get_tzinfo(city= tou_venue[1])
            RECORD['game_status'] = status
            RECORD['tz_info'] = tz_info
            RECORD['source_key'] = source_key
            RECORD['rich_data'] = {"channels": '', "game_note": '',
                                    "location": {'city': tou_venue[1]}, "stadium": tou_venue[0]}
            RECORD['location_info'] = tou_venue[1]
            RECORD['game_datetime'] = game_datetime
            RECORD['reference_url'] = tou_link
            RECORD['participants'] = {}
            RECORD['result'] = {}
            if status == 'scheduled':
                yield RECORD
            else:
                yield Request(tou_link, callback=self.parse_next, meta={'status': status})

    def parse_next(self, response):
        hxs = Selector(response)
        players = {}
        result = {}
        tee_times_link = extract_data(hxs, '//div[@class="tabs-nav"]//li/a[contains(@href, "tee-times")]/@href')
        leaderboard_link = extract_data(hxs, '//div[@class="tabs-nav"]//li/a[contains(@href, "leaderboard")]/@href')
        if response.meta['status'] == "scheduled":
            yield Request(tee_times_link, callback=self.parse_tee_times)
        else:
            nodes = get_nodes(hxs, '//table[@class="data-special"]/tbody/tr')

            for node in nodes:
                position = extract_data(node, './/td[2]//text()')
                if not position:
                    continue
                first_name = extract_data(node, './/td//span[@class="player-i"]/span[2]/text()')
                last_name = extract_data(node, './/td[contains(@class, "player")]//strong/text()')
                player_name = first_name + " " + last_name
                player_sk = player_name.replace(' ', '-').lower()
                if position == "1":
                    winner = player_sk
                    result['0'] = {'winner': winner}
                to_par = extract_data(node, './td[@class="constant-cell"]/strong/text()')
                score = extract_list_data(node, './td[@class="highlight-cell"]/text()')
                total = extract_data(node, './td[12]//text()')
                if not total:
                    total = int(score[0]) + int(score[1])
                players[player_sk] = (0, player_name)
                result[player_sk] = {'R1': score[0], 'R2': score[1], 'R3': score[2], 'R4': score[3], 'TO PAR': to_par, 'position': position, 'final': total}
            RECORD['result'] = result
            RECORD['game_status'] = "completed"
            RECORD['participants'] = players
            yield RECORD


    def parse_tee_times(self, response):
        hxs = Selector(response)
        result = {}
        players_dict = {}

        root_nodes = get_nodes(hxs, '//div[@class="tabs-panel"]//div')

        for root_node in root_nodes:
            round_num = extract_data(root_node, './h3/text()').replace('Round ', 'R').replace(' ', '_').replace('times', 'time')
            nodes = get_nodes(root_node, './/div[@class="table-wrapper tee-times"]//table/tbody/tr')

            for node in nodes:
                tee_time = extract_data(node, './td/span[@class="time-convert"]/text()')
                players = extract_list_data(node, './td[5]//text()')
                for player in players:
                    player_sk = player.strip().replace(' ', '-').lower()
                    players_dict[player_sk] = (0, player)
                    result[player_sk] = {round_num: tee_time}
            RECORD['result'] = result
            RECORD['game_status'] = "ongoing"
            RECORD['participants'] = players_dict
            yield RECORD
