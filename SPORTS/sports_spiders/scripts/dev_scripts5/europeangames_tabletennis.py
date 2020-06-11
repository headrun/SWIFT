from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, \
extract_list_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime

DOMAIN_LINK = "http://www.baku2015.com"

class EropeanTabletennisGames(VTVSpider):

    name = "europeangames_tabletennis"
    allowed_domains = []
    start_urls = ['http://www.baku2015.com/schedules-results/sport=tt/index.html?intcmp=sr-overview']

    def parse(self, response):
        sel = Selector(response)
        ref_nodes = get_nodes(sel, '//table[@class="or-tbl or-sch"]//tr//th[@class="or-lbl-sport"]//a')

        for ref_node in ref_nodes:
            ref_url = extract_data(ref_node, './/@href')

            if ref_url:
                ref_url =  DOMAIN_LINK + ref_url
                yield Request(ref_url, callback = self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        round_nodes = get_nodes(sel, '//nav[@class="or-en"]//ul[@class="or-tabs-nav"]//li//a')

        for round_node in round_nodes:
            round_link = extract_data(round_node, './/@href')

            if round_link:
                round_link = DOMAIN_LINK + round_link
                yield Request(round_link, callback = self.parse_roundinfo)

    def parse_roundinfo(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//div[contains(@class, "or-plainRow or-row")]//a[@class="or-link-match"]')

        for node in nodes:
            result_link = extract_data(node, './/@href')

            if result_link:
                result_link = DOMAIN_LINK + result_link
                yield Request(result_link, callback = self.parse_results)

    def parse_results(self, response):
        sel = Selector(response)
        record = SportsSetupItem()

        round_info = extract_data(sel, '//div[@class="or-sb-h-info"]//h2//span//text()')
        venue = extract_data(sel, '//span[@class="or-venue"]//a//text()').strip()
        status = extract_data(sel, '//span[contains(@class, "or-status")]//text()').lower()

        if "official" in status:
            status = "completed"

        elif "live" in status:
            status = "ongoing"

        else:
            status = "scheduled"

        game_date = extract_list_data(sel, '//span[@class="or-date-time"]//time//@datetime')[0]
        game_time = extract_list_data(sel, '//span[@class="or-date-time"]//@data-or-utc')[0]
        tz_info = game_date.split(':')[-2].replace('00', '')
        game_datetime = datetime.datetime.strptime(game_time, '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        event_info = extract_data(sel, '//h1[@class="or-eh or-disc-pg"]//text()').strip()
        event_name = "European Games " + event_info
        teams = extract_list_data(sel, '//div[@class="orp-country"]//span[@class="orp-noc orp-vxs"]//text()')
        home_team = teams[0].strip()
        away_team = teams[1].strip()

        if "/women-team/" in response.url:
            home_team_sk = home_team.lower() + "_womens_tabletennis"
            away_team_sk = away_team.lower() + "_womens_tabletennis"

        elif "/men-team/" in response.url:
            home_team_sk = home_team.lower() + "_mens_tabletennis"
            away_team_sk = away_team.lower() + "_mens_tabletennis"

        results = extract_data(sel, '//div[@class="orp-score"]//div[@class="orp-score-res"]//span//text()')
        home_final = results.split('-')[0].strip()
        away_final = results.split('-')[-1].strip()
        game_sk = "".join(re.findall('/match=(.*)/index.html', response.url))

        record['game'] = "table tennis"
        record['source'] = "eoc_baku"
        record['tournament'] = "European Games - Table tennis"
        record['source_key'] = game_sk
        record['affiliation'] = "eoc"
        record['game_status'] = status
        record['game_datetime'] = game_datetime
        record['event'] = event_info
        record['reference_url'] = response.url
        record['rich_data'] = {'game_note': round_info, 'stadium': venue, \
                'location': {'city': 'Baku', 'country': 'Azerbaijan'}}
        record['time_unknown'] = 0
        record['event'] = event_name
        record['tz_info'] = tz_info
        record['participants'] = { home_team_sk: (0, ''), away_team_sk: (0, '')}
        record['result'] = {home_team_sk: { 'final': home_final}, \
                away_team_sk: { 'final': away_final}}
        record['result'].setdefault('0', {})['score'] = results.strip()

        if status == "completed":

            if int(home_final) > int(away_final):
                team_winner = home_team_sk

            elif int(home_final) < int(away_final):
                team_winner = away_team_sk

            else:
                team_winner = ''

            record['result'].setdefault('0', {})['winner'] = team_winner

        if "Gold Medal Match" in round_info and status == "completed":
            record['result'].setdefault(team_winner, {})['medal'] = "gold"
            record['result'].setdefault(away_team_sk, {})['medal'] = "silver"

        if "Bronze Medal Match" in round_info and status == "completed":
            record['result'].setdefault(team_winner, {})['medal'] = "bronze"

        if game_sk:
            yield record
