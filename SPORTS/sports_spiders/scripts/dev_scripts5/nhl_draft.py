from scrapy.http import Request
import re
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
             get_nodes, extract_list_data

class NHLDraft(VTVSpider):
    name = "nhl_draft"
    allowed_domains = []
    start_urls = ['http://www.nhl.com/ice/draftsearch.htm?year=2016&team=&position=&round=1']

    def parse(self, response):
        sel = Selector(response)
        tou_name = extract_data(sel, '//div[@id="pageHeader"]//h3//text()')
        season = "".join(re.findall(r'\d+', tou_name))
        tou_name = tou_name.replace(season, '').strip()
        nodes = get_nodes(sel, '//table[@class="data"]//tr')
        for node in nodes:
            round_ = extract_data(node, './/td[1]//text()')
            pick   = extract_data(node, './/td[2]//text()') 
            overall = extract_data(node,'.//td[3]//text()')
            player_id = extract_data(node, './/a[@rel="playerLinkData"]//@href')
            player_id = re.findall(r"\d+", player_id)
            if player_id:
                player_id = player_id[0]
            else:
                continue
            record = SportsSetupItem()
            record['result_type'] = "tournament_standings"
            record['season'] = season
            record['tournament'] = "NHL Entry Draft"
            record['participant_type'] = "player"
            record['source'] = 'NHL'
            record['result'] = {player_id: {'rank': overall, 'pick': pick, 'round': round_}}
            yield record

