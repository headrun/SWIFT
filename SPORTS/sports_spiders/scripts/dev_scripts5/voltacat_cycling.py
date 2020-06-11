from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
get_nodes, extract_list_data, get_utc_time, get_tzinfo
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime


class VoltacatCycling(VTVSpider):
    name="voltacat_cycling"
    allowed_domains = []
    start_urls = ['http://www.voltacatalunya.cat/ca/etapes/']

    def parse(self, response):
        import pdb;pdb.set_trace()
        hxs = Selector(response)
        record = SportsSetupItem()
        st_nodes = get_nodes(hxs, '//div[@class="fila"]')
        for node in st_nodes:
            stage_date = extract_data(node, './/div[@class="cela amp80 centre"][1]/text()')
            stages =  extract_data(node, './/div[@class="cela amp50 centre"]/text()')
            stages = "Stage "+stages
            distance = extract_data(node, './/div[@class="cela amp60 dreta"]/text()')
            game_note = extract_data(node, './/div[@class="cela amp320"]//text()')
            game_note = stages + " " +game_note + " " +"- " + distance.replace(',', '.') + " km"
            pattern = "%d/%m/%Y"
            tou_datetime = get_utc_time(stage_date, pattern, 'US/Eastern')
            today_date = datetime.datetime.now().date()
            game_sk = "Volta_Ciclista_a_Catalunya" +"_" +stages.replace(' ', '_') +"_"+tou_datetime.split(' ')[0].replace('-', '')
            if tou_datetime.split(' ')[0] < str(today_date):
                status = "completed"
            elif tou_datetime.split(' ')[0] == str(today_date):
                status = 'ongoing'
            else:
                status = 'scheduled'
            record['tournament']    = "Volta Ciclista a Catalunya"
            record['game_datetime'] = tou_datetime
            record['game']          = "cycling"
            record['game_status']   = status
            record['source_key']    = game_sk
            record['reference_url'] = response.url
            record['source']        = "uciworldtour_cycling"
            record['affiliation']   = "uci"
            record['rich_data'] = {'game_note': game_note}
            record['participant_type'] = "player"
            record['time_unknown'] = '1'
            if status=="scheduled":
                record['participants'] = {}
                record['result'] = {}
                import pdb;pdb.set_trace()
                yield record
            else:
                sc_nodes = get_nodes(hxs, '//div[@class="nav_tappe"]//ul//li//a')
                for node in sc_nodes:
                    score_link = extract_data(node, './/@href')
                    score_link = "http://www.gazzetta.it" +score_link
                    yield Request(score_link, callback=self.parse_scores)


    def parse_scores(self, parse):
        pass


