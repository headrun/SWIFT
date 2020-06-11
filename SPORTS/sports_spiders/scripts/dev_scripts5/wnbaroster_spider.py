from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
from vtvspider import VTVSpider, extract_data, get_nodes
from scrapy_spiders_dev.items import SportsSetupItem

CALL_SIGN = {
             'F' : 'Forward',
             'F-C' : 'Forward/Center',
             'C' : 'Center',
             'G-F' : 'Guard/Forward',
             'G' : 'Guard',
             }

TEAM_CALL_SIGN = {'Sparks' : 'Los Angeles Sparks',
                  'Dream'  : 'Atlanta Dream',
                  'Lynx'   : 'Minnesota Lynx',
                  'Sky'    : 'Chicago Sky', 'Mercury' : 'Phoenix Mercury', 'Sun' : 'Connecticut Sun'}

class WnbaRoster(VTVSpider):
    name = "wnba_roster"
    start_urls = ['http://www.wnba.com/statistics/feeds/json/activeleagueplayers.jsp']

        

