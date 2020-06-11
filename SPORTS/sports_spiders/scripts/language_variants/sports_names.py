import re
import datetime
from scrapy.http import Request
from scrapy.selector import Selector
from sports_spiders.items import SportsSetupItem
from vtvspider import VTVSpider, get_nodes, extract_data
from vtvspider import extract_list_data, get_utc_time, get_tzinfo
import MySQLdb

conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTS_FILMS", charset='utf8', use_unicode=True)
cur = conn.cursor()

sports_dict = {}

class SportsNamesSpider(VTVSpider):
    name = 'sports_names'
    start_urls  = ['https://en.wikipedia.org/wiki/List_of_sports']

    def parse(self, response):
        sel = Selector(response)
        sport_nodes = sel.xpath('//div[@class="mw-content-ltr"][@id="mw-content-text"]//a')
        for node in sport_nodes:
            sport_name = ''.join(node.xpath('./text()').extract())
            sports_link = ''.join(node.xpath('./@href').extract())
            sports_dict.update({sport_name.lower():sports_link})

        qry = 'select distinct sport_name from sports_movies'
        cur.execute(qry)
        rows = cur.fetchall()
        for row in rows:
            row = row[0]
            if row.lower() in sports_dict.keys():
                wiki_link = sports_dict[row.lower()]
                if 'http' not in wiki_link: wiki_link = 'https://en.wikipedia.org' + wiki_link
                yield Request(wiki_link, callback = self.parse_names, meta = {'sp': row})

    def parse_names(self, response):
        sel = Selector(response)
        temp = ''.join(sel.xpath('//script//text()').extract())
        wiki_id = ''.join(re.findall('ArticleId\":(.*?),\"wgIsArt', temp))
        spt = response.meta['sp']+'{WIKI%s}'%wiki_id
        qry = 'update sports_movies set sport_name = %s where sport_name = %s'
        vals = (spt, response.meta['sp'])
        cur.execute(qry, vals)

            
