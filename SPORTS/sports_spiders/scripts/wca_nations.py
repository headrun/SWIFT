# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from vtvspider import VTVSpider, extract_data, get_nodes 
from scrapy.http import Request
import MySQLdb

DOMAIN_LINK = "https://en.wikipedia.org"

TM_MAX_ID_QUERY = 'select id, gid from sports_tournaments_groups where id in \
                (select max(id) from sports_tournaments_groups)'

PAT_QRY = 'select id from sports_tournaments_groups where group_name = %s'
GR_QRY = 'insert into sports_tournaments_groups(id, gid, group_name, sport_id, keywords, aka, tournament_id, group_type, info, base_popularity, image_link, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'


def check_title(self, title):
    title = (title)
    self.cursor.execute(PAT_QRY, title)
    data = self.cursor.fetchone()
    if data:
            id_ = str(data[0])
    else:
            id_ = ''
    return id_

class WCAANations(VTVSpider):
    name = "wcaa_nation"
    start_urls = ['https://en.wikipedia.org/wiki/2017_World_Championships_in_Athletics']
    
    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", \
        passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
    
    def parse(self, response):
        sel = Selector(response)
        n_nodes = get_nodes(sel, '//div[@class="div-col columns column-width"]//ul//li//a')
        for node in n_nodes:
            n_url = extract_data(node, './/@href')
            n_title = extract_data(node, './/text()')
            if "http" not in n_url:
                    n_url = DOMAIN_LINK + n_url
            yield Request(n_url, callback=self.parse_next, meta = {'title': n_title})

    def parse_next(self, response):
        sel = Selector(response)
        tm_title = extract_data(sel, '//h1[@id="firstHeading"]//text()').replace(' 2017', '').strip()
        tm_img = extract_data(sel, '//table[@class="infobox vevent"]//tr//td//img//@src')
        tm_img = "https:" + tm_img

        if 'File:' in tm_title:
            return
        tm_id = check_title(self, tm_title)

        if tm_id:
            print tm_title
        else:
            self.cursor.execute(TM_MAX_ID_QUERY)
            pl_data = self.cursor.fetchall()
            max_id, max_gid = pl_data[0]
            next_id = max_id + 1
            next_gid = 'GR' + str(int(max_gid.replace('GR', '').\
            replace('GR', '')) + 1)
            values = (next_id, next_gid, tm_title, '98', '', '', '2578', 'Team', '', '100', tm_img)
            self.cursor.execute(GR_QRY, values)
