from scrapy.http import Request
import re
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, extract_data, \
             get_nodes, extract_list_data
import MySQLdb



class NFLDraft(VTVSpider):
    name = "nfl_draft"
    allowed_domains = []
    start_urls = ['https://en.wikipedia.org/wiki/2015_NFL_draft']

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
        self.cursor = self.conn.cursor()
        self.merge_dict = {}
        self.get_merge()


    def get_merge(self):
        merges = open('sports_to_wiki_guid_merge.list', 'r').readlines()
        for merge in merges:
            if 'PL' in merge.strip():
                wiki_gid, pl_gid = merge.strip().split('<>')
                self.merge_dict[wiki_gid] = pl_gid

    def get_pl_ids(self, pls):
        pl_ids = []
        pl = 'WIKI' + pls
        if self.merge_dict.get(pl, ''):
            pl_gid = self.merge_dict[pl]
        else:
            pl_gid = ''
        if pl_gid:
            query = 'select id from sports_participants where gid=%s'
            self.cursor.execute(query, pl_gid)
            data = self.cursor.fetchone()
            pl_id = data[0]
            pl_ids.append(pl_id)

        return pl_ids



    def parse(self, response):
        sel = Selector(response)
        pl_nodes = get_nodes(sel, '//table[@class="wikitable sortable"]//tr')
        count = 0
        for node in pl_nodes:
            pl_title = extract_data(node, './/td//span[@class="fn"]/a/text()')
            pl_link = extract_data(node, './/td//span[@class="fn"]/a/@href')
            if not pl_title:
                continue
            if "http" not in pl_link:
                pl_link = "https://en.wikipedia.org" +pl_link
            count += 1
            round_ = extract_data(node, './/th[1]//text()')
            pick   = extract_data(node, './/th[2]//text()')
            if pl_link and round_ == '1':
                yield Request(pl_link, callback=self.parse_next, meta = {'round_': round_, 'pick': pick, 'rank': count})


    def parse_next(self, response):
        sel = Selector(response)
        rank = response.meta['rank']
        pick = response.meta['pick']
        round_ = response.meta['round_']
        wiki_gid = ''.join(re.findall('"wgArticleId":\d+', response.body)). \
        strip('"').replace('wgArticleId":', '' ).strip('"').strip()
        print wiki_gid
        pl_id = self.get_pl_ids(wiki_gid)
        if pl_id:
            data = {'pick': pick, 'rank': rank, 'round': round_}
            self.populate_draft(pl_id, data)
        else:
            print response.url



    def populate_draft(self, pl_id, data):
        query = 'insert ignore into sports_tournaments_results (tournament_id, participant_id, season, result_type, result_sub_type, result_value, modified_at) values ("%s", "%s", "%s", "%s", "%s", "%s", now())'
        for key in data.keys():
            res_value = data[key]
            values = ('2985', pl_id[0], '2015', 'draft', key, res_value)
            self.cursor.execute(query % values) 

