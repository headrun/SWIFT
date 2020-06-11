import re
from scrapy.selector import Selector
from scrapy.http import Request
import MySQLdb
from vtvspider import VTVSpider, get_nodes, extract_list_data, extract_data
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import codecs
conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset="utf8", use_unicode=True)
cur = conn.cursor()


class EspnStandings(VTVSpider):
    name = 'wikigids'
    start_urls = []
       
    def __init__(self):
        self.wiki_dict = {}
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def start_requests(self):
        team_lines = open('sports_team_merge_latest.txt', 'r').readlines()
        wiki_gids = []
        for line in team_lines:
            if 'In Old' not in line: continue
            k_link = line.split('New:')[-1].strip().split(' ')
            wiki_gid = k_link[0]
            team_gid = k_link[1]
            wiki_gids.append('%s ,%s'%(wiki_gid, team_gid))
        for wiki_gid in wiki_gids:
            wiki = wiki_gid.split(',')[0].strip().replace('WIKI','').strip()
            wiki_url = 'https://en.wikipedia.org/?curid=%s'%wiki
            yield Request(wiki_url, callback=self.parse, meta = {'w_gid' : wiki_gid.split(',')[0].strip(), 't_gid':wiki_gid.split(',')[-1].strip()})

    def parse(self, response):
        sel = Selector(response)
        wiki_title = ''.join(sel.xpath('//h1[@id="firstHeading"]/text()').extract()).strip()
        wiki_gid = response.meta['w_gid']
        team_gid = response.meta['t_gid']
        qry = 'select title, sport_id, participant_type from sports_participants where gid = %s'
        vals = (team_gid)
        cur.execute(qry, vals)
        row = cur.fetchone()
        team_title = row[0]
        sport = row[1]
        _type = row[2]
        if wiki_title == team_title: return
        if sport not in self.wiki_dict.keys():
            self.wiki_dict[sport] = [(wiki_gid, team_gid, wiki_title, team_title, _type)]
        else:
            self.wiki_dict[sport].append((wiki_gid, team_gid, wiki_title, team_title, _type))

    def spider_closed(self, spider):
        for i in self.wiki_dict.keys():
            f = codecs.open('sport_%s_%s.txt'%(str(i), str(len(self.wiki_dict[i]))), 'w', 'utf-8')
            for y in self.wiki_dict[i]:
                f.write('%s<>%s<>%s<>%s<>%s\n'%y)
            f.flush()

        
