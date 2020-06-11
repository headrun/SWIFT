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
    name = 'wiki_teams_titles'
    start_urls = []
       
    def __init__(self):
        self.wiki_dict = {}
        self.teams_matching_file = codecs.open('teams_matching_file.txt', 'wb+', 'utf8')
        self.teams_mismatching_file = codecs.open('teams_mismatching_file.txt', 'wb+', 'utf8')
        self.missing_team_gids = codecs.open('missing_team_gids.txt','wb+', 'utf8')
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def start_requests(self):
        qry = 'select guid_map from MERGE_TOOL.teams_merge_verification_entry where action = "reject"'
        cur.execute(qry)
        team_rows = cur.fetchall()
        wiki_gids = []
        for wiki_gid in team_rows:
            wiki = wiki_gid[0].split('<>')[0].strip().replace('WIKI','').strip()
            wiki_url = 'https://en.wikipedia.org/?curid=%s'%wiki
            yield Request(wiki_url, callback=self.parse, meta = {'w_gid' : wiki_gid[0].split('<>')[0].strip(), 't_gid':wiki_gid[0].split('<>')[-1].strip()})

    def parse(self, response):
        sel = Selector(response)
        wiki_title = ''.join(sel.xpath('//h1[@id="firstHeading"]/text()').extract()).strip()
        wiki_gid = response.meta['w_gid']
        team_gid = response.meta['t_gid']
        qry = 'select title, sport_id, participant_type from sports_participants where gid = "%s";'%team_gid
        cur.execute(qry)
        row = cur.fetchone()
        try:
            team_title = row[0]
        except:
            self.missing_team_gids.write('%s\n'%team_gid)
            return
        sport = row[1]
        _type = row[2]
        if wiki_title == team_title:
            self.teams_matching_file.write('%s<>%s<>Wiki : %s<>DB : %s<>%s\n'%(wiki_gid, team_gid, wiki_title, team_title, str(sport)))
        else:
            self.teams_mismatching_file.write('%s<>%s<>Wiki : %s<>DB : %s<>%s\n'%(wiki_gid, team_gid, wiki_title, team_title, str(sport)))

    def spider_closed(self, spider):
        for i in self.wiki_dict.keys():
            f = codecs.open('sport_%s_%s.txt'%(str(i), str(len(self.wiki_dict[i]))), 'w', 'utf-8')
            for y in self.wiki_dict[i]:
                f.write('%s<>%s<>%s<>%s<>%s\n'%y)
            f.flush()

        
