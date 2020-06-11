from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import MySQLdb
import re

stadium_dict = {'Stadium:mk': 'Stadium mk',
                'Oakwell': 'Oakwell stadium',
                'Spotland': 'Spotland stadium'}

class WikiFacup(VTVSpider):
    name = 'wiki_facup'
    start_urls = []

    domain_url = 'https://en.wikipedia.org/?curid=%s'
    facup_teams = []
    wiki_gids   = {}

    def mysql_connection(self):
        conn = MySQLdb.connect(host='10.4.18.34', db='SPORTSDB_BKP', user='root', charset='utf8', use_unicode=True)
        cursor = conn.cursor()
        return conn, cursor

    def merge_conn(self):
        conn = MySQLdb.connect(host='10.4.2.187', db='GUIDMERGE', user='root', charset='utf8', use_unicode=True)
        cursor = conn.cursor()
        return conn, cursor


    def get_facup_teams(self):
        self.conn, self.cursor = self.mysql_connection()
        query = 'select participant_id from sports_tournaments_participants where tournament_id in (213) and participant_id in (select participant_id from sports_teams where stadium_id=0) '
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            team_id = str(row[0])
            query = 'select gid from sports_participants where id=%s'
            self.cursor.execute(query, team_id)
            data = self.cursor.fetchone()
            if data:
                data = data[0]
                self.facup_teams.append([data, team_id])

    def get_wiki_gid(self):
        for row in open('sports_to_wiki_guid_merge.list', 'r'):
            if 'TEAM' in row.strip():
                wiki_gid, sports_gid = row.strip().split('<>')
                self.wiki_gids[sports_gid] = wiki_gid

    def start_requests(self):
        self.get_facup_teams()
        self.get_wiki_gid()

        for row in self.facup_teams:
            team_gid, team_id = row
            if self.wiki_gids.get(team_gid, ''):
                wiki_gid = self.wiki_gids[team_gid].replace('WIKI', '')
                req_link = self.domain_url % wiki_gid
                yield Request(req_link, self.parse, meta={'team_id': team_id})

    def parse(self, response):
        hxs = Selector(response)
        team_id = response.meta['team_id']
        founded = extract_data(hxs, '//th[contains(text(), "Founded")]/following-sibling::td[1]/text()').replace(u'\xa0', ' ').replace(',','').strip()
        if not founded:
            founded = extract_data(hxs, '//th[contains(text(), "Founded")]/following-sibling::td[1]//text()')
        ground = extract_list_data(hxs, '//th[contains(text(), "Ground")]/following-sibling::td[1]//text()')
        if not ground:
            ground = extract_list_data(hxs, '//th[contains(text(), "Arena")]/following-sibling::td[1]//text()')
        '''if founded:
            self.update_founded(founded, team_id)'''
        if ground:
            self.update_ground(ground[0], team_id)

    def update_ground(self, stadium, team_id):
        self.conn, self.cursor = self.mysql_connection()
        if ',' in stadium:
            stadium = stadium.split(',')[0].strip()
        if '(' in stadium:
            stadium = stadium.split('(')[0].strip()
        query = 'select id from sports_stadiums where title like %s'
        std = '%' + stadium + '%'
        self.cursor.execute(query, std)
        data = self.cursor.fetchone()
        if data:
            data = data[0]
            self.update_id(data, team_id)
        else:
            query = 'select id from sports_stadiums where aka like %s'
            name = '%' + stadium + '%'
            self.cursor.execute(query, name)
            data = self.cursor.fetchone()
            if data:
                data = data[0]
                self.update_id(data, team_id)

            else:
                print stadium
        if 'Starom' in stadium:
            loc_id= ''
            self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_stadiums' and TABLE_SCHEMA='SPORTSDB_BKP'")
            count = self.cursor.fetchone()
            stadium_gid = 'STAD%s' % str(count[0])

            query = 'insert into sports_stadiums (id, gid, title, location_id, created_at, modified_at) values(%s, %s, %s, %s, now(), now())'
            values = (count[0], stadium_gid, stadium, loc_id)
            self.cursor.execute(query, values)
            self.update_id(count[0], team_id)


    def update_id(self, stadium_id, team_id):
        query = 'update sports_teams set stadium_id=%s where participant_id=%s'
        values = (stadium_id, team_id)
        self.cursor.execute(query, values)

    def update_founded(self, founded, team_id):
        if '(' in founded:
            founded = founded.split('(')[0].strip()
        if not founded:
            return
        formed = ''
        if founded.endswith(' as'):
            founded = founded.replace('as', '').strip()
        elif ' as ' in founded:
            founded = founded.split(' as ')[0].strip()
        elif ';' in founded:
            founded = founded.split(';')[0].strip()
        if re.findall('\w+ ([0-9]{4})', founded):
            founded = re.findall('\w+ ([0-9]{4})', founded)[0]
        if len(founded) == 4:
            formed = '%s-01-01' % founded
        else:
            try:
                formed = str(datetime.datetime.strptime(founded, '%d %B %Y'))
            except:
                formed = ''
            if not formed:
                try:
                    formed = str(datetime.datetime.strptime(founded, '%Y %B %d'))
                except:
                    formed = ''
        if formed:
            query = 'update sports_teams set formed=%s where participant_id=%s'
            values = (formed, team_id)
            self.cursor.execute(query, values)
        else:
            print founded
