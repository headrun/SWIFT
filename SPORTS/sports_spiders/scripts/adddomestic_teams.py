# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes, log
from scrapy.http import Request
from sports_spiders.items import SportsSetupItem
import MySQLdb
import re
DOMAIN_LINK = "https://en.wikipedia.org"

TM_MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'

PAT_QRY = "select id from sports_participants where title like %s and participant_type='team' and sport_id=7"


TM_SK_QRY = 'insert into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

SEL_SK_QRY = 'select id from sports_source_keys where source = "uefa_soccer" and entity_type = "participant" and source_key=%s'

PAR_QUERY = "insert into sports_participants (id, gid, title, aka, sport_id, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"


TEAMS_QRY = "insert into sports_teams(participant_id, short_title, callsign, category, keywords, tournament_id, division, gender, formed, timezone, logo_url, vtap_logo_url, youtube_channel, stadium_id, created_at, modified_at, display_title) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now(), %s) on duplicate key update modified_at=now()"


def add_team_sk(self, par_id, tm_sk):
    sel_values = (tm_sk)
    self.cursor.execute(SEL_SK_QRY, sel_values)
    data = self.cursor.fetchone()
    if not data:
        values = (par_id, 'participant', 'uefa_soccer', tm_sk)
        self.cursor.execute(TM_SK_QRY, values)


def check_title(self, title):
    title = (title)
    self.cursor.execute(PAT_QRY, title)
    data = self.cursor.fetchone()
    if data:
        id_ = str(data[0])
    else:
        id_ = ''
    return id_


class DomesticCricketTeamAddition(VTVSpider):
    name = "adddomestic_teams"
    start_urls = ["https://en.wikipedia.org/wiki/Baroda_cricket_team"]

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.216.45", user="veveo",
                                    passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True)
        self.conn_wiki = MySQLdb.connect(
            host="10.28.218.81", user="veveo", passwd="veveo123", db="GUIDMERGE", charset='utf8', use_unicode=True)
        self.wiki_cursor = self.conn_wiki.cursor()
        self.cursor = self.conn.cursor()

    def parse(self, response):
        sel = Selector(response)
        tm_title = extract_data(
            sel, '//h1[@id="firstHeading"]//text()').split('(')[0]
        location_id = ''
        stadium_id = ''
        short_title = extract_data(sel, '//caption[@class="fn org"]/text()')

        founded = extract_data(
            sel, '//th[contains(text(), "Founded")]/following-sibling::td[1]//text()').replace(u'\xa0', ' ').replace(',', '').strip()
        if not founded:
            founded = extract_data(
                sel, '//th[contains(text(), "First season")]/following-sibling::td[1]//text()')
        ground = extract_list_data(
            sel, '//th[contains(text(), "Ground")]/following-sibling::td[1]//a//text()')
        if not ground:
            ground = extract_list_data(
                sel, '//th[contains(text(), "Ground")]/following-sibling::td[1]//text()')
        if not ground:
            ground = extract_list_data(
                sel, '//th[contains(text(), "Stadium")]/following-sibling::td[1]//a//text()')
        if not ground:
            ground = extract_list_data(
                sel, '//th[contains(text(), "Stadium")]/following-sibling::td[1]//text()')
        if not ground:
            ground = extract_list_data(
                sel, '//th[contains(text(), "Home ground:")]/following-sibling::td[1]//text()')
        if len(founded) == 4:
            formed = '%s-01-01' % founded
        else:
            try:
                formed = str(datetime.datetime.strptime(founded, '%B %d, %Y'))
            except:
                formed = ''
        if not formed:
            try:
                formed = str(datetime.datetime.strptime(founded, '%B %d %Y'))
            except:
                formed = ''
        query = 'select id,location_id from sports_stadiums where title=%s'
        if ground:
            std = ground[0]
            self.cursor.execute(query, std)
            data = self.cursor.fetchone()
            if data:
                stadium_id = data[0]
                location_id = data[1]

            else:
                query = 'select id,location_id from sports_stadiums where title like %s'
            std = '%' + std + '%'
            self.cursor.execute(query, std)
            data = self.cursor.fetchone()
            if data:
                stadium_id = data[0]
                location_id = data[1]
        else:
            stadium_id = ''
            location_id = ''

        #tm_img = extract_data(sel, '//table[@class="infobox vcard"]//tr//td//img//@src')
        tm_img = extract_data(
            sel, '//table[@class="infobox vcard"]//tr//td/a/img/@src')
        if tm_img:
            tm_img = "https:" + tm_img
        else:
            tm_img = ''
        #country = response.meta['title']
        #short_title = response.meta['title']
        team_sk = response.url.split('/')[-1].strip().replace('_', '-').lower()
        call_sign = extract_data(sel, '//table//tr//td//span//b//text()')
        loc_query = 'select id from sports_locations where country=%s and state=%s'
        values = ('India', tm_title.replace('cricket team', '').strip())
        self.cursor.execute(loc_query, values)
        data = self.cursor.fetchone()
        loc_id = ''
        if data:
            loc_id = str(data[0])
        tm_id = check_title(self, tm_title)
        if tm_id:
            add_team_sk(self, tm_id, team_sk)
            print 'Added the Team source key'
        else:
            #game = "soccer"
            sport_id = '6'
            gender = "male"
            self.cursor.execute(TM_MAX_ID_QUERY)
            pl_data = self.cursor.fetchall()
            max_id, max_gid = pl_data[0]
            next_id = max_id + 1
            loc_id = ''
            next_gid = 'TEAM' + str(int(max_gid.replace('TEAM', '').
                                        replace('PL', '')) + 1)

            #sport_id_qry = 'select id from sports_types where title=%s'
            #value = (game)
            #self.cursor.execute(sport_id_qry, value)
            #data = self.cursor.fetchone()
            #sport_id = data[0]

            values = (next_id, next_gid, tm_title, '', sport_id, 'team', tm_img, '-150',
                      response.url, location_id)
            self.cursor.execute(PAR_QUERY, values)

            tm_values = (next_id, short_title, '', 'BCCI', '', '3296',
                         '', gender, formed, '', '', '', '', stadium_id, short_title)
            self.cursor.execute(TEAMS_QRY, tm_values)

            add_team_sk(self, next_id, team_sk)
            print 'Added the new team ->', tm_title

            wiki_gid = ''.join(re.findall('"wgArticleId":\d+', response.body)
                               ).strip('"').replace('wgArticleId":', '').strip('"').strip()
            GID_QRY = 'select child_gid from sports_wiki_merge where exposed_gid=%s'
            IN_QRY = 'insert into sports_wiki_merge(child_gid, exposed_gid, action, modified_date) values(%s, %s, "override", now()) on duplicate key update modified_date = now()'

            if wiki_gid and wiki_gid != "0":
                wiki_gid = "WIKI" + wiki_gid
                values = (wiki_gid)
                self.wiki_cursor.execute(GID_QRY, values)
                gid_data = self.wiki_cursor.fetchone()
                if not gid_data:
                    in_values = (next_gid, wiki_gid)
                    #self.wiki_cursor.execute(IN_QRY, in_values)
                else:
                    print gid_data
            self.conn_wiki.close()
            self.conn.close()
