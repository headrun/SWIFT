import os
import re
import MySQLdb
from scrapy.http import Request
from scrapy.selector import Selector
from vtvspider import VTVSpider, \
extract_data, get_nodes, extract_list_data

INSERT_AWARD = 'INSERT INTO sports_awards (id, award_gid, award_title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE award_title = %s'

INSERT_AWARD_CATEGORY = 'INSERT INTO sports_awards_category (id, title_gid, title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE title = %s'

INSERT_AWARD_RESULTS = 'INSERT INTO sports_awards_results (award_id, category_id, genre, season, location, result_type, participants, created_at, modified_at) VALUES (%s, %s, %s, %s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE participants= %s'

def mysql_connection():
    connection = MySQLdb.connect(host = '10.4.18.183', user = 'root', db = 'SPORTSDB')
    cursor = connection.cursor()
    return connection, cursor

class FifaAwards(VTVSpider):
    name = 'fifa_awards'
    start_urls= ['http://www.fifa.com/worldcup/awards/index.html']

    def parse(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//ul[@class="qlink-list"]')
        for node in nodes:
            award_link = extract_data(node, './/a[contains(@href, "awards")]/@href')
            if award_link and not "http" in award_link:
                award_link = 'http://www.fifa.com' + award_link
                yield Request(award_link, callback=self.parse_award_details, meta = {})

    def get_award_id(self, award_title):
        connection, cursor = mysql_connection()
        query = 'select id from sports_awards where award_title = %s'
        values = (award_title)
        cursor.execute(query, values)
        ids = cursor.fetchone()
        if ids:
            aw_id = ids[0]
        else:
            query = 'select max(id) from sports_awards'
            cursor.execute(query)
            ids = cursor.fetchone()
            if ids:
                aw_id = ids[0]
                aw_id = aw_id + 1
                aw_gid = "AWARD" + str(aw_id)
            cursor.execute(INSERT_AWARD, (aw_id, aw_gid, award_title, "soccer{G51}", award_title))
            return aw_id, aw_gid
        return aw_id
        connection.close()
    def get_award_cat_id(self, award_cat):
        connection, cursor = mysql_connection()
        query = 'select id from sports_awards_category where title = %s'
        values = (award_cat)
        cursor.execute(query, values)
        ids = cursor.fetchone()
        if ids:
            awc_id = ids[0]
        else:
            query = 'select max(id) from sports_awards_category'
            cursor.execute(query)
            ids = cursor.fetchone()
            if ids:
                awc_id = ids[0]
                awc_id = awc_id + 1
                awc_gid = "AWARD" + str(awc_id)
            cursor.execute(INSERT_AWARD_CATEGORY, (awc_id, awc_gid, award_cat, "soccer{G51}",  award_cat))
            return awc_id, awc_gid
        return awc_id
        connection.close()


    def get_player_id(self, player_id, player_name):
        connection, cursor = mysql_connection()
        query = 'select gid from sports_participants where  title = %s and game="soccer"'
        values = (player_name)
        cursor.execute(query, values)
        pid = cursor.fetchone()
        connection.close()
        if pid:
            pid = str(pid[0])
        return pid

    def populate_sports_awards(self, player_id, player_name, player_team, award_cat, award_title, location, year):
        aw_id = self.get_award_id(award_title)
        awc_id = self.get_award_cat_id(award_cat)
        pid = self.get_player_id(player_id, player_name)

        if aw_id and awc_id and pid:
            connection, cursor = mysql_connection()
            cursor.execute(INSERT_AWARD_RESULTS, (aw_id, awc_id, "soccer{G51}", year, location, "winner", pid, pid))
            connection.close()

    def parse_award_details(self, response):
        sel = Selector(response)
        year = extract_data(sel, '//div[@class="title-wrap"]//h1//a/text()').split(' ')[0].strip()
        nodes = get_nodes(sel, '//div[@class="players awards podium top "]//div[@class="p detail p-i-prt-2"]')
        for node in nodes:
            player_id = extract_data(node, './@data-playerid')
            player_name = extract_data(node, './/li[@class="p-name"]//text()')
            player_team = extract_data(node, './/li[@class="p-team"]//span[@class="t-nTri"]/text()')
            location = extract_data(node, './/li[@class="p-team"]//span[@class="t-nText "]/text()')
            award_cat = "Adidas " + re.findall(r'worldcup/awards/(.*)/index', response.url)[0].replace('-', ' ').title()
            award_title = 'FIFA World Cup Awards'

            self.populate_sports_awards(player_id, player_name, player_team, award_cat, award_title, location, year)
