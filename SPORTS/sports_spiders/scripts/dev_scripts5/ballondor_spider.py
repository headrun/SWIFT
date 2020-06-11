from vtvspider_dev import VTVSpider, extract_data, get_nodes, extract_list_data
from scrapy.selector import Selector
from scrapy.http import Request
import MySQLdb
import re

AWARD_HISTORY_QUERY = "INSERT INTO sports_awards_history \
(award_id, award_gid, category_id, category_gid, genre, season, \
location, winner_nominee, participants, created_at, modified_at) \
VALUES (%s, %s, '', '', %s, %s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE participants= %s"

INSERT_AWARD_RESULTS = 'INSERT INTO sports_awards_results \
(award_id, category_id, genre, location, season, result_type, \
participants, created_at, modified_at) \
VALUES (%s, %s, %s, %s, %s, %s, %s, now(), now()) \
ON DUPLICATE KEY UPDATE participants = %s'
INSERT_AWARD = 'INSERT INTO sports_awards \
(id, award_gid, award_title, genre, created_at, modified_at) \
VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE award_title = %s'

def mysql_connection():
    connection = MySQLdb.connect(host = '10.4.18.183', user = 'root', db = 'SPORTSDB')
    cursor = connection.cursor()
    return connection, cursor

def awards_connection():
    conn = MySQLdb.connect(host = '10.4.2.187', user = 'root', db = 'AWARDS')
    cursor = conn.cursor()
    return conn, cursor


class BallonCrawler(VTVSpider):
    name = "fifa_ballon"
    allowed_domains = ["www.fifa.com"]
    start_urls = ["http://www.fifa.com/ballondor/playeroftheyear/index.html",
                  "http://www.fifa.com/ballon-dor/player-of-the-year/women.html",
                  "http://www.fifa.com/ballon-dor/history/index.html"]
    start_urls = ['http://www.fifa.com/ballondor/playeroftheyear/index.html']
    def get_award_id(self, award_title):
        connection, cursor = mysql_connection()
        query = 'select id from sports_awards where award_title = %s'
        values = (award_title)
        cursor.execute(query, values)
        ids = cursor.fetchone()
        if ids:
            aw_id =  ids[0]
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

    def get_player_id(self, player_name):
        connection, cursor = mysql_connection()
        query = 'select gid from sports_participants where  title = %s and game= "soccer"'
        values = (player_name)
        cursor.execute(query, values)
        pid = cursor.fetchone()
        connection.close()
        if pid:
            pid = str(pid[0])
        else:
            print player_name
        return pid

    def get_award_history_id(self, award_title):
        conn, cursor = awards_connection()
        query = 'select id, award_gid from award_ceremonies where award_title= %s'
        cursor.execute(query, (award_title))
        data = cursor.fetchall()
        award_id, award_gid = data[0]
        conn.close()
        return award_id, award_gid


    def populate_awards_history(self, player_name, award_title, city, year, result_type):
        aw_id, aw_gid = self.get_award_history_id(award_title)
        genre = "soccer{G51}"
        pid = self.get_player_id(player_name)
        if aw_id and aw_gid and pid:
            conn, cursor = awards_connection()
            values = (str(aw_id), aw_gid, genre, year, city, result_type, pid, pid)
            cursor.execute(AWARD_HISTORY_QUERY, values)
            conn.close()


    def populate_sports_awards(self, player_name, player_team, award_title, city, year, result_type):
        aw_id = self.get_award_id(award_title)
        awc_id = ""
        genre = "soccer{G51}"
        pid = self.get_player_id(player_name)

        if aw_id and pid:
            connection, cursor = mysql_connection()
            cursor.execute(INSERT_AWARD_RESULTS, (aw_id, awc_id, genre, str(city), year, result_type, pid, pid))
            connection.close()


    def parse(self, response):
        winners = []
        data = {}
        hxs = Selector(response)
        nodes = get_nodes(hxs, './/div[contains(@class, "module anchor")]//div[contains(@class, "inner")]')
        if not nodes:
            nodes = get_nodes(hxs, '//div[@class="bo-edition-award bo-poy"]')
        if not nodes:
            nodes = get_nodes(hxs, '//div[@class="players awards bo-candidate"]')[::1]


        for node in nodes:
            year = extract_data(node, './/h2/text()')
            players = extract_list_data(node, './/div[@class="bo-edition-award bo-poy"]//a[@class="bo-edition-wname"]/text()')
            players = extract_list_data(node, './/@data-playername')
            player_ids = extract_list_data(node, './/div[@class="bo-edition-award bo-poy"]//a[@class="bo-edition-wname"]/@href')
            teams = extract_list_data(node, './/div[@class="bo-edition-award bo-poy"]//div[@class="bo-edition-t"]//text()')
            award_title = extract_data(node, './/div[@class="bo-edition-award bo-poy"]//h4/text()')
            if not year and "edition" in response.url:
                year_ = response.url.split('=')[-1]
                year = re.findall('\d+', year_)[0]
                players = extract_list_data(node, './/a[@class="bo-edition-wname"]/text()')  or \
                            extract_list_data(node, './/span[@class="bo-edition-wname"]/text()')
                players_ = extract_list_data(node, './/span[@class="bo-edition-wname"]/text()')
                if players_ and len(players) == 1:
                    players.append(players_[0])
                player_ids = extract_list_data(node, './/a[@class="bo-edition-wname"]/@href') or \
                            extract_list_data(node, './/span[@class="bo-edition-wname"]/@href')
                teams = extract_list_data(node, './/div[@class="bo-edition-t"]//text()')
                award_title = extract_data(node, './/h4/text()')
            if 'ballon-dor/player-of-the-year' in response.url:
                award_title = extract_data(hxs, '//h1/span/text()')
            year_link = extract_data(node, './/@data-url')
            if year_link:
                year_link = "http://www.fifa.com" + year_link
                yield Request(year_link, callback=self.parse)
                continue
            venue = extract_data(hxs, '//div[@class="title-wrap"]//h1/a/span/text()').split(',')
            if year in venue:
                city = venue[-1].strip()
            else:
                city = ""
            if "women" in response.url:
                award_title = "FIFA World Player of the Year"
            else:
                award_title = award_title
            if players and year:
                year = int(year) + 1
            for ind, player in enumerate(players):
                if "(" in player:
                    player = player.split('(')[0]
                if ind == 1:
                    award_title = "FIFA World Player of the Year"
                result_type = "nominee"
                self.populate_sports_awards(player, teams, award_title, city, year, result_type)
                self.populate_awards_history(player, award_title, city, year, result_type)

            other_finalists = get_nodes(hxs, '//div[@class="col-xs-12 clear-grid "]//div[@class="players awards bo-candidate"]//div[@class="p detail p-i-prt-2"]')
            for node in other_finalists:
                player_id = extract_data(node, './@data-playerid').strip()
                player_name = extract_data(node, './/ul//li[@class="p-name"]//h3/text()').strip()
                player_team = extract_data(node, './/ul//li[@class="p-team"]//span/text()').strip()
                result_type = "nominee"
                self.populate_sports_awards(player_id, player_name, player_team, award_title, city, year, result_type)
