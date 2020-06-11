import time
import datetime
from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_list_data, extract_data
import MySQLdb

INSERT_AWARD_RESULTS = 'INSERT INTO sports_awards_results (award_id, category_id, genre, location, season, result_type, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'
INSERT_AWARD = 'INSERT INTO sports_awards (id, award_gid, award_title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE award_title = %s'
INSERT_AWARD_HIS = 'INSERT INTO  sports_awards_history (award_id, award_gid, category_id, category_gid, genre, season, location, winner_nominee, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'

def mysql_connection():
    connection = MySQLdb.connect(host = '10.4.18.183', user = 'root', db = 'SPORTSDB')
    cursor = connection.cursor()
    return connection, cursor
def mysql_conn():
    conne = MySQLdb.connect(host = '10.4.2.187', user = 'root', db = 'AWARDS')
    cursor = conne.cursor()
    return conne, cursor

class HeismanTrophySpider(VTVSpider):
    name = 'heisman_trophy_spider'
    start_urls = ['http://espn.go.com/college-football/awards/_/id/9']

    def get_award_id(self, award_title, genre):
        connection, cursor = mysql_connection()
        query = 'select id from sports_awards where award_title = %s'
        values = (award_title)
        cursor.execute(query, values)
        ids = cursor.fetchone()
        if ids:
            aw_id =  str(ids[0])
        else:
            query = 'select max(id) from sports_awards'
            cursor.execute(query)
            ids = cursor.fetchone()
            if ids:
                aw_id = ids[0]
                aw_id = aw_id + 1
                aw_gid = "AWARD" + str(aw_id)
            cursor.execute(INSERT_AWARD, (aw_id, aw_gid, award_title, genre, award_title))
            return aw_id, aw_gid
        return aw_id
        connection.close()

    def get_player_id(self, player_name, game):
        connection, cursor = mysql_connection()
        query = 'select gid from sports_participants where  title = %s and game= %s'
        values = (player_name, game)
        cursor.execute(query, values)
        pid = cursor.fetchone()
        connection.close()
        if pid:
            pid = str(pid[0])
        return pid
    def get_award_details(self, award_title):
        conne, cursor = mysql_conn()
        query = 'select id, award_gid from award_ceremonies where award_title = %s'
        values = (award_title)
        cursor.execute(query, values)
        ids = cursor.fetchall()
        return ids
        conne.close()


    def populate_sports_awards(self, player_name, award_cat, award_title, venue, season, result_type, genre, game):
        aw_id = self.get_award_id(award_title, genre)
        pid = self.get_player_id(player_name, game)
        if aw_id and pid:
            connection, cursor = mysql_connection()
            cursor.execute(INSERT_AWARD_RESULTS % (aw_id, award_cat, genre, str(venue), season, result_type, pid, pid))
            connection.close()
    def populate_sports_history(self, player_name, award_cat, award_title, venue, season, result_type, genre, game):
        pid = self.get_player_id(player_name, game)
        ids =  self.get_award_details(award_title)
        if pid and ids:
            conne, cursor = mysql_conn()
            cursor.execute(INSERT_AWARD_HIS % (str(ids[0][0]), str(ids[0][1]), "", "", genre, season, str(venue),result_type, pid, pid))
            conne.close()

    def parse(self, response):
        hxs = Selector(response)
        record = {}
        now = datetime.datetime.now()
        tou_name = "Heisman Trophy"
        game ="football"
        nodes = get_nodes(hxs, '//div[@class="mod-content"]//table[@class="tablehead"]//tr[contains(@class, "row")]')
        for node in nodes:
            year  = extract_data(node, './/td[1]//text()')
            player_name = extract_data(node, './/td[2]//text()')
            venue = ""
            season= year
            award_title = 'Heisman Trophy'
            award_cat   = '0'
            result_type = 'winner'
            genre = "football{G27}"
            self.populate_sports_awards(player_name, award_cat, award_title, venue, season, result_type, genre, game)
            self.populate_sports_history(player_name, award_cat, award_title, venue, season, result_type, genre, game)
