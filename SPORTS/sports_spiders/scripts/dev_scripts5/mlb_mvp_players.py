from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import MySQLdb


INSERT_AWARD_RESULTS = 'INSERT INTO sports_awards_results (award_id, category_id, genre, location, season, result_type, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'
INSERT_AWARD = 'INSERT INTO sports_awards (id, award_gid, award_title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE award_title= %s'
INSERT_AWARD_CATEGORY = 'INSERT INTO sports_awards_category (id, title_gid, title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE title = %s'
INSERT_AWARD_HIS = 'INSERT INTO  sports_awards_history (award_id, award_gid, category_id, category_gid, genre, season, location, winner_nominee, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'

GET_ENTITY_ID = 'select entity_id from sports_source_keys where \
source="MLB" and entity_type="participant" and source_key="%s"'

def mysql_conn():
    conne = MySQLdb.connect(host = '10.4.2.187', user = 'root', db = 'AWARDS')
    cursor = conne.cursor()
    return conne, cursor


def mysql_connection():
    conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = conn.cursor()
    return conn, cursor


class MLBMVPSpider(VTVSpider):
    name = "mvp_awards"
    start_urls = []

    def start_requests(self):
        urls = ['http://m.mlb.com/awards/history-winners/?award_id=ALMVP',
                'http://m.mlb.com/awards/history-winners/?award_id=NLMVP']
        for url in urls:
            yield Request(url, callback=self.parse)
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

    def get_award_cat_id(self, award_cat, genre):
        connection, cursor = mysql_connection()
        query = 'select id from sports_awards_category where title = %s'
        values = (award_cat)
        cursor.execute(query, values)
        ids = cursor.fetchone()
        if ids:
            awc_id = str(ids[0])
        else:
            query = 'select max(id) from sports_awards_category'
            cursor.execute(query)
            ids = cursor.fetchone()
            if ids:
                awc_id = ids[0]
                awc_id = awc_id + 1
                awc_gid = "AWARD" + str(awc_id)
            cursor.execute(INSERT_AWARD_CATEGORY, (awc_id, awc_gid, award_cat, genre, award_cat))
            return awc_id, awc_gid
        return awc_id
    def get_player_id(self, player_name, game, player_sk):
        connection, cursor = mysql_connection()
        cursor.execute(GET_ENTITY_ID %(player_sk))
        pid_ = cursor.fetchone()
        if pid_:
            pid_ = str(pid_[0])
            query = 'select gid from sports_participants where  title = %s and game = %s and id = %s'
            values = (player_name, game, pid_)
            cursor.execute(query, values)
            pid = cursor.fetchone()
        else:
            query = 'select gid from sports_participants where  title = %s and game = %s'
            values = (player_name, game)
            cursor.execute(query, values)
            pid = cursor.fetchone()
        if pid:
            pid = str(pid[0])
        return pid
        connection.close()
    def populate_sports_awards(self, player_name, award_cat, award_title, venue, season, result_type, genre, game, player_sk):
        aw_id = self.get_award_id(award_title, genre)
        awc_id = self.get_award_cat_id(award_cat, genre)
        pid = self.get_player_id(player_name, game, player_sk)

        if aw_id and pid and awc_id:
            connection, cursor = mysql_connection()
            cursor.execute(INSERT_AWARD_RESULTS % (aw_id, awc_id, genre, str(venue), season, result_type, pid, pid))
            connection.close()
    def get_award_details(self, award_title):
        conne, cursor = mysql_conn()
        query = 'select id, award_gid from award_ceremonies where award_title = %s'
        values = (award_title)
        cursor.execute(query, values)
        ids = cursor.fetchall()
        return ids
        conne.close()
    def get_cat_details(self, award_cat):
        conne, cursor = mysql_conn()
        query = 'select id, category_gid from award_ceremony_categories where category_title= %s'
        values = (award_cat)
        cursor.execute(query, values)
        id_ = cursor.fetchall()
        return id_
        conne.close()

    def populate_sports_history(self, player_name, award_cat, award_title, venue, season, result_type, genre, game, player_sk):
        pid = self.get_player_id(player_name, game, player_sk)
        id_ = self.get_cat_details(award_cat)
        ids =  self.get_award_details(award_title)
        if pid and ids and id_:
            conne, cursor = mysql_conn()
            cursor.execute(INSERT_AWARD_HIS % (str(ids[0][0]), str(ids[0][1]), str(id_[0][0]), str(id_[0][1]), genre, season, str(venue),result_type, pid, pid))
            conne.close()

    def parse(self, response):
        hxs = Selector(response)
        league = extract_data(hxs, './/li[@class="active"]/a/text()')
        pl_nodes = get_nodes(hxs, './/table[contains(@class, "data awards-list")]/tbody/tr')

        for pl_node in pl_nodes[:1]:
            year = extract_data(pl_node, './td[1]/text()')
            print year
            player_url = extract_data(pl_node, './td/a/@href')
            player_sk = player_url.split('/')[-1]
            player_name = extract_data(pl_node, './td/a/text()')
            if "Ken Griffey Jr." in player_name:
                player_name = "Ken Griffey"
            award_cat = league + " MVP"
            award_title = "Major League Baseball Most Valuable Player Award"
            venue = "USA"
            season = year
            result_type = "winner"
            game = "baseball"
            genre = "baseball{G12}"
            self.populate_sports_awards(player_name, award_cat, award_title, venue, season, result_type, genre, game, player_sk)
            self.populate_sports_history(player_name, award_cat, award_title, venue, season, result_type, genre, game, player_sk)

