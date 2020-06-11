import re
import time
import datetime
from scrapy.selector import Selector
from vtvspider import VTVSpider, \
extract_data, extract_list_data, get_nodes
from scrapy.http import Request
import MySQLdb

CAT_DICT= {'AP Most Valuable Player': 'National Football League Most Valuable Player Award',
'AP Offensive Player of the Year presented by Surface': 'AP NFL Offensive Player of the Year Award',
'AP Comeback Player of the Year': 'AP Comeback Player of the Year',
'AP Offensive Rookie of the Year': 'AP Offensive Rookie of the Year',
'AP Defensive Player of the Year': 'AP NFL Defensive Player of the Year Award',
'AP Defensive Rookie of the Year': 'AP Defensive Rookie of the Year',
'Walter Payton NFL Man of the Year presented by Nationwide': 'Walter Payton NFL Man of the Year Award',
'NFL.com Fantasy Player of the Year presented by SAP': 'Fantasy Player of the Year',
'FedEx Air & Ground Players of the Year': 'FedEx Air Player of the Year',
'Deacon Jones Award': 'Deacon Jones Player of the Year',
'"Greatness on the Road" Award presented by Courtyard': 'Greatness on the Road',
'Bridgestone Performance Play of the Year': 'NFL Play of the Year Award', \
'Art Rooney Award presented by Bose': 'Art Rooney Award', \
'Salute To Service Award Presented by USAA': 'Salute to Service Award' }
CATT_DICT= {'AP Most Valuable Player': 'National Football League Most Valuable Player Award',
    'AP Offensive Player of the Year': 'AP NFL Offensive Player of the Year Award',
    'AP Comeback Player of the Year': 'AP Comeback Player of the Year',
    'AP Offensive Rookie of the Year': 'AP Offensive Rookie of the Year',
    'AP Defensive Player of the Year': 'AP NFL Defensive Player of the Year Award',
    'AP Defensive Rookie of the Year': 'AP Defensive Rookie of the Year',
    'Walter Payton NFL Man of the Year': 'Walter Payton NFL Man of the Year Award',
    'Bridgestone Performance Play of the Year': 'NFL Play of the Year Award', \
    'Salute to Service Award presented by USAA': 'Salute to Service Award',
    'Greatness on the Road presented by Courtyard': 'Greatness on the Road',
    'GMC Never Say Never Moment of the Year': 'GMC Never Say Never Moment of the Year',
    'FedEx Air Player of the Year': 'FedEx Air Player of the Year',
    'FedEx Ground Player of the Year': 'FedEx Ground Player of the Year',
    'Fantasy Player of the Year': 'Fantasy Player of the Year',
    'Deacon Jones Player of the Year': 'Deacon Jones Player of the Year',
    'Pepsi Next Rookie of the Year': 'Pepsi NFL Rookie of the Week'}

INSERT_AWARD_RESULTS = 'INSERT INTO sports_awards_results (award_id, category_id, genre, location, season, result_type, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'
INSERT_AWARD = 'INSERT INTO sports_awards (id, award_gid, award_title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE award_title = %s'
INSERT_AWARD_CATEGORY = 'INSERT INTO sports_awards_category (id, title_gid, title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE title = %s'
INSERT_AWARD_HIS = 'INSERT INTO  sports_awards_history (award_id, award_gid, category_id, category_gid, genre, season, location, winner_nominee, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'

GET_ENTITY_ID = 'select entity_id from sports_source_keys where \
source="NFL" and entity_type="participant" and source_key="%s"'

def mysql_connection():
    connection = MySQLdb.connect(host = '10.4.15.132', user = 'root', db = 'SPORTSDB_BKP')
    cursor = connection.cursor()
    return connection, cursor
def mysql_conn():
    conne = MySQLdb.connect(host = '10.4.2.187', user = 'root', db = 'AWARDS')
    cursor = conne.cursor()
    return conne, cursor

class NFLAwards(VTVSpider):
    name = "nfl_awards"
    allowed_domains = ["www.nfl.com"]
    start_urls = ["http://www.nfl.com/news/story/0ap3000000466415/article/2015-nfl-honors-complete-list-of-winners"]
    #start_urls = ["http://www.nfl.com/news/story/0ap2000000321591/article/nfl-honors-complete-list-of-winners"]
    def get_award_id(self, award_title):
        connection, cursor = mysql_connection()
        query = 'select id from sports_awards where award_title = %s'
        values = (award_title)
        cursor.execute(query, values)
        ids = cursor.fetchone()
        if ids:
            aw_id =  str(ids[0])
            return aw_id
        connection.close()

    def get_award_cat_id(self, award_cat):
        connection, cursor = mysql_connection()
        query = 'select id from sports_awards_category where title = %s'
        values = (award_cat)
        cursor.execute(query, values)
        ids = cursor.fetchone()
        if ids:
            awc_id = str(ids[0])
        return awc_id
        connection.close()
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
        aw_id = self.get_award_id(award_title)
        awc_id = self.get_award_cat_id(award_cat)
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
            cursor.execute(INSERT_AWARD_HIS % (str(ids[0][0]), str(ids[0][1]), str(id_[0][0]), str(id_[0][1]), genre, season,
            str(venue),result_type, pid, pid))
            conne.close()

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="article-location"]//div[@class="articleText"]//a[contains(@href, "/player/")]')
        award_title = "NFL Honors"
        for node in nodes[3:]:
            player_name = extract_data(node, './text()')
            player_sk = extract_data(node, './@href').split('/')[-2]
            if "Odell Beckham" in player_name:
                player_name ="Odell Beckham"
            cat =  extract_data(node, './../a[contains(@href, "http://www.nfl.com/news/story/")]/text()')
            if not cat:
                cat = extract_data(node, './..//text()').split(':')[0].strip().encode('utf-8').replace('\xc2\xbb', '').replace('\xc2\xa0', '').strip()
            if "Coach of the Year" in cat:
                continue
            if "Don Shula " in cat:
                continue
            if "Pro Football Hall of Fame" in cat:
                continue
            print cat
            game ="football"
            genre = "football{G27}"
            result_type = "winner"
            season = "2015"
            venue = "Phoenix"
            for key, value in CAT_DICT.iteritems():
                if cat in key:
                    award_cat = value
            if "FedEx Air Player of the Year" in award_cat and player_name == "Le'Veon Bell":
                award_cat = "FedEx Ground Player of the Year"
            self.populate_sports_awards(player_name, award_cat, award_title, venue, season, result_type, genre, game, player_sk)
            self.populate_sports_history(player_name, award_cat, award_title, venue, season, result_type, genre, game, player_sk)


