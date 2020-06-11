import re
from scrapy.selector import Selector
from vtvspider import VTVSpider, \
extract_data, extract_list_data, get_nodes
from scrapy.http import Request
import MySQLdb

INSERT_AWARD_RESULTS = 'INSERT INTO sports_awards_results (award_id, category_id, genre, location, season, result_type, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'
INSERT_AWARD = 'INSERT INTO sports_awards (id, award_gid, award_title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE award_title = %s'
INSERT_AWARD_CATEGORY = 'INSERT INTO sports_awards_category (id, title_gid, title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE title = %s'
INSERT_AWARD_HIS = 'INSERT INTO  sports_awards_history (award_id, award_gid, category_id, category_gid, genre, season, location, winner_nominee, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'

def mysql_connection():
    connection = MySQLdb.connect(host = '10.4.18.183', user = 'root', db = 'SPORTSDB')
    cursor = connection.cursor()
    return connection, cursor
def mysql_conn():
    conne = MySQLdb.connect(host = '10.4.2.187', user = 'root', db = 'AWARDS')
    cursor = conne.cursor()
    return conne, cursor

GENER_DICT = {'running': 'G157', 'auto racing': 'G73', \
                'tennis': 'G55', 'swimming': 'G176', \
                'cricket': 'G96', 'rugby union': 'SG1321', \
                'athletics': 'G258', 'surfing': 'G175', 'golf': 'G29', \
                'snowboard': 'G167', 'basketball': 'G13', \
                'cycling': 'G80', 'skiing': 'G165', \
                'motorcycle racing': 'G133', \
                'baseball': 'G12',  \
                'skateboarding': 'G162', \
                'soccer': 'G51', 'wheelchair racing': ''}

class LaureusWinners(VTVSpider):

    name = "laureus_winners"
    allowed_domains = ["china.laureus.com"]
    start_urls = ["http://china.laureus.com/2015-winners/#tab-container"]

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
        else:
            if "Laureus" in award_cat:
                award_cat = award_cat
            else:
                award_cat = "Laureus World Sports Award for " +award_cat
            query = 'select max(id) from sports_awards_category'
            cursor.execute(query)
            ids = cursor.fetchone()
            if ids:
                awc_id = ids[0]
                awc_id = awc_id + 1
                awc_gid = "AWARD" + str(awc_id)
            cursor.execute(INSERT_AWARD_CATEGORY, (awc_id, awc_gid, award_cat, '',  award_cat))
            return awc_id, awc_gid
        return awc_id
        connection.close()

    def get_player_id(self, player_name):
        connection, cursor = mysql_connection()
        query = 'select gid, game from sports_participants where  title = %s'
        values = (player_name)
        cursor.execute(query, values)
        pid_ = cursor.fetchall()
        if pid_:
            pid  = str(pid_[0][0])
            game = str(pid_[0][1])
            return pid, game
        connection.close()
    def get_cat_details(self, award_cat):
        conne, cursor = mysql_conn()
        query = 'select id, category_gid from award_ceremony_categories where category_title= %s'
        values = (award_cat)
        cursor.execute(query, values)
        id_ = cursor.fetchall()
        return id_
        conne.close()


    def populate_sports_awards(self, player_name, award_cat, award_title, venue, season, result_type):
        aw_id = self.get_award_id(award_title)
        awc_id = self.get_award_cat_id(award_cat)
        pid, game = self.get_player_id(player_name)

        if aw_id and pid and awc_id:
            for key, value in GENER_DICT.iteritems():
                if game not in key:
                    continue
                game = game.lower()
                genre_gid = value
                genre = game+"{"+genre_gid+"}"
            if '{}' in genre:
                genre = ''
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
    def populate_sports_history(self, player_name, award_cat, award_title, venue, season, result_type):
        pid, game = self.get_player_id(player_name)
        id_ = self.get_cat_details(award_cat)
        ids =  self.get_award_details(award_title)
        if pid and ids and id_:
            for key, value in GENER_DICT.iteritems():
                if game not in key:
                    continue
                game = game.lower()
                genre_gid = value
                genre = game+"{"+genre_gid+"}"
            if '{}' in genre:
                genre = ''

            conne, cursor = mysql_conn()
            cursor.execute(INSERT_AWARD_HIS % (str(ids[0][0]), str(ids[0][1]), str(id_[0][0]), str(id_[0][1]), genre, season, str(venue),result_type, pid, pid))
            conne.close()

    def parse(self, response):
        sel = Selector(response)
        nodes = get_nodes(sel, '//div[@class="col-lg-4 col-md-4 col-sm-4"]')
        for node in nodes:
            award       = extract_data(node, './/h3[@class="category-title"]//text()')
            player_name = extract_data(node, './/h3[2]//text()').replace("Men's", 'National'). \
            replace('\r\n', '').strip().encode('utf-8').replace('\u010', ''). \
            replace('\u0107', '').replace('\xc4\x87', '').replace('  ', ' '). \
            replace('\xc3\xb8', '')
            if player_name in ['Skateistan', 'Alan Eustace']:
                continue
            award_cat = "Laureus World Sports Award for " +award
            if "Academy Exceptional Achievement" in award_cat:
                award_cat = "Laureus Academy Exceptional Achievement Award"
            result_type = "winner"
            award_title = "Laureus World Sports Awards"
            venue       = "Shanghai"
            season      = "2015"
            self.populate_sports_awards(player_name, award_cat, award_title, venue, season, result_type)
            self.populate_sports_history(player_name, award_cat, award_title, venue, season, result_type)


