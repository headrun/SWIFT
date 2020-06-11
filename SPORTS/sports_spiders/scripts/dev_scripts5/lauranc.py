import re
import time
import datetime
from scrapy.selector import Selector
from vtvspider import VTVSpider, \
extract_data, extract_list_data, get_nodes
from scrapy.http import Request
import MySQLdb
GENER_DICT = {'Running': 'G157', 'Auto Racing': 'G73', \
                'Tennis': 'G55', 'Swimming': 'G176', \
                'Cricket': 'G96', 'Rugby': 'G156', 'Football': 'G27', \
                'Athletics': 'G258', 'Surfing': 'G175', 'Golf': 'G29', \
                'Snowboarding': 'G167', 'Basketball': 'G13', \
                'Cycling': 'G80', 'Skiing': 'G165', 'Motor Cycling': 'G133', \
                'Baseball': 'G12', 'Motor Racing': 'G73', \
                'auto racing' : "G73", 'Alpine Skiing': 'SG1315',
                'Skateboarding': 'G162', 'Soccer': 'G51', \
                'Sailing': 'G158', 'snowboard': 'G167', \
                'Boxing' : 'SG214', 'Wheelchair Racing': ''}

CAT_DICT = {'Sportsman': 'Laureus World Sports Award for Sportsman of the Year', \
'Sportswoman': 'Laureus World Sports Award for Sportswoman of the Year', \
'Team': 'Laureus World Sports Award for Team of the Year', \
 'Comeback' : 'Laureus World Sports Award for Comeback of the Year', \
 'Breakthrough': 'Laureus World Sports Award for Breakthrough of the Year', \
 'Action': 'Laureus World Sports Award for Action Sportsperson of the Year', \
 'Disabled': 'Laureus World Sports Award for Sportsperson of the Year with a Disability', \
'Lifetime Achievement': 'Laureus World Sports Award for Lifetime Achievement', \
'Newcomer': "Laureus World Sports Award for Newcomer", \
'Alternative': 'Laureus World Sports Award for Alternative', \
'Sport for Good': 'Laureus World Sports Award for Sport for Good', \
'Spirit of Sport': 'Laureus World Sports Award for Spirit of Sport'}

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

class Laureus(VTVSpider):
    name = "laureus"
    allowed_domains = ["www.laureus.com"]
    start_urls = ["http://www.laureus.com/"]
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

    def get_player_id(self, player_name, game):
        connection, cursor = mysql_connection()
        query = 'select gid from sports_participants where  title = %s and game = %s'
        values = (player_name, game)
        cursor.execute(query, values)
        pid = cursor.fetchone()
        if pid:
            pid = str(pid[0])
        return pid
        connection.close()

    def populate_sports_awards(self, player_name, award_cat, award_title, venue, season, result_type, genre, game):
        aw_id = self.get_award_id(award_title)
        awc_id = self.get_award_cat_id(award_cat)
        pid = self.get_player_id(player_name, game)

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

    def populate_sports_history(self, player_name, award_cat, award_title, venue, season, result_type, genre, game):
        pid = self.get_player_id(player_name, game)
        id_ = self.get_cat_details(award_cat)
        ids =  self.get_award_details(award_title)
        if pid and ids and id_:
            conne, cursor = mysql_conn()
            cursor.execute(INSERT_AWARD_HIS % (str(ids[0][0]), str(ids[0][1]), str(id_[0][0]), str(id_[0][1]), genre, season, str(venue),result_type, pid, pid))
            conne.close()



    def parse(self, response):
        awards_dict = {}
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@id="mainNav"]//ul//li//a[contains(@href, "/awards/")]')
        for node in nodes:
            ref = extract_data(node, './/@href')
            ref = "http://www.laureus.com" +ref
            if "2013" not in ref:
                continue
            yield Request(ref, callback=self.parse_next, meta = {})

    def parse_next(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="awards_widget clearfix"]//div[@class="widget_category"]')
        season = "".join(response.url.split('/')[-1]).strip()
        venue = extract_data(hxs, '//h3//a[contains(@href, "host-city-rio-de-janeiro")]//text()').split('-')[-1].strip()
        if not venue:
            venue = extract_data(hxs, '//div[@class="c_h_des"]//h3//text()')
            if "Laureus World Sports Awards" in venue:
                venue = venue.split('-')[-1].strip()
            else:
                venue = ''
        award_title = "Laureus World Sports Awards"
        result_type ="winner"
        for node in nodes:
            award_cat = extract_data(node, './/div[@class="title"]/text()').strip().replace('\r\n', '').strip()
            player_name = extract_data(node, './/div[@class="desc"]/text()').strip().replace('\r\n', '').strip()
            for key, value in CAT_DICT.iteritems():
                if award_cat in key:
                    award_cat = value
                else:
                    award_cat = award_cat
            player_ur = extract_data(node, './/div[@class="image"]//a//@href')
            player_ur = "http://www.laureus.com" +player_ur
            yield Request(player_ur, callback = self.parse_det, meta = {'venue': venue, 'season': season, 'award_cat': award_cat, 'player_name': player_name})

    def parse_det(self, response):
        hxs = Selector(response)
        game = extract_data(hxs, '//div[@class="profile-field"]/text()')
        venue = response.meta['venue']
        season = response.meta['season']
        award_cat = response.meta['award_cat']
        player_name = response.meta['player_name']
        if "Australia Men's Cricket Team" in player_name:
            player_name ="Australia national cricket team"
        if "Manchester United" in player_name:
            player_name ="Manchester United FC"
        if "European Ryder Cup Team" in player_name:
            player_name ="Europe Ryder Cup team"
        if "Spain Football Team" in player_name:
            player_name = "Spain national football team"
            game ="Soccer"
        if "South Africa Rugby Team" in player_name:
            player_name = "South Africa national rugby union team"
        if "Italy " in player_name:
            player_name = "Italy national football team"
            game = "Soccer"
        if "Greece Men's Football Team" in player_name:
            player_name = "Greece national football team"
            game = "Soccer"
        if "England Rugby Union Team" in player_name:
            player_name = "England national rugby union team"
        if "Jessica Ennis" in player_name:
            player_name = "Jessica Ennis-Hill"
        if player_name =="Serena Williams":
            game = "Tennis"
        if player_name =="Lindsey Vonn":
            game = "Alpine Skiing"
        if player_name =="Zinedine Zidane":
            game = "Soccer"
        if player_name =="Daniel Dias":
            game =  "Swimming"
            player_name = "Daniel de Faria Dias"
        if player_name =="Sergey Bubka":
            game =  "Athletics"
        if "FC Barcelona" in player_name:
            game = "Soccer"
        if "Johan Cruyff" in player_name:
            game = "Soccer"
        if "Ronaldo" in player_name:
            game = "Soccer"
        if "Goran Ivani" in player_name:
            player_name = "Goran Ivanisevic"
        if "Kip Keino" in player_name:
            player_name = "Kipchoge Keino"
        if "Athletics" in game:
            game ="Athletics"
        if "Manchester United" in player_name:
            player_name= "Manchester United FC"
            game ="Soccer"
        if "Verena Bentele" in player_name:
            game ="Skiing"
        if "Michael Milton" in player_name:
            game ="Alpine Skiing"
        if "Brazil's National Football Team" in player_name:
            player_name = "Brazil national football team"
            game = "Soccer"
        if "Earle Connor" in player_name:
            player_name = "Earle Connor"
        if "Lance Armstrong" in player_name:
            game ="Cycling"
            player_name = "Lance Armstrong"
        if "Marion Jones" in player_name:
            game ="Athletics"
            player_name = "Marion Jones"
        if "French Men's Football Team" in player_name:
            game = "Soccer"
            player_name = "France national football team"
        if "Martin Braxenthaler" in player_name:
            game ="Alpine Skiing"
            player_name = "Martin Braxenthaler"
        if "Sebastian Coe" in player_name:
            game ="Athletics"

        award_title = "Laureus World Sports Awards"
        result_type ="winner"
        if "Pole Vault" in game:
            game = "Athletics"
        if "Formula One" in game:
            game = "auto racing"
        if "Shaun Palmer" in player_name:
            game ="snowboard"
        if "Vinny Lauwers" in player_name:
            game ="Athletics"
        for key, value in GENER_DICT.iteritems():
            if game not in key:
                continue
            if "Motor Cycling" in game:
                game = "motorcycle racing"
            if "Snowboarding" in game:
                game = "snowboard"
            game = game.lower()
            genre_gid = value
            genre = game+"{"+genre_gid+"}"
            if 'Wheelchair Racing' in key:
                genre = ''
        self.populate_sports_awards(player_name, award_cat, award_title, venue, season, result_type, genre, game)
        self.populate_sports_history(player_name, award_cat, award_title, venue, season, result_type, genre, game)
