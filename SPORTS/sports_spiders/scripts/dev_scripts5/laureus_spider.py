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
                'Cricket': 'G96', 'Rugby': 'G156', 'Football': 'G51', \
                'Athletics': 'G258', 'Surfing': 'G175', 'Golf': 'G29', \
                'Snowboarding': 'G167', 'Basketball': 'G13', \
                'Cycling': 'G80', 'Skiing': 'G165', 'Motor Cycling': 'G133', \
                'Baseball': 'G12', 'Motor Racing': 'G73', \
                'Skateboarding': 'G162'}

INSERT_AWARD_RESULTS = 'INSERT INTO sports_awards_results (award_id, category_id, genre, location, season, result_type, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'
INSERT_AWARD = 'INSERT INTO sports_awards (id, award_gid, award_title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE award_title = %s'
INSERT_AWARD_CATEGORY = 'INSERT INTO sports_awards_category (id, title_gid, title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE title = %s'
def mysql_connection():
    connection = MySQLdb.connect(host = '10.4.18.183', user = 'root', db = 'SPORTSDB')
    cursor = connection.cursor()
    return connection, cursor


class LaureusAwards(VTVSpider):
    name = "laureus_awards"
    allowed_domains = ["www.laureus.com"]
    start_urls = ["https://www.laureus.com/awards2014/"]
    def get_award_id(self, award_title):
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
            cursor.execute(INSERT_AWARD, (aw_id, aw_gid, award_title, '', award_title))
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
            awc_id = str(ids[0])
        else:
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
        query = 'select gid from sports_participants where  title = %s and game= %s'
        values = (player_name, game)
        cursor.execute(query, values)
        pid = cursor.fetchone()
        connection.close()
        if pid:
            pid = str(pid[0])
        return pid

    def populate_sports_awards(self, player_name, award_cat, award_title, venue, season, result_type, genre, game):
        aw_id = self.get_award_id(award_title)
        awc_id = self.get_award_cat_id(award_cat)
        pid = self.get_player_id(player_name, game)
        if aw_id and pid:
            connection, cursor = mysql_connection()
            cursor.execute(INSERT_AWARD_RESULTS % (aw_id, awc_id, genre, str(venue), season, result_type, pid, pid))
            connection.close()


    def parse(self, response):
        awards_dict = {}
        hxs = Selector(response)
        season = "".join(response.url.split('awards')[1].replace('/', '')).strip()
        venue_details = extract_data(hxs, '//p[@class="head-text"]//text()')
        venue = "".join(re.findall(r'Awards (.*) 2014', venue_details))
        nodes = get_nodes(hxs, '//div[@class="col-lg-12"]//div[@class="sub-container"]/div')
        for node in nodes:
            award = extract_data(node, './/@id')
            nominees = get_nodes(node, './/div[@class="row centered nominees"]//div[@class="col-lg-4 col-md-4 col-sm-4"]')
            for nominee in nominees:
                player_name = extract_data(nominee, './/h3//text()')
                if "Bayern Munich" in player_name:
                    player_name = "FC Bayern Munich"
                if "Afghanistan" in player_name:
                    player_name ="Afghanistan national cricket team"
                if "Nairo Quintana" in player_name:
                    player_name ="Nairo Alexander Quintana Rojas"
                details = extract_data(nominee, './/div[@class="small-info"]//p//text()')
                game = "".join(details.split(') ')[1].strip())
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
                country = "".join(details.split(') ')[0].replace('(', '').strip())
                award_title = "Laureus World Sports Awards"
                result_type = 'nominee'
                award_cat = "Laureus World Sports Award for " +award + " of the Year"
                if "action" in award:
                    award_cat = "Laureus World Sports Award for "+award + " Sportsperson of the Year"
                if "disability" in award:
                    award_cat = "Laureus World Sports Award for "+ "Sportsperson of the Year with a Disability"
                self.populate_sports_awards(player_name, award_cat, award_title, venue, season, result_type, genre, game)
        winner_nodes = get_nodes(hxs, '//div[@class="row top-margin-30 winners main-tab"]//div[contains(@class, "row centered nominees")]/div[@class="col-lg-4 col-md-4 col-sm-4"]')
        for wnodes in winner_nodes:
            cat_title = extract_data(wnodes, './div/h3[contains(@class, "category-title")]/text()')
            player_name = extract_data(wnodes, './div/h3[contains(@class, "category-title")]//following-sibling::h3//text()')
            details = extract_data(wnodes, './/div[@class="small-info"]//p//text()')
            game = "".join(details.split(') ')[1].strip())
            country = "".join(details.split(') ')[0].replace('(', '').strip())
            if "Afghanistan" in player_name:
                player_name ="Afghanistan national cricket team"
            if "Bayern Munich" in player_name:
                player_name = "FC Bayern Munich"
            award_cat = "Laureus World Sports Award for " +cat_title
            if "Sport for GoodAward" in award_cat:
                award_cat = "Sport for Good Award"
            if "Spirit of Sport" in award_cat:
                award_cat = "Spirit of Sport Award"
            if "with a Disability" in award_cat:
                award_cat = "Laureus World Sports Award for Sportsperson of the Year with a Disability"
            award_title = "Laureus World Sports Awards"
            result_type = 'winner'
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

            self.populate_sports_awards(player_name, award_cat, award_title, venue, season, result_type, genre, game)
