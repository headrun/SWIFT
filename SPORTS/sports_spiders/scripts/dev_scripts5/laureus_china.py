import re
from scrapy.selector import Selector
from vtvspider import VTVSpider, \
extract_data, extract_list_data, get_nodes
from scrapy.http import Request
import MySQLdb

INSERT_AWARD_RESULTS = 'INSERT INTO sports_awards_results (award_id, category_id, genre, sport_id, location, season, result_type, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'
INSERT_AWARD = 'INSERT INTO sports_awards (id, award_gid, award_title, genre, sport_id, created_at, modified_at) VALUES (%s, %s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE award_title = %s'
INSERT_AWARD_CATEGORY = 'INSERT INTO sports_awards_category (id, title_gid, title, genre, sport_id, created_at, modified_at) VALUES (%s, %s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE title = %s'
INSERT_AWARD_HIS = 'INSERT INTO  sports_awards_history (award_id, award_gid, category_id, category_gid, genre, sport_id, season, location, winner_nominee, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'

def mysql_connection():
    connection = MySQLdb.connect(host = '10.28.216.45', user = 'veveo', db = 'SPORTSDB_DEV')
    cursor = connection.cursor()
    return connection, cursor
def mysql_conn():
    conne = MySQLdb.connect(host = '10.4.218.81', user = 'root', db = 'AWARDS')
    cursor = conne.cursor()
    return conne, cursor


GENER_DICT = {'Running': 'G157', 'Auto Racing': 'G73', \
                'Tennis': 'G55', 'Swimming': 'G176', \
                'Cricket': 'G96', 'Rugby Union': 'SG1321', 'Football': 'G51', \
                'Athletics': 'G258', 'Surfing': 'G175', 'Golf': 'G29', \
                'Snowboard': 'G167', 'Basketball': 'G13', \
                'Cycling': 'G80', 'Skiing': 'G165', 'Motorcycle Racing': 'G133', \
                'Baseball': 'G12', 'Auto Racing': 'G73', \
                'Skateboarding': 'G162', \
                'Soccer': 'G51', 'Wheelchair Racing': '', \
                'Alpine Skiing': 'SG1315', 'Cross-country Skiing': 'SG1318', \
                'Nordic Skiing': 'SG1317'}

class LaureusChinaAwards(VTVSpider):
    name = "laureus_china_awards"
    #allowed_domains = ["http://lwsa16.laureus.com/"]
    start_urls = ["http://lwsa16.laureus.com/"]

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
            cursor.execute(INSERT_AWARD_CATEGORY, (awc_id, awc_gid, award_cat, '', award_cat))
            return awc_id, awc_gid
        return awc_id
        connection.close()

    def get_player_id(self, player_name, game):
        connection, cursor = mysql_connection()
        query = 'select gid, sport_id from sports_participants where  title = %s'
        values = (player_name)
        cursor.execute(query, values)
        vals = cursor.fetchone()
        import pdb;pdb.set_trace()
        a = vals[0]
        b= vals[-1]
        print a,b
        '''pid = vals[0]
        sport_id = vals[1]
        if pid:
            pid = str(pid)
        if sport_id:
            sport_id = ''.join(re.findall('\d+',sport_id))
        return pid, sport_id'''
        connection.close()

    def populate_sports_awards(self, player_name, award_cat, award_title, venue, season, result_type, genre, game):
        aw_id = self.get_award_id(award_title)
        awc_id = self.get_award_cat_id(award_cat)
        pid, sport_id = self.get_player_id(player_name, game)

        if aw_id and pid and awc_id and sport_id:
            connection, cursor = mysql_connection()
            cursor.execute(INSERT_AWARD_RESULTS % (aw_id, awc_id, genre, sport_id, str(venue), season, result_type, pid, pid))
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
        pid, sport_id = self.get_player_id(player_name, game)
        id_ = self.get_cat_details(award_cat)
        ids =  self.get_award_details(award_title)
        if pid and ids and id_:
            conne, cursor = mysql_conn()
            cursor.execute(INSERT_AWARD_HIS % (str(ids[0][0]), str(ids[0][1]), str(id_[0][0]), str(id_[0][1]), genre, sport_id, season, str(venue),result_type, pid, pid))
            conne.close()

    def parse(self, response):
        hxs = Selector(response)
        ref= extract_data(hxs, '//div[@class="tab"]//a[contains(@href, "nominees")]//@href')
        ref = "http://lwsa16.laureus.com" +ref
        venue_details = extract_data(hxs, '//p[@class="head-text"]//text()')
        season = venue_details.split(' ')[-1]
        venue  = venue_details.split(' ')[-2]
        yield Request(ref, callback=self.parse_next, meta ={'venue': venue, 'season': season})

    def parse_next(self, response):
        hxs    = Selector(response)
        season = response.meta['season']
        venue  = response.meta['venue']
        nodes = get_nodes(hxs, '//div[@class="col-lg-12"]//ul//li//a[contains(@href, "#")]')
        for node in nodes:
            #import pdb;pdb.set_trace()
            ref = extract_data(node, './/@href')
            ref = "http://lwsa16.laureus.com/" + ref
            yield Request(ref, callback=self.parse_award, meta = {'venue': venue, 'season': season})

    def parse_award(self, response):
        hxs    = Selector(response)
        season = response.meta['season']
        venue  = response.meta['venue']
        award  = response.url.split('/')[-2].strip()
        pl_nodes = get_nodes(hxs, '//div[@class="col-lg-12"]//div[@class="row centered nominees"]//div[@class="col-lg-4 col-md-4 col-sm-4"]')
        for node in pl_nodes:
            player_name = extract_data(node, './/h3//text()').replace('European', 'Europe').replace("Men's", 'National').replace('\r\n', '').strip().encode('utf-8').replace('\u010', '').replace('\u0107', '').replace('\xc4\x87', '').replace('  ', ' ').replace('\xc3\xb8', '')
            if "Real Madrid" in player_name:
                player_name = "Real Madrid CF"
            if "Marin" in player_name:
                player_name = "Marin Cilic"
            if "Mario" in player_name:
                player_name = "Mario Gotze"
            if "Marit Bj" in player_name:
                player_name = "Marit Bjorgen"
            game_details = extract_data(node, './/div[@class="small-info"]//p//text()')
            if "(" in game_details:
                game = "".join(game_details.split(') ')[1].strip())
            else:
                game = game
            if "Football" in game:
                game = "Soccer"
            if "Motorcycling" in game:
                game = "Motorcycle Racing"
            if "Snowboarding" in game:
                game = "Snowboard"
            if "Motor Racing" in game:
                game = "Auto Racing"
            if "Rugby" in game:
                game = "Rugby Union"
            if "Tennnis" in game:
                game = "Tennis"
            if "Skydiving" in game or "Trials Cycling" in game or "Boccia" in game:
                continue
            for key, value in GENER_DICT.iteritems():
                if game not in key:
                    continue
                game = game.lower()
                genre_gid = value
                genre = game
            award_title = "Laureus World Sports Awards"
            result_type = 'nominee'
            award_cat = "Laureus World Sports Award for " +award + " of the Year"
            if "action" in award:
                award_cat = "Laureus World Sports Award for Action Sportsperson of the Year"
            if "disability" in award:
                award_cat = "Laureus World Sports Award for Sportsperson of the Year with a Disability"

            self.populate_sports_awards(player_name, award_cat, award_title, venue, season, result_type, genre, game)
            self.populate_sports_history(player_name, award_cat, award_title, venue, season, result_type, genre, game)





