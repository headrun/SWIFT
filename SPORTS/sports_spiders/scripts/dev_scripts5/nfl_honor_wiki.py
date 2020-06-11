from scrapy.selector import Selector
from vtvspider import VTVSpider, \
extract_data, get_nodes
from scrapy.http import Request
import re
import MySQLdb

CAT_DICT= {'AP MVP': 'National Football League Most Valuable Player Award',
'AP Offensive Player': 'AP NFL Offensive Player of the Year Award',
'AP Comeback Player of the Year': 'AP Comeback Player of the Year',
'AP Offensive Rookie': 'AP Offensive Rookie of the Year',
'AP Defensive Player': 'AP NFL Defensive Player of the Year Award',
'AP Defensive Rookie': 'AP Defensive Rookie of the Year',
'Walter Payton Man of the Year Award': 'Walter Payton NFL Man of the Year Award',
'Play of the Year': 'NFL Play of the Year Award', \
'NFL Salute to Service Award': 'Salute to Service Award',
'NFL.com Fantasy Player': 'Fantasy Player of the Year', \
'Madden Most Valuable Protectors Award': 'Madden Most Valuable Protectors Award', \
'Pepsi NFL Rookie of the Year': 'National Football League Rookie of the Year Award',
'GMC Never Say Never Moment of the Year': 'GMC Never Say Never Moment of the Year', \
'NFL.com Fantasy Player of the Year' : 'Fantasy Player of the Year', \
'Walter Payton NFL Man of the Year Award': 'Walter Payton NFL Man of the Year Award',
'FedEx Air Player of the Year': 'FedEx Air Player of the Year', \
'FedEx Ground Player of the Year': 'FedEx Ground Player of the Year', \
'Bridgestone Play of the Year': 'NFL Play of the Year Award', \
'"Greatness on the Road" Award': 'Greatness on the Road', \
'Salute to Service Award': 'Salute to Service Award', \
'AP Offensive Player of the Year': 'AP NFL Offensive Player of the Year Award', \
'AP Defensive Player of the Year': 'AP NFL Defensive Player of the Year Award', \
'AP Offensive Rookie of the Year': 'AP Offensive Rookie of the Year', \
'AP Defensive Rookie of the Year': 'AP Defensive Rookie of the Year', \
'Pepsi NEXT Rookie of the Year': 'National Football League Rookie of the Year Award', \
'Bridgestone Performance Play of the Year': 'NFL Play of the Year Award', \
'Greatness on the Road Award': 'Greatness on the Road', \
'Deacon Jones Award': 'Deacon Jones Award'
}

INSERT_AWARD_RESULTS = 'INSERT INTO sports_awards_results (award_id, category_id, genre, location, season, result_type, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'
INSERT_AWARD = 'INSERT INTO sports_awards (id, award_gid, award_title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE award_title = %s'
INSERT_AWARD_CATEGORY = 'INSERT INTO sports_awards_category (id, title_gid, title, genre, created_at, modified_at) VALUES (%s, %s, %s, %s, now(), now()) ON DUPLICATE KEY UPDATE title = %s'
INSERT_AWARD_HIS = 'INSERT INTO  sports_awards_history (award_id, award_gid, category_id, category_gid, genre, season, location, winner_nominee, participants, created_at, modified_at) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", now(), now()) ON DUPLICATE KEY UPDATE participants = "%s"'

GET_PL_GID = 'select sports_gid from sports_wiki_merge where wiki_gid ="%s"'

def mysql_connection():
    connection = MySQLdb.connect(host = '10.4.18.183', user = 'root', db = 'SPORTSDB')
    cursor = connection.cursor()
    return connection, cursor
def mysql_conn():
    conne = MySQLdb.connect(host = '10.4.2.187', user = 'root', db = 'AWARDS')
    cursor = conne.cursor()
    return conne, cursor



class NFLHonoresWiki(VTVSpider):
    name = "nflhonores_wiki"
    allowed_domains = []
    start_urls = ['http://en.wikipedia.org/wiki/1st_Annual_NFL_Honors', \
                  'http://en.wikipedia.org/wiki/2nd_Annual_NFL_Honors', \
                  'http://en.wikipedia.org/wiki/3rd_Annual_NFL_Honors']
    start_urls = ['https://en.wikipedia.org/wiki/5th_Annual_NFL_Honors']
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
            awc_id= ''
        return awc_id
        connection.close()
    def get_player_id(self, player_name, game, award_gid):
        conn   = MySQLdb.connect(host="10.4.2.187", user="root", db="GUIDMERGE")
        cursor = self.conn.cursor()
        cursor.execute(GET_PL_GID %(award_gid))
        pid_ = cursor.fetchone()
        if pid_:
            pid_ = str(pid_[0])
        return pid_
        conn.close()

    def populate_sports_awards(self, player_name, award_cat, award_title, venue, season, result_type, genre, game, wiki_gid):
        aw_id = self.get_award_id(award_title)
        awc_id = self.get_award_cat_id(award_cat)
        pid = self.get_player_id(player_name, game, wiki_gid)
        print award_cat

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

    def populate_sports_history(self, player_name, award_cat, award_title, venue, season, result_type, genre, game, wiki_gid):
        pid = self.get_player_id(player_name, game, wiki_gid)
        id_ = self.get_cat_details(award_cat)
        ids =  self.get_award_details(award_title)
        if pid and ids and id_:
            conne, cursor = mysql_conn()
            cursor.execute(INSERT_AWARD_HIS % (str(ids[0][0]), str(ids[0][1]), str(id_[0][0]), str(id_[0][1]), genre, season,
            str(venue),result_type, pid, pid))
            conne.close()



    def parse(self, response):
        hxs = Selector(response)
        nfl_nodes = get_nodes(hxs, '//table[@class="wikitable sortable"]//tr//td[2]')
        season = extract_data(hxs, '//p//a[contains(@href, "_season")]//@title').replace(' NFL season', '')
        if not season:
            season = extract_data(hxs, '//table[@class="infobox vevent"]//tr//th[contains(text(), "Date")]//following-sibling::td/text()')
            season = season.split(',')[-1].strip()
        venue = extract_data(hxs, '//table[@class="infobox vevent"]//tr//th[contains(text(), "Site")]//following-sibling::td//a[@class="mw-redirect"]//text()')
        venue = venue.split(',')[0].strip()
        pl_link = ''
        for node in nfl_nodes:
            player_name = extract_data(node, './/a[contains(@href, "/wiki/")]//text()')
            player_link = extract_data(node, './/a[contains(@href, "/wiki/")]//@href')
            if player_link:
                player_link = "http://en.wikipedia.org" +player_link
            award_name = extract_data(node, './/preceding-sibling::td[1]//text()')
            if player_link:
                pl_link = player_link
            else:
                player_link = pl_link
            if "AP Head Coach" in award_name or "NFL High School Coach" \
                in award_name or "AP Coach of the Year" in award_name:
                continue
            yield Request(player_link, callback=self.parse_wikigid, \
                        meta ={'player_name': player_name, \
                        'award_name': award_name, 'season':season, 'venue': venue}, \
                        dont_filter=True)

    def parse_wikigid(self, response):
        player_name = response.meta['player_name']
        award_name = response.meta['award_name']
        venue = response.meta['venue']
        award_title = "NFL Honors"
        game ="football"
        genre = "football{G27}"
        result_type = "winner"
        season = response.meta['season']
        if "2012" in season:
            venue = "Louisiana"
        if "2013" in season:
            venue = "New York City"
        wiki_gid = re.findall('"wgArticleId":(\d+)', response.body)
        if wiki_gid:
           wiki_gid = "WIKI%s" %wiki_gid[0]
        award_cat = CAT_DICT.get(award_name, '')


        self.populate_sports_awards(player_name, award_cat, award_title, venue, season, result_type, genre, game, wiki_gid)
        self.populate_sports_history(player_name, award_cat, award_title, venue, season, result_type, genre, game, wiki_gid)

