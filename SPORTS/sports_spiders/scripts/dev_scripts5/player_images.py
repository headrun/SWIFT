import re
import MySQLdb
from vtvspider_dev import VTVSpider
from scrapy.selector import Selector
from scrapy.http import Request
import urllib2

UPDATE_Q = 'update sports_participants set image_link="%s" where id="%s" and game = "%s" and image_link = "http://www.procyclingstats.com/" limit 1'


def mysql_connection():
    #conn = MySQLdb.connect(host="10.4.15.132", user="root", db="SPORTSDB_BKP")
    conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = conn.cursor()
    return conn, cursor

class UpdateImages:
    images = open("cycling_img.txt", "r+")

    def main(self):
        for data in self.images:
            data = data.strip()
            got_data = data.split(',')
            if len(got_data) == 3:
                pl_id, pl_name, image_link = data.split(',')
                values = (image_link, pl_id, 'cycling')
                query = UPDATE_Q % values
            if image_link:
                conn, cursor = mysql_connection()
                cursor.execute(query)
                conn.close()



if __name__ == '__main__':
    OBJ = UpdateImages()
    OBJ.main()

