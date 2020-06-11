import  traceback
from datetime import datetime
import MySQLdb
from vtv_utils import initialize_timed_rotating_logger#, vtv_send_html_mail

class SportsValidator:
    def __init__(self):
        self.today = datetime.now().date().strftime("%Y-%m-%d")
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        #self.con = MySQLdb.connect(host="10.4.2.207", db="IMAGEDB", user="root")
        #self.cur = self.con.cur()
        self.logger = initialize_timed_rotating_logger('sports_validator.log')
        self.run_main()

    def run_main(self):
        query = 'select gid, title from sports_participants where image_link= "https://i.duckduckgo.com/i/feba8576.png" and game = "cycling"'
        self.cursor.execute(query)
        gids = self.cursor.fetchall()
        for gid in gids:
            gid, title = gid[0], gid[1]
            #con = MySQLdb.connect(host="10.4.2.207", db="IMAGEDB", user="root")
            con = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB")
            cursor  = con.cursor()
            query = 'select wiki_gid from sports_wiki_merge where sports_gid = "%s"' % gid
            self.cursor.execute(query)
            wiki_gid = self.cursor.fetchone()
            if wiki_gid:
                if len(wiki_gid) == 1:
                    wiki_gid = wiki_gid[0]
                    query = 'select image_url, image_name from wiki_images where gid = "%s"' % (wiki_gid)
                    data = cursor.execute(query)
                    data = cursor.fetchall()
                    if len(data) != 0:
                        image_url = data[0][0]
                        image_title = data[0][1].replace('Image:', '').replace('.jpg', '')
                        update = 'update sports_participants set image_link = "%s" where gid = "%s"' % (image_url, gid)
                        wiki_gids_imgdb = open('wiki_gids_imgdb', 'ab+').write('%s####%s###%s\n' % (gid, image_title, wiki_gid))
                        self.cursor.execute(update)
                        #image_link, pigid, image_title = self.update_images_sportsdb(image_url, gid[0], image_title)
                    else:
                        no_wiki_gid = open('no_wiki_gid_imgdb', 'ab+').write('%s###%s\n' % (gid, wiki_gid))
                        print "error"
            else:
                gids_file = open('player_gids', 'ab+').write('%s###%s\n' % (gid, wiki_gid))


if __name__ == "__main__":
    try:
        SportsValidator()
    except:
        print "Stats file needed!"
        traceback.print_exc()

