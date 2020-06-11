from datetime import datetime
import traceback
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail
import MySQLdb

class SportsValidator:
    def __init__(self):
        self.today = datetime.now().date().strftime("%Y-%m-%d")
        #self.conn = MySQLdb.connect(host="10.4.18.183", db="SPORTSDB", user="root")
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd='veveo123', db="SPORTSDB")
        self.cursor = self.conn.cursor()
        self.logger = initialize_timed_rotating_logger('sports_validator.log')
        self.updated_gids = None
        self.inserted_gids = None
        self.run_main()

    def send_mail(self, source, text):
        subject = 'Gids to be marked as Hole for %s' %source
        #server = '10.4.1.112'
        server = "localhost"
        sender = 'headrun@veveo.net'
        receivers = ['headrun@veveo.net']
        text = '\n'.join([str(t) for t in text])
        vtv_send_html_mail(self.logger, server, sender, receivers, subject, text, '', '')

    def get_query_result(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def run_main(self):
        query = 'select id, gid, title, season_start, season_end from sports_tournaments \
                   where season_start < now() and season_end > now()'
        self.cursor.execute(query)
        active_tournaments = self.cursor.fetchall()

if __name__ == "__main__":
    try:
        SportsValidator()
    except:
        print "Stats file needed!"
        traceback.print_exc()

