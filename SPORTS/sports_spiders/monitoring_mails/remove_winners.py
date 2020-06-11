import MySQLdb
import datetime
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2

def get_table_header(title, headers):
    table_header = '<br /><br /><b>%s</b><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' %title
    for header in headers:
        table_header += '<th>%s</th>' %header
    table_header += '</tr>'
    return table_header

def get_table_body(removed_list):
    body = ''
    for data in removed_list:
        body += '<tr>'
        for d in data:
            body += '<td>%s</td>' %d
        body += '</tr>'
    body += '</table>'
    return body

class RemoveWinners:
    def __init__(self):
        self.receivers  = ['headrun@veveo.net', 'sports@headrun.com']
        self.sender     = "headrun@veveo.net"
        self.conn       = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSDB')
        self.cursor     = self.conn.cursor()
        self.server     = 'localhost'
        self.logger     = initialize_timed_rotating_logger('removed_winners.log')
        self.year       = datetime.datetime.now().year
        self.removed_entries = set()

    def run(self):

        subject = "Removed old winners from sports_tournaments_results !!!"
        text    = "Genuine cases might be removed, Please check"
        print  text
        now     = datetime.datetime.now()

        query = 'select id, title, DATE(season_start), DATE(season_end) from sports_tournaments where season_start < now() and season_end > now()'
        self.cursor.execute(query)
        entries = self.cursor.fetchall()
        for entry in entries:
            tou_id, title, season_start, season_end = entry
            query = 'select count(*) from sports_games where tournament_id = %s and status="scheduled"'
            values = (str(tou_id))
            self.cursor.execute(query % values)
            print values
            count = self.cursor.fetchall()
            if int(count[0][0]) >0:
                query = 'select id, tournament_id, participant_id, season, result_type, result_sub_type, result_value from sports_tournaments_results where tournament_id = %s and season not in ("%s") and season not like "%%%s%%" and result_type = "winner"'
                values = (str(tou_id), str(self.year), str(self.year)[-2:])
                self.cursor.execute(query % values)
                data = self.cursor.fetchall()
                print data
                for d in data:
                    self.removed_entries.add(d)

                query = 'delete from sports_tournaments_results where tournament_id = %s and season not in ("%s") and result_type = "winner" and season not like "%%%s%%" limit 10'
                values = (str(tou_id), str(self.year), str(self.year)[-2:])
                self.cursor.execute(query % values)

        if self.removed_entries:
            headers = ('Id', 'tournament_id', 'participant_id', 'season', 'result_type', 'result_sub_type', 'result_value')

            text += get_table_header('Removed Entries from sports_tournaments_results', headers)
            text += get_table_body(self.removed_entries)
            vtv_send_html_mail_2(self.logger, self.server, self.sender, self.receivers, subject, '', text, '')

if __name__ == "__main__":
    class_obj = RemoveWinners()
    class_obj.run()
