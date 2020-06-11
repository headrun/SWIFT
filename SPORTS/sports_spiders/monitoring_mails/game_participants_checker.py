import MySQLdb, json, datetime, smtplib
from vtv_utils import vtv_send_html_mail_2
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart


class GameParticipantsChecker(object):

    def normalizer(self, text):
        if type(text) == int:
            text = str(text)
        text = text.replace(u'\u2013', '').replace(',', '').split('(')[0].strip()
        return str(text)

    def __init__(self):
        self.cursor = MySQLdb.connect(host='10.28.218.81', db = 'SPORTSDB').cursor()
        self.tournament_dict = {}
        self.tournaments_list = ['591', '572', '2235', '3601', '585', '573', '589', '597', '598', '596', '34', '216', '215', '562', '599', '1105', '595', '4096', '586', '558', '571', '574', '32', '28', '567', '559', '1064', '564', '575', '29', '33', '610', '579', '3626', '590', '580', '3838', '35', '88', '2841', '2850', '229', '197', '9', '1870', '1844', '4992', '240', '5982', '1825', '1891', '1808', '1892', '1895']
        with open("tournament_counts.json") as tournament_counts:
            self.tournament_counts = json.loads(tournament_counts.read())
            guid_query = "select title, id, exposed_gid from GUIDMERGE.sports_wiki_merge G, sports_tournaments T where T.gid collate utf8_unicode_ci = G.child_gid and T.id in (%s)"
            self.cursor.execute(guid_query % ', '.join(self.tournaments_list))
            guid_data = self.cursor.fetchall()
            for dt in guid_data:
                title, _id, gid = dt
                team_count = self.normalizer(self.tournament_counts[gid])
                if ' ' in unicode(team_count):
                    team_count = self.normalizer(team_count)
                self.tournament_dict.update({title: [int(_id), int(team_count)]})

    def main(self):
        now = datetime.datetime.now()
        receivers = ["sports@headrun.com"]
        #receivers  = ['sports@headrun.com', 'bibeejan@headrun.com', 'vinuthna@headrun.com','durgakp@headrun.com','sreeharsha@headrun.com','saikeerthi@headrun.com','sirisha@headrun.com','sownthirya@headrun.com']
        sender = "noreply@headrun.com"
        subject = "Major Leagues Participants Check"
        table = '<table border = "1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px">'
        table += "<tr><th>Tournament</th><th>DB Count</th><th>Need Count"
        for key, value in self.tournament_dict.iteritems():
            tournament_id, count = value
            dates_query = 'select season_start, season_end from sports_tournaments where id = %s'
            self.cursor.execute(dates_query % tournament_id)
            tou_data = self.cursor.fetchone()
            if tou_data:
                start_date, end_date = tou_data
                if now < start_date: continue
                if start_date == end_date: end_date = end_date + datetime.timedelta(days=1)
                count_query = 'select count(distinct participant_id) from sports_games SG, sports_games_participants GP, sports_participants SP where SG.id = GP.game_id and GP.participant_id = SP.id and SG.tournament_id = %s and SG.game_datetime between %s and %s and participant_id not in (169107, 677, 688) and title not like "TBD" and SG.status != "Hole" and event_id not in (899,1493, 320,900)'
                ##conference ids (677, 688) not team ids
                values = (tournament_id, str(start_date.date()), str(end_date.date()))
                self.cursor.execute(count_query, values)
                db_count = self.cursor.fetchone()[0]
                if int(db_count) == count:
                    table += "<tr><td>%s</td><td align = 'right'>%s</td><td align = 'right'>%s</td>" % (key, str(db_count), count)
                else:
                    table += "<tr><td>%s</td><td align = 'right' style = 'color: red;'>%s</td><td align = 'right'>%s</td>" % (key, str(db_count), count)

        self.cursor.close()
        table += "</table>\n"
        print "Mail Sent!"
        vtv_send_html_mail_2('mail.log', 'localhost', sender, receivers, subject, '', ''.join(table), '')
        #self.send_mail(''.join(table), subject)

    def send_mail(self, body, subject):
        msg = MIMEText(body, 'html', _charset='UTF-8')
        msg['Subject'] = subject
        _to = ['sports@headrun.com']
        _from = 'headrun@veveo.net'
        msg['From'] = _from
        msg['To'] = ','.join(_to)
        s = smtplib.SMTP('localhost', 25) 
        s.sendmail(_from, _to, msg.as_string())
        s.quit()


if __name__ == '__main__':
    GameParticipantsChecker().main()        
