import MySQLdb
import datetime
import smtplib
import os
import string
import ssh_utils
from sports_check_fields import *
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders


def send_mail(to, subject, text, fro="", files=[], cc=[], bcc=[], server="localhost"):
    assert type(to)==list
    assert type(files)==list
    assert type(cc)==list
    assert type(bcc)==list

    message = MIMEMultipart()
    message['From'] = fro
    message['To'] = COMMASPACE.join(to)
    message['Date'] = formatdate(localtime=True)
    message['Subject'] = subject
    message['Cc'] = COMMASPACE.join(cc)
    message.attach(MIMEText(text, 'html'))


    for f in files:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(open(f, 'rb').read())
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        message.attach(part)

    addresses = []
    for x in to:
        addresses.append(x)
    for x in cc:
        addresses.append(x)
    for x in bcc:
        addresses.append(x)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(fro, addresses, message.as_string())
    smtp.close()


def main():

    to      = ['headrun@veveo.net']

    subject = "Sports tournament stats !!!"
    text    = "Please check the stats and fix the issues"
    sender  = "headrun@veveo.net"

    now     = datetime.datetime.now()

    conn    = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSDB')
    cursor  = conn.cursor()

    tou_ids = set()
    eve_ids = set()

    cursor.execute('select tournament_id from sports_games where status = "scheduled"')
    tou_id = cursor.fetchall()
    cursor.execute('select event_id from sports_games where status = "scheduled"')
    event_id = cursor.fetchall()

    for t_id in tou_id:
        tou_ids.add(t_id[0])
    tournament_id = list(tou_ids)
    if None in tournament_id:
        tournament_id.remove(None)
    for e_id in event_id:
        eve_ids.add(e_id[0])
    event_ids = list(eve_ids)
    if None in event_ids:
        event_ids.remove(None)

    tournament_ids = set()
    tou = tou_id + event_id
    for t in tou:
        if t[0] != 0:
            tournament_ids.add(t[0])

    wrong_dates = set()
    test_data = list(tournament_ids)
    test_data.remove(None)
    cursor.execute("select id, title, season_start, season_end from sports_tournaments where id in (%s)" % ','.join(map(str, test_data)))
    tournament_dates = cursor.fetchall()
    for tournament_date in tournament_dates:
        if tournament_date[0] in tournament_id:
            cursor.execute('select max(game_datetime), min(game_datetime) from sports_games where tournament_id = %s and status = "scheduled"' %tournament_date[0])
            data = cursor.fetchone()
            if data[0].date() < tournament_date[2].date() or data[0].date() > tournament_date[3].date():
                wrong_dates.add(tournament_date)
            if data[1].date() < tournament_date[2].date() or data[1].date() > tournament_date[3].date():
                wrong_dates.add(tournament_date)
        elif tournament_date[0] in event_ids:
            cursor.execute('select max(game_datetime), min(game_datetime) from sports_games where event_id = %s and status = "scheduled"' %tournament_date[0])
            data = cursor.fetchone()
            if data[0].date() < tournament_date[2].date() or data[0].date() > tournament_date[3].date():
                wrong_dates.add(tournament_date)
            if data[1].date() < tournament_date[2].date() or data[1].date() > tournament_date[3].date():
                wrong_dates.add(tournament_date)

        if tournament_date[3].date() < datetime.datetime.now().date():
            wrong_dates.add(tournament_date)

    if wrong_dates:
        text += '<br /><br /><b>Tournaments having wrong dates</b><br /><table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr><th>Id</th><th>Title</th><th>Season_Start</th><th>Season_End</th></tr>'
        for wrong_date in wrong_dates:
            text += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" %(wrong_date[0], wrong_date[1], wrong_date[2].strftime("%Y-%m-%d"), wrong_date[3].strftime("%Y-%m-%d"))
        text += "</table>"


    send_mail(to, subject, text, sender, [])
main()
