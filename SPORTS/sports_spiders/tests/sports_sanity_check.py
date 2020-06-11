import urllib2
import MySQLdb
import datetime
import time
import smtplib
import os
import re
import pexpect
import random
import string
import ssh_utils
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders

ALPHANUM = string.letters + string.digits
CRAWLERS_PATH = '/home/veveo/crawlers/sports_setup/new_sportsdb_updations/'
CRAWLERS_LOG = {'motor_sports': 'racing/games/REPORT', 'tennis_scores': 'tennis/REPORT_SCORES', 'tennis_schedules': 'tennis/REPORT_SCHEDULE', 'tennis_winners': 'tennis/ERROR', 'icc_scores': 'cricket/icc/ICC_REPORT', 'icc_ongoing': 'cricket/icc/ICC_ONGOING_REPORT', 'cricket': 'cricket/icc/SCORES_REPORT', 'pga_tour': 'golf/pgatour/games/MISSED', 'euro_tour': 'golf/european/games/MISSED', 'champions': 'golf/champions/games/MISSED', 'web.com': 'golf/web.com/games/MISSED', 'tou_missing': 'tennis/tou_seasons/missing_tou_final', 'cricket_stadiums' : 'cricket/icc/cricket_stadiums'}

def run_remote_cmd(ip, command,
                username,
                password):

    tmpfname = '/tmp/%s' % ''.join([random.choice(ALPHANUM) for i in xrange(10)])
    tmperrfname = tmpfname + '.err'
    command = '%s 2> %s > %s' % (command, tmperrfname, tmpfname)

    try:
        # execute remote command
        result = ssh_utils.ssh_cmd(ip, username, password, command)

        # fetch stdout and stderr files
        src = '%s@%s:%s' % (username, ip, tmpfname)
        ssh_utils.scp(password, src, tmpfname)

        src = '%s@%s:%s' % (username, ip, tmperrfname)
        ssh_utils.scp(password, src, tmperrfname)

        # read the output
        stdout = open(tmpfname, 'rb').read()
        stderr = open(tmperrfname, 'rb').read()

        return result, stdout, stderr
    finally:

        if os.path.exists(tmpfname): os.remove(tmpfname)
        if os.path.exists(tmperrfname): os.remove(tmperrfname)

        ssh_utils.ssh_cmd(ip, username, password, 'rm -f %s' % tmpfname)
        ssh_utils.ssh_cmd(ip, username, password, 'rm -f %s' % tmperrfname)


def scp(ip, path, login_name, password, prompt, destination_path=None):
    """ General Utility for Secure copy. """

    ret_val = 1

    if not destination_path:
        file = path.split('/')[-1]
    else:
        file = destination_path

    secure_copy_cmd = "scp %s@%s:%s %s" % (login_name, ip, path,\
                                                             file)
    try:
        process = pexpect.spawn(secure_copy_cmd)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception, e:
        return ret_val

    index = process.expect(['Are you sure you want to continue ' \
                            'connecting .*\?', 'password:'], 100)
    if index is 0:
        process.sendline('yes')
        expect_cmd = '%s@%s\'s password:' % (login_name, ip)
        process.expect(expect_cmd)
        process.sendline(password)
    elif index is 1:
        process.sendline(password)
    else:
        return ret_val

    index = process.expect([prompt, pexpect.EOF], 1200)

    if index is 0 or 1:
        ret_val = 0
    else:
        pass

    return file, ret_val

def send_mail(to, subject, text, fro="", files=[], cc=[], bcc=[], server="10.4.1.112"):
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

    subject = "Sports sanity issues !!!"
    text    = "Please check the below sanity issues on priority"
    sender  = "headrun@veveo.net"

    conn    = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
    cursor  = conn.cursor()
    cursor.execute('select distinct(game_datetime), count(*) from sports_games where status != "Hole" and game = "auto racing" group by game_datetime having count(*) > 1')
    data    = cursor.fetchall()
    two_game_dates = set()

    for d in data:
        game_time, count = d
        cursor.execute('select tournament_id from sports_games where game_datetime="%s"'%game_time)
        val = cursor.fetchall()
        if val[0][0] == val[1][0]:
            two_game_dates.add(str(game_time))

    cursor.execute('select id, game_datetime, game from sports_games where date(game_datetime) < date(now()) and status = "scheduled"')
    games_data = cursor.fetchall()
    status = 0
    game_string = '<table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"> <tr><th>id</th><th>game_datetime</th><th>game</th><tr>'
    for game_data in games_data:
        status = 1
        game_string += "<tr>"
        for g in game_data:
            game_string += '<td>%s</td>'%g
        game_string += "</tr>"
    game_string += "</table>"

    cursor.execute('select distinct(entity_id), count(*) from sports_source_keys where entity_type = "game" group by entity_id having count(*) > 1')
    duplicate_ids = cursor.fetchall()
    duplicate_string = '<table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"> <tr><th>id</th><th>count</th><tr>'
    duplicate_status = 0
    for ids in duplicate_ids:
        duplicate_status = 1
        duplicate_string += "<tr>"
        for id in ids:
            duplicate_string += '<td>%s</td>'%id
        duplicate_string += "</tr>"
    duplicate_string += "</table>"

    games_without_stadium   = []
    query       = 'select distinct(event_id) from sports_games where game="soccer" and not event_id=0;'
    cursor.execute(query)
    event_ids   = cursor.fetchall()

    for event in event_ids:
        query = 'select id, event_id from sports_games where event_id=%s and game="soccer" and status="scheduled" and (stadium_id=0 or stadium_id="");' %event[0]
        cursor.execute(query)
        game_ids = cursor.fetchall()

        if game_ids:
            for id in game_ids:
                event_info = "%s###%s" %(id[0],id[1])
                games_without_stadium.append(event_info)

    if games_without_stadium:
        events_without_stadium ='<table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr><th>Game ids</th><th>Event Name</th><tr>'
        for id in games_without_stadium:
            game_id, event_id = id.split("###")
            query = 'select title from sports_tournaments where id=%s' %event_id
            cursor.execute(query)
            event_name  = cursor.fetchall()
            events_without_stadium += "<tr><td>%s</td><td>%s</td></tr>" %(game_id, event_name[0][0])

        events_without_stadium += "</table>"

    dup_games_golf = []
    cursor.execute('select G.game_datetime, G.tournament_id from sports_games G, sports_tournaments T where T.id = G.tournament_id and G.game = T.game and T.game = "golf" and G.status = "scheduled" group by G.tournament_id having count(*) >1')
    dup_tou_ids = cursor.fetchall()
    golf_duplicates ='<table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr><th>Game Datetime</th><th>Tournament Id</th><tr>'
    for id in dup_tou_ids:
        _info = "%s###%s" %(id[0],id[1])
        game_datetime, tou_id = _info.split('###')
        dup_games_golf.append(event_info)
        golf_duplicates += "<tr><td>%s</td><td>%s</td></tr>" %(game_datetime, tou_id)

    golf_duplicates += "</table>"

    cursor.execute('select distinct(player_id) from sports_roster where status = "active" group by player_id having count(*) > 1')
    player_data = cursor.fetchall()

    cursor.execute("select count(*) from sports_games where game = 'auto racing' and status in ('scheduled', 'completed') and year(game_datetime) = year(curdate()) and tournament_id in (53)")
    camping_tou = cursor.fetchall()

    cursor.execute("select count(*) from sports_games where game = 'auto racing' and status in ('scheduled', 'completed') and year(game_datetime) = year(curdate()) and tournament_id in (281, 50) and event_id = 0")
    autoracing = cursor.fetchall()

    cursor.execute('select count(*) from sports_roster where player_id in (select id from sports_participants where image_link = "http://www.mysportsnutrition.com/images/blank_face_placeholder.jpg") and status = "active"')
    active_null = cursor.fetchall()

    cursor.execute('select count(*) from sports_games_participants where participant_id in (select id from sports_participants where image_link in ("http://www.mysportsnutrition.com/images/blank_face_placeholder.jpg", "http://static.weltsport.net/bilder/spieler/gross/0.png", ""))')
    null_game_participants = cursor.fetchall()

    cursor.execute("select id, game from sports_participants where title = '' and participant_type = 'player'")
    empty_players = cursor.fetchall()

    cursor.execute("select count(*) from sports_stadiums where title like '%&amp;%'")
    stad_amp = cursor.fetchone()
    cursor.execute("select count(*) from sports_stadiums where title like '%,%'")
    stad_com = cursor.fetchone()

    dt      = datetime.datetime.now()
    fname   = dt.strftime('sports_sanity_%Y%m%dT%H%M%S.M%%s.html')
    fname   = fname % dt.microsecond

    scp('10.28.218.80', '/data/REPORTS/SANITY_REPORTS/SPORTSDB_SANITY_REPORT.html', 'veveo', 'veveo123', ']$', fname)

    data    = open(fname, 'r').read()
    count   = re.findall('<li type = square><a href="#basicsanity">All Sanity: (.*?)</a></li>', data)
    string  = ''

    if '/' in count[0]:
        count = count[0].strip().split('/')
    if int(count[0]) > 3:
        string = re.findall('<TABLE class="table table-bordered table-hover">.*?</TABLE>', data, re.DOTALL | re.MULTILINE)

    log_status = 0
    log_string = '<table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"> <tr><th>error</th><th>log_path</th><tr>'


    for source, path in CRAWLERS_LOG.iteritems():
        file_path   = os.path.join(CRAWLERS_PATH, path)
        cmd         = 'cat %s' %os.path.join(CRAWLERS_PATH, path)
        result, stdout, stderr = run_remote_cmd(ip='10.4.15.132', username='root', password='oneoneone', command=cmd)
        if stdout:
            log_status = 1
            log_string += '<tr><td>%s</td><td>%s</td></tr>' %(stdout.strip(), file_path)
    log_string += '</table>'

    if string:
        text += '\n\n' + string[1].replace('\n', '').replace('TABLE class="table table-bordered table-hover"', 'table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"')
    if two_game_dates:
        text += '<br /><br /> No of auto racing games having more than one game a day:  %s'%str(list(two_game_dates))

    if status:
        text += '<br /><br /><b>Games that are scheduled and game_datetime < now()</b><br />'+  game_string

    if games_without_stadium:
        text += '<br /><br /><b>Event Without Stadium Ids</b><br />'+ events_without_stadium

    if dup_games_golf:
        text += '<br /><br /><b>Number of duplicate games for Golf</b><br />'+ golf_duplicates

    if duplicate_status:
        text += '<br /><br />Multiple games for a single game_id</b><br />'+ duplicate_string

    if empty_players:
        player_table = '<table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"> <tr><th>player_id</th><th>game</th><tr>'

        for empty_player in empty_players:
            player_table += '<tr><td>%s</td><td>%s</td></tr>' %(empty_player[0], empty_player[1])
        player_table += '</table>'
        text += '<br /><br /><b>Players having empty titles</b><br />'+  game_string

    if player_data:
       text += '<br /><br /><b>Total number of active rosters participating in more that one team</b><br /><table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr><th>count</th></tr><tr><td>%s</td></tr></table>'%len(player_data)

    if camping_tou[0][0] != 0:
        text += '<br /><br /><b>Total number of Camping world truck games not having correct tournament_id</b><br /><table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr><th>count</th></tr><tr><td>%s</td></tr></table>'%camping_tou[0][0]

    if autoracing[0][0] != 0:
        text += '<br /><br /><b>Total number of Nationwide and Sprint cup series games not having event_id</b><br /><table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr><th>count</th></tr><tr><td>%s</td></tr></table>'%autoracing[0][0]

    if active_null[0][0] != 0:
       text += '<br /><br /><b>Active roster players having null/default images</b><br /><table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr><th>count</th></tr><tr><td>%s</td></tr></table>'%active_null[0][0]

    if stad_amp[0] != 0:
       text += '<br /><br /><b>Count of stadium titles having "&amp;" with "amp;" in sports_stadiums</b><br /><table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr><th>count</th></tr><tr><td>%s</td></tr></table>'%stad_amp[0]
    if stad_com[0] != 0:
       text += '<br /><br /><b>Count of stadium titles having "," in sports_stadiums</b><br /><table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr><th>count</th></tr><tr><td>%s</td></tr></table>'%stad_com[0]
    if null_game_participants[0][0] != 0:
       text += '<br /><br /><b>Participants present in sports_games_participants with null/empty image</b><br /><table border="1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr><th>count</th></tr><tr><td>%s</td></tr></table>'%null_game_participants[0][0]
    if log_status:
        text += '<br /><br /><b>Handler log errors</b><br />'+  log_string
    if string or two_game_dates or status or player_data or empty_players:
        send_mail(to, subject, text, sender, [fname])
main()
