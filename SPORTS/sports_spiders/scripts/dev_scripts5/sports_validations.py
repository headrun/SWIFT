#!/usr/bin/env python

################################################################################
#$Id: sports_validations.py,v 1.1 2016/03/23 07:12:32 headrun Exp $
################################################################################

from datetime import datetime
import MySQLdb
import pickle
import os
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2
import sys, traceback


ALLOWED_TITLES = ['Round', 'final']
GOLF_GAMES = ["PGAOngoing"]
MOTO_GP = ["MotogpSpider"]
NFL_GAMES = ['NFLSpider']

def get_html_table(title, headers, table_body):
    table_data = '<br /><br /><b>%s</b><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
    for header in headers:
        table_data += '<th>%s</th>' % header
    table_data += '</tr>'

    for data in table_body:
        table_data += '<tr>'
        for row in data:
            table_data += '<td>%s</td>' % str(row)
        table_data += '</tr>'
    table_data += '</table>'

    return table_data

def check_file_access(fname):
    existance = False
    if os.path.isfile(fname):
        existance = True
    return existance

class Validator:
    def __init__(self, stats_file):
        self.stats_dir     = 'SPORTS_STATS_DIR'
        self.stats_file    = stats_file
        self.pickle_file   = os.path.join(self.stats_dir, "%s_gids.pickle" % self.stats_file)
        self.today         = datetime.now().date().strftime("%Y-%m-%d")
        self.conn          = MySQLdb.connect(host="10.4.15.132", db="SPORTSDB_BKP", user="root")
        self.cursor        = self.conn.cursor()
        self.logger        = initialize_timed_rotating_logger('sports_validator.log')
        self.event_titles  = {}
        self.removed_games = []
        self.tbd_games     = []
        self.text          = ''
        self.updated_gids  = None
        self.inserted_gids = None

        existance = check_file_access(self.pickle_file)
        if existance:
            self.run_main()

    def send_mail(self, source):
        subject    = 'Gids to be marked as Hole for %s' % source
        server     = '10.4.1.112'
        sender     = 'bibeejan@headrun.com'
        receivers  = ['bibeejan@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', self.text, '')

    def get_one_result(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_all_results(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_tou_events(self, tournament, games_count=0):
        query = 'select event_id from sports_tournaments_events where tournament_id = %s'
        events = self.get_all_results(query % tournament)
        events = [e[0] for e in events]
        if len(events) == 1:
            tou_id = str(events[0])
        elif len(events) >= 1:
            tou_id = ', '.join(map(str, events))
        else:
            return

        query = 'select id, title from sports_tournaments where id in (%s)' % tou_id
        title_id = self.get_all_results(query)
        for record in title_id:
            tou_id, title = record
            for titles in ALLOWED_TITLES:
                if titles.lower() in title.lower():
                    games_count = games_count / 2
                    self.event_titles[tou_id] = (title, games_count)
            self.get_tou_events(tou_id, games_count)


    def check_grandslam(self):
        tournament_domain = self.stats_file
        if "UsopenTennis" in self.stats_file:
            tournament_domain = "U.S. Open"
        query = 'select id, title from sports_tournaments where title like "%%%s%%" and type = "tournament" and game = "tennis"'
        tournaments = self.get_all_results(query % tournament_domain)
        if not tournaments or len(tournaments) > 1:
            return

        self.get_tou_events(tournaments[0][0], games_count=128)

        dup_game   = set()
        hole_games = set()

        tbd_check = False
        for event_id, event_count in self.event_titles.iteritems():
            query = 'select id, created_at from sports_games where event_id = %s and game_datetime >= (curdate() - interval 70 day) and status not in ("Hole", "postponed")'
            event_games = self.get_all_results(query % str(event_id))
            if not event_games:
                continue
            event_games = {key: value for (key, value) in event_games}
            query = 'select id from sports_participants where title = "TBD"'
            tbd_ids = self.get_all_results(query)
            tbd_ids = [tbd[0] for tbd in tbd_ids]
            query = 'select game_id, participant_id from sports_games_participants where game_id in (%s) and participant_id not in (%s)'
            event_participants = self.get_all_results(query % (','.join(map(str, event_games.keys())), ','.join(map(str, tbd_ids))))

            query = 'select count(id) from sports_games where event_id = %s and game_datetime >= (curdate() - interval 70 day) and status not in ("Hole", "postponed", "cancelled")'
            event_tbds = self.get_all_results(query % str(event_id))

            if int(event_tbds[0][0]) > 1:
                query = 'select game_id, participant_id from sports_games_participants where game_id in (%s) and participant_id in (%s)'
                event_tbd_participants = self.get_all_results(query % (','.join(map(str, event_games.keys())), ','.join(map(str, tbd_ids))))
                if event_tbd_participants:
                    tbd_id = event_tbd_participants[0][0]
                    query = 'update sports_games set status = "Hole" where id in (%s)'  % tbd_id
                    self.cursor.execute(query)
                    tbd_check = True
                    self.tbd_games.append(str(tbd_id))

            participants = {}
            for participant in event_participants:
                game_id, pt_id = participant
                participants.setdefault(pt_id, []).append(game_id)

            for pt_id, pt_games in participants.iteritems():
                if len(pt_games) > 1:
                    game_dates = {}
                    for pt_game in pt_games:
                        game_dates[event_games[pt_game]] = pt_game
                    max_game_date = max(game_dates.keys())
                    del game_dates[max_game_date]
                    for key, value in game_dates.iteritems():
                        hole_games.add(value)
                    self.removed_games.extend(game_dates.values())
                    dup_game.add((event_count[0], tuple(pt_games)))

        if hole_games:
            query = 'update sports_games set status = "Hole" where id in (%s)' % ', '.join(map(str, hole_games))
            self.cursor.execute(query)

        if self.removed_games:
            self.text += '<font size="2" color="red">Gids marked as Hole. Please verify on priority</font><br />'
            self.text += str(self.removed_games)
            self.text += '<br />'
        if self.tbd_games:
            self.text += '<font size="2" color="red">Gids marked as Hole For TBD Games. Please verify on priority</font><br />'
            self.text += str(self.tbd_games)
            self.text += '<br />'

        if dup_game or tbd_check:
            headers = ('Event Name', 'Duplicate Gids')
            self.text += get_html_table('%s Duplicate games' % self.stats_file, headers, dup_game)
            self.send_mail(self.stats_file)

    def default_check(self):
        pickle_data = pickle.load(open(self.pickle_file))
        self.updated_gids = pickle_data.get('updated_%s' % self.today, [])
        self.inserted_gids = pickle_data.get('inserted_%s' % self.today, [])

        total_gids = list(self.updated_gids) + list(self.inserted_gids)
        min_id = min(total_gids) if total_gids else ''
        max_id = max(total_gids) if total_gids else ''

        if not min_id:
            return
        query = 'select source from sports_source_keys where entity_id = %s' % min_id
        selected_source = self.get_one_result(query)
        date_time = 'select game_datetime from sports_games where id = %s'
        query = date_time % max_id
        max_time = self.get_one_result(query)
        query = date_time % min_id
        min_time = self.get_one_result(query)

        query = "select id from sports_games where id in (select entity_id from\
            sports_source_keys where source = %s and entity_type = 'game') and status != 'Hole'\
            and game_datetime <= %s and game_datetime >= %s"
        values = (selected_source, max_time, min_time)
        got_gids = self.get_all_results(query % values)
        collected_gids = [gid[0] for gid in got_gids]
        missing_gids = set(collected_gids) - set(total_gids)
        self.text += ', '.join(map(str, missing_gids))
        if self.text:
            self.send_mail(selected_source)

    def check_golf(self):

        query = "select distinct(tournament_id), count(*) from sports_games \
                where status in ('scheduled', 'completed') and \
                year(game_datetime) = year(now()) and game = 'golf' and \
                tournament_id != 529 group by tournament_id having count(*) > 1"

        duplicate_ids = self.get_all_results(query)

        if duplicate_ids:
            headers = ('Tournament Id', 'Gids_Count')
            self.text += get_html_table('%s Duplicate games' % self.stats_file, headers, duplicate_ids)
            self.send_mail(self.stats_file)

    def check_hole_games(self):
        query = "select distinct(event_id), count(*), min(created_at), from sports_games \
                where status in ('scheduled', 'completed') and \
                year(game_datetime) = year(now()) and game in ('football', 'hockey') and \
                event_id in (319, 320, 1010, 900) group by event_id having count(*) > 1"
        duplicate_ids = self.get_all_results(query)
        if duplicate_ids:
            headers = ('Event Id', 'Gids_Count', 'Min(Date)')
            self.text += get_html_table('%s Duplicate games' % self.stats_file, headers, duplicate_ids)
            self.send_mail(self.stats_file)
        for duplicate_ids in duplicate_id:
            query = 'update sports_games set status ="Hole" where event_id=%s and created_at= %s'
            values = (str(duplicate_id[0]), str(duplicate_id[2]))
            self.get_all_results(query, values)

    def check_nhl_nfl_mlb(self):
        if "NFLSpider" in self.stats_file:
            src = ["NFL"]
            game = "football"
        elif "NHLSpider" in self.stats_file:
            src = ["NHL"]
            game = "hockey"
        elif "MLBSpider" in self.stats_file:
            src = ["MLB"]
            game = "baseball"
        elif "NCAASpider" in self.stats_file:
            src = ["ncaa_ncb", "ncaa_ncw"]
            game = "basketball"

        game_ids = set()
        for source in src:
            query = "select entity_id from sports_source_keys where \
                    source='%s' and entity_type='game'" % (source)
            entity_ids = self.get_all_results(query)
            for entity_id in entity_ids:
                game_id = str(entity_id[0])
                query = 'select count(id) from sports_games where id=%s and game="%s" and status="scheduled"'
                values = (game_id, game)
                res_query = query % values
                res = self.get_one_result(res_query)
                if res:
                    query = "select participant_id from sports_games_participants \
                             where game_id=%s" % (game_id)
                    participant_ids = self.get_all_results(query)

                    for p_id in participant_ids:
                        query = "select title from sports_participants \
                                 where id = %s" % (p_id)
                        title = self.get_one_result(query)
                        if "TBD" in title:
                            query = "select event_id from sports_games where id=%s and game_datetime < now()" % (game_id)
                            res = self.get_all_results(query)
                            if res:
                                game_ids.add((res, game_id))

                if game_ids:
                    headers = ('Event Id', 'Game_id')
                    self.text += get_html_table('%s TBD games' % self.stats_file, headers, game_ids)
                    self.send_mail(self.stats_file)

    def check_motogp(self):
        query = "select distinct(tournament_id), count(*) from sports_games \
                where status in ('scheduled', 'completed') and \
                year(game_datetime) > year(now()) and game = 'motorcycle racing' \
                group by tournament_id having count(*) > 3"

        duplicate_ids = self.get_all_results(query)
        if duplicate_ids:
            headers = ('Tournament Id', 'Gids_Count')
            self.text += get_html_table('%s Duplicate games' % self.stats_file, headers, duplicate_ids)
            self.send_mail(self.stats_file)

        query = ""

    def run_main(self):

        if 'Wimbledon' in self.stats_file or 'UsopenTennis' in self.stats_file :
            self.check_grandslam()
        elif self.stats_file in GOLF_GAMES:
            self.check_golf()
        elif "NFLSpider" in self.stats_file:
            self.check_nhl_nfl_mlb()
        elif "NHLSpider" in self.stats_file:
            self.check_nhl_nfl_mlb()
        elif "MLBSpider" in self.stats_file:
            self.check_nhl_nfl_mlb()
        elif self.stats_file in MOTO_GP:
            self.check_motogp()
        elif 'NFLSpider' in self.stats_file or 'NHLSpider' in self.stats_file :
            self.check_hole_games()
        else:
            self.default_check()

if __name__ == "__main__":
    try:
        PICKLE_FILE = sys.argv[1]
        Validator(PICKLE_FILE)
    except:
        print "Stats file needed!"
        traceback.print_exc()
