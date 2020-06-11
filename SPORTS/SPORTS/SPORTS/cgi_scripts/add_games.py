#!/usr/bin/python
import cgitb
cgitb.enable()
import cgi
from datetime import datetime
import MySQLdb

ALERT   = '<script>alert("%s");</script>'
END_TAG = '</body></html>'

GAME_FIELDS = ( 'id', 'gid', 'game_datetime', 'sport_id', 'game_note',
                'tournament_id', 'status', 'reference_url',
                'event_id', 'created_at', 'modified_at' )

SK_FIELDS = ('id', 'entity_id', 'entity_type', 'source',
             'source_key', 'created_at', 'modified_at')

GAME_PTS = ('id', 'game_id', 'participant_id', 'is_home',
                  'group_number', 'created_at', 'modified_at')

SOURCE_KEY = 'select entity_id from sports_source_keys where entity_type="participant" and source="%s" and source_key="%s"'

TOU_ID = 'select id from sports_tournaments where sport_id = "%s" and title = "%s"'

INSERT_PTS = "insert into sports_games_participants (game_id, participant_id, is_home, group_number, created_at, modified_at) values(%s, %s, %s, %s, now(), now())"

PTS = "select * from sports_games_participants where game_id=%s"

STADIUM = 'select id from sports_stadiums where title = "%s"'

CHK_SOURCE_KEY = 'select id, entity_id, entity_type, source, source_key, created_at, modified_at from sports_source_keys where entity_type="game" and source_key="%s" and source="%s"'

GAME_MAX_ID = 'select id, gid from sports_games where id in (select max(id) from sports_games)'

INSERT_SOURCE_KEY = 'insert into sports_source_keys (entity_id, entity_type, source, source_key, created_at, modified_at) values("%s", "%s", "%s", "%s", now(), now())'

INSERT_GAMES = "insert into sports_games (id, gid, game_datetime, sport_id, game_note, tournament_id, status, reference_url, location_id, stadium_id, event_id, time_unknown, created_at, modified_at, tzinfo, channels, radio) values(%s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', now(), now(), '%s', '', '')"

GAME = "select * from sports_games where id = %s"

GET_SOURCE_KEY = 'select * from sports_source_keys where entity_id="%s" and source="%s" and source_key="%s" and entity_type = "game"'

def prepare_result_table(heading, data, table_fields):
    print heading
    if len(data) == 1:
        res = '<table border="1">'
        for i in range(len(table_fields)):
            if data[0][i]:
                value = data[0][i]
            else:
                value = '-'
            res += '<tr><td>%s</td><td>%s</td></tr>' % (table_fields[i], value)
        res += '</table>'

    elif len(data) > 1:
        res = '<table border="1"><thead>'
        for field in table_fields:
            res += '<th>%s</th>' % field
        res += '</thead>'
        for record in data:
            res += '<tr>'
            for key in record:
                res += '<td>%s</td>' % key
            res += '</tr>'
        res += '</table>'
    print res

def line_break(count=1):
    for i in range(count):
        print '<br />'

def display_input(values):
    input_pattern = '<tr><td>%s:</td><td><input type="text", \
                     size=50, name="%s", value="%s"></input><br/></td></tr>'
    for value in values:
        if isinstance(value, list) or isinstance(value, tuple):
            print input_pattern % value
        else:
            print input_pattern % values
            break

class Games:
    def __init__(self, field_storage):
        self.field_storage = field_storage
        self.host          = '10.28.218.81'
        self.user          = 'root'
        self.database      = 'SPORTSDB'
        self.participants_ids = {}
        self.conn = MySQLdb.connect(host=self.host,
                                    user = self.user,
                                    db = self.database)
        self.conn.set_character_set('utf8')
        self.cursor = self.conn.cursor()

    def execute_cursor(self, query, get_result=False):
        self.cursor.execute(query)
        if get_result:
            return self.cursor.fetchall()


    def get_tournament_id(self, tou_name, sport_id):
        values = (sport_id, tou_name)
        tou_id = self.execute_cursor(TOU_ID % values, True)
        tournament = None
        if tou_id:
            if tou_id:
                tournament = tou_id[0][0]
        else:
            print "</br>No tournament exist in DB: %s</br>" % tou_name
            print
        return tournament


    def check_participants(self, src, participants):
        #print checking participants
        for participant in participants:
            values = (src, participant)
            entity_id = self.execute_cursor(SOURCE_KEY % values, True)
            if len(entity_id) == 1:
                self.participants_ids.update({participant: entity_id})
            elif len(entity_id) > 1:
                print ALERT % ("Duplicate player exist, For: %s" % participant)
            else:
                print ALERT % "No records available, Add player."

    def insert_participants(self, participants, next_id):
        for participant in participants:
            for key, value in self.participants_ids.iteritems():
                if key in participant:
                    part_id = value[0][0]
            values = (next_id, part_id, '0', '0')
            self.execute_cursor(INSERT_PTS % values)
        pt_data = self.execute_cursor(PTS % next_id, True)

        prepare_result_table("Games Participants Table Data :", pt_data, GAME_PTS)
        line_break()

    def get_stadium_id(self, stadium):
        stadium_id = self.execute_cursor(STADIUM % stadium, True)
        if stadium_id:
            sta_id = stadium_id[0][0]
            return sta_id

    def validate_date(self, game_datetime):
        if game_datetime:
            try:
                game_datetime = datetime.strptime(game_datetime, '%Y-%m-%d %H:%M')
            except ValueError:
                print "Game datetime format should be like Y-m-d H:M"
                return

    def run(self):
        val = self.field_storage.getfirst('action', '')
        #Game table details
        sport_id = self.field_storage.getfirst('sport_id', '')
        game_datetime = self.field_storage.getfirst('game_datetime', '')
        game_note = self.field_storage.getfirst('game_note', '')
        tournament_name = self.field_storage.getfirst('tournament_name', '')
        event_name = self.field_storage.getfirst('event_name', '')
        status = self.field_storage.getfirst('status', '')
        ref_url = self.field_storage.getfirst('ref_url', '')
        location = self.field_storage.getfirst('location', '')
        stadium = self.field_storage.getfirst('stadium', '')
        participants = self.field_storage.getfirst('participants', '')
        time_unknown = self.field_storage.getfirst('time_unknown', '')
        tzinfo = self.field_storage.getfirst('tzinfo', '')

        #Source key table details
        src = self.field_storage.getfirst('src', '')
        source_key = self.field_storage.getfirst('sk', '')

        print "Content-Type: text/html"
        print

        print '<html>'
        print '<head></head>'
        print '<center><h2><b>GAME ADDITION</b></h2></center><br/>'
        print '<body>'
        print '<form><table>'
        print '<p><b>SOURCEKEYS TABLE</b></p>'
        data = (('Source Name', 'src', src), ('Source Key', 'sk', source_key))
        display_input(data)

        print '<tr><td><br/></td></tr>'
        print '<tr><td><b>GAME TABLE</b></td></tr>'
        print '<tr><td><br/></td></tr>'

        data = (('Sport id', 'sport_id', sport_id), ('Game Note', 'game_note', game_note),
                ('Game Datetime', 'game_datetime', game_datetime),
                ('Game Status', 'status', status),
                ('Patricipants seperated by Comma(,)', 'participants', participants),
                ('Tournament Name', 'tournament_name', tournament_name),
                ('Event Name', 'event_name', event_name),
                ('Reference Url', 'ref_url', ref_url),
                ('Location', 'location', location), ('Stadium', 'stadium', stadium),
                ('Time Unknown', 'time_unknown', time_unknown),
                ('Tzinfo', 'tzinfo', tzinfo))
        display_input(data)

        print '<tr><td><br/></td></tr>'
        print '<tr><td></br><input type="submit", name="action" value="CONFIRM"><br/></td></tr>'
        print '</table>'

        game_cond = (not sport_id or not game_datetime)
        sk_cond = (not src or not source_key)
        pts_cond = (not participants)
        tou_cond = (not tournament_name)

        if sk_cond and val == 'CONFIRM':
            print ALERT % "Please enter full source_keys data."
            print END_TAG
            return

        if status:
            if status == "scheduled":
                print "Game status is: %s"  % status
                line_break()
            else:
                print "Game status Should be :",  status
                return

        if game_cond  and  val == 'CONFIRM':
            print ALERT % "Please enter sport_id name and game datetime details"
            print END_TAG
            return

        self.validate_date(game_datetime)

        if tou_cond and val == 'CONFIRM':
            print "Enter Tournamnet Id"
        elif tournament_name:
            tou_id = self.get_tournament_id(tournament_name, sport_id)

        event_id = sta_id = ''
        if event_name:
            event_id = self.get_tournament_id(event_name, sport_id)

        if stadium:
            sta_id = self.get_stadium_id(stadium)
        if not sta_id:
            sta_id = stadium

        if pts_cond and val == 'CONFIRM':
            print "Enter participants"
        elif participants:
            participant = participants.split(',')
            self.check_participants(src, participant)

        if val == 'CONFIRM':
            db_data = self.execute_cursor(CHK_SOURCE_KEY % (source_key, src), True)
            if db_data:
                error = "DUPLICATE RECORDS FOUND.SEE BELOW DATA"
                print ALERT % error
                print END_TAG
                prepare_result_table(error, db_data, SK_FIELDS)
                line_break(2)
            else:
                print ALERT % "Please verify once before you submit data"
                line_break()
                print 'Game Name: %s\n' % sport_id
                line_break()
                print 'Game Datetime: %s\n' % game_datetime
                print 'Participants: \n' , self.participants_ids
                line_break(2)

                print '<input type="submit" name="action" value="submit"><br/><br/>'

        if val == 'submit':
            game_data = self.execute_cursor(GAME_MAX_ID, True)
            max_id, max_gid = game_data[0]
            next_id = max_id + 1
            next_gid = 'EV' + str(int(max_gid.replace('EV', '')) + 1)

            values1 = (next_id, 'game', src, source_key)

            values2 = (next_id, next_gid, game_datetime, sport_id, game_note,
                       tou_id, status, ref_url, location, sta_id, event_id, time_unknown, tzinfo)
            print  INSERT_GAMES % values2
            self.execute_cursor(INSERT_GAMES % values2)
            if participants:
                participant = participants.split(',')
                self.insert_participants(participant, next_id)
            self.execute_cursor(INSERT_SOURCE_KEY % values1)

            game_data = self.execute_cursor(GAME % next_id, True)

            values = (next_id, src, source_key)
            sk_data = self.execute_cursor(GET_SOURCE_KEY % values, True)

            prepare_result_table('Game Table Data :', game_data, GAME_FIELDS)
            line_break()
            prepare_result_table('Source Key Table Data :', sk_data, SK_FIELDS)
            print '</form></body></html>'


if __name__ == '__main__':
    GAMES = Games(cgi.FieldStorage())
    GAMES.run()
