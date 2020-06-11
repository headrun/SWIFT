import MySQLdb

QUERY = 'INSERT IGNORE INTO sports_wiki_merge \
(wiki_gid, sports_gid, action, modified_date)\
values(%s, %s, %s, now())'

class PopulateMerge:

    def __init__(self):
        self.dates_dict = {}
        self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
        self.cursor = self.conn.cursor()
        self.text   = ''
        self.file_ = open('/home/veveo/reports/Active_players_stats.html', 'w')
        self.file_one = open('active_players_birth_place', 'w')

    def get_html_table(self, title, headers, table_body):
        table_data = '<br /><br /><b>%s</b><br /><table border="1" \
                        style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
        for header in headers:
            table_data += '<th>%s</th>' % header
        table_data += '</tr>'

        for data in table_body:
            table_data += '<tr>'
            for index, row in enumerate(data):
                table_data += '<td>%s</td>' % (str(row))

            table_data += '</tr>'
        table_data += '</table>'

        return table_data
 

    def main(self):
        query = 'select participant_id from sports_teams where tournament_id in (88)'#, 229, 197, 240, 35, 562)'
        self.cursor.execute(query)
        teams = self.cursor.fetchall()
        for team in teams:
            query = 'select title from sports_participants where id=%s' % team
            self.cursor.execute(query)
            team_title = self.cursor.fetchone()[0]
            query = 'select player_id from sports_roster where team_id=%s and status="active"'
            self.cursor.execute(query % team)
            players = self.cursor.fetchall()
            for player in players:
                query = 'select sp.id, title, participant_type, birth_date, birth_place, main_role from sports_participants sp, sports_players pl where sp.id=pl.participant_id and sp.id=%s'
                self.cursor.execute(query % player)
                data = self.cursor.fetchall()
                try:
                    data = data[0]
                except:
                    print data
                    continue
                    #import pdb; pdb.set_trace()
                pl_id, title, type_, birth_date, birth_place, roles = data
                self.file_one.write('%s\n' % '<>'.join([str(pl_id), title, type_, str(birth_date), birth_place, roles]))
                self.dates_dict.setdefault(team_title, []).append([pl_id, title, type_, str(birth_date), birth_place, roles])

        for key, value in self.dates_dict.iteritems():
            headers = ('Pl Id', 'Title', 'P.Type', 'Birth Date', 'Birth Place', 'Role')
            self.text += self.get_html_table(key, headers, value)
        self.file_.write('%s\n' % self.text)

if __name__ == '__main__':
    OBJ = PopulateMerge()
    OBJ.main()
