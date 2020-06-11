import MySQLdb

QUERY = 'INSERT IGNORE INTO sports_wiki_merge \
(wiki_gid, sports_gid, action, modified_date)\
values(%s, %s, %s, now())'

class PopulateMerge:

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()

        self.file_ = open('facup_display', 'w')

    def main(self):
        query = "select P.id, P.title from sports_teams T, sports_participants P, sports_tournaments ST where P.id = T.participant_id and T.tournament_id = ST.id and P.id in (select participant_id from sports_tournaments_participants where tournament_id=215)"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            team_id, title = row
            self.file_.write('%s<>%s\n' % (team_id, title))

if __name__ == '__main__':
    OBJ = PopulateMerge()
    OBJ.main()
