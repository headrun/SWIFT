import MySQLdb
from optparse import OptionParser

class StadiumAdd:

    def __init__(self):
        self.conn = MySQLdb.connect(db='SPORTSDB', host='10.4.18.183', user='root')
        self.cursor = self.conn.cursor()

    def main(self):
        stadium = options.name.strip().replace('-', ' ')
        team_id = options.team_id.strip()
        loc_id = options.loc_id.strip()
        query = "select auto_increment from information_schema.TABLES where TABLE_NAME='sports_stadiums' and TABLE_SCHEMA='SPORTSDB'"
        self.cursor.execute(query)
        count = self.cursor.fetchone()
        stadium_gid = 'STAD%s' % str(count[0])

        query = 'insert into sports_stadiums (id, gid, title, location_id, created_at, modified_at) values(%s, %s, %s, %s, now(), now())'
        values = (count[0], stadium_gid, stadium, loc_id)
        self.cursor.execute(query, values)

        query = 'update sports_teams set stadium_id=%s where participant_id=%s limit 1'
        values = (count[0], team_id)
        self.cursor.execute(query, values)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-n', '--name',     default='', help='Location ID')
    parser.add_option('-l', '--loc_id',  default='', help='Search ID')
    parser.add_option('-t', '--team_id', default='', help='Replace ID')
    (options, args) = parser.parse_args()

    OBJ = StadiumAdd()
    OBJ.main()
