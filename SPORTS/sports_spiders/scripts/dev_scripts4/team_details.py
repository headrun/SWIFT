import MySQLdb
import datetime

class PopulateMerge:

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def main(self):
        data = open('details', 'r')
        for record in data:
            record = [rec.strip() for rec in record.split('<>')]
            team_id, title, stadium_id, display_title, short_title, year, loc_id, std_loc_id = record
            if '/' in year:
                formed = str(datetime.datetime.strptime(year, '%m/%d/%Y'))
            else:
                formed = '%s-01-01' % year

            query = 'update sports_teams set stadium_id=%s, display_title=%s , short_title=%s where participant_id=%s'
            values = (stadium_id, display_title, short_title, team_id)
            self.cursor.execute(query, values)
            query = 'update sports_participants set location_id=%s where id=%s'
            values = (loc_id, team_id)
            self.cursor.execute(query, values)

            query = 'update sports_stadiums set location_id=%s where id= %s'
            values = (std_loc_id, stadium_id)
            self.cursor.execute(query, values)

            query = 'update sports_games set location_id=%s where stadium_id=%s'
            values = (std_loc_id, stadium_id)
            self.cursor.execute(query, values)


if __name__ == '__main__':
    OBJ = PopulateMerge()
    OBJ.main()
