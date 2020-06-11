import sys
import MySQLdb
from optparse import OptionParser

class Cities:

    def __init__(self):
        #self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()

    def main(self, options):
        loc_id = options.loc_id.strip()
        search_id = options.search_id.strip()
        replace_id = options.replace_id.strip()
        table_name = options.table_name.strip()

        if loc_id:
            query = 'select count(*) from sports_games where location_id="%s"'
            self.cursor.execute(query % loc_id)
            data = self.cursor.fetchone()
            if data and str(data[0]) != '0':
                print 'present in sports_games table>>>> ', loc_id

            query = 'select count(*) from sports_stadiums where location_id="%s"'
            self.cursor.execute(query % loc_id)
            data = self.cursor.fetchone()
            if data and str(data[0]) != '0':
                print 'present in sports_stadiums table>>>> ', loc_id

            query = 'select count(*) from sports_participants where location_id="%s"'
            self.cursor.execute(query % loc_id)
            data = self.cursor.fetchone()
            if data and str(data[0]) != '0':
                print 'present in sports_participants table>>>> ', loc_id

            query = 'select count(*) from sports_players where birth_place="%s"'
            self.cursor.execute(query % loc_id)
            data = self.cursor.fetchone()
            if data and str(data[0]) != '0':
                print 'present in sports_players table>>>> ', loc_id

            query = 'select count(*) from sports_tournaments where location_ids like "%s"'
            loc_id = loc_id + "%"
            self.cursor.execute(query % loc_id)
            data = self.cursor.fetchone()
            if data and str(data[0]) != '0':
                print 'present in sports_tournaments table>>>> ', loc_id

        if search_id and replace_id:
            if table_name == 'sports_players':
                update_query = "update %s set  birth_place='%s' where birth_place = '%s'" \
                %(table_name, replace_id, search_id)
                self.cursor.execute(update_query)

            elif table_name != 'sports_tournaments':
                update_query = "update %s set location_id='%s' where location_id='%s'"  \
                %(table_name, replace_id,search_id)
                self.cursor.execute(update_query)

        elif search_id and not replace_id:
            delete_query = 'delete from sports_locations where id ="%s" limit 1' %(search_id)
            self.cursor.execute(delete_query)


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-l', '--loc_id',     default='', help='Location ID')
    parser.add_option('-s', '--search_id',  default='', help='Search ID')
    parser.add_option('-r', '--replace_id', default='', help='Replace ID')
    parser.add_option('-t', '--table_name', default='', help='Table Name')
    (options, args) = parser.parse_args()
    OBJ = Cities()
    OBJ.main(options)

