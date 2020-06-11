import MySQLdb
import sys 
from optparse import OptionParser


types_query = "insert into sports_types_popularity(sport_id, location, base_popularity, created_at, modified_at) values('%s', '%s', '%s', now(), now()) on duplicate key update modified_at = now()"


class TypesPopularity:

    def __init__(self):
        #self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def main(self, options):
        self.sport_id       = options.sport_id.strip()
        self.location       = options.location.strip()
        self.base_popularity  = options.base_popularity.strip()
        if not (self.sport_id and self.location and self.base_popularity):
            print 'Please sport_id, location and base_popularity'
            sys.exit(1)
        if "," in self.location:
            for data in self.location.split(','):
                location = data
                values = (self.sport_id, location, self.base_popularity)
                self.cursor.execute(types_query % values)
        else:
            values = (self.sport_id, self.location, self.base_popularity)
            self.cursor.execute(types_query % values)


       

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-s', '--sport_id',        default='', help='Sport ID')
    parser.add_option('-l', '--location',        default='', help='Location ISO Code')
    parser.add_option('-b', '--base_popularity', default='', help='Base Popularity')
    (options, args) = parser.parse_args()
    OBJ = TypesPopularity()
    OBJ.main(options)

