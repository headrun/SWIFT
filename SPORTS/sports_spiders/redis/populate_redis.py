import MySQLdb
import redis

SOURCE_KEYS = "select source_key, entity_type, source, entity_id from sports_source_keys"

#GAMES = "select id, gid, game_datetime, game, status from sports_games where status = 'Hole'"
#TOURNAMENTS = "select id, gid, title, type from sports_tournaments"
#PARTICIPANTS = "select id, gid, title, game, participant_type from sports_participants"


class SportsRedis(object):
    def __init__(self):
        self.redis_con   = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.sports_conn = MySQLdb.connect(host='10.28.218.81', user='veveo', db='SPORTSDB', passwd="veveo123")
        self.cursor      = self.sports_conn.cursor()
        self.redis_con.flushall()

    def get_bulk_results(self, query):
        self.cursor.execute(query)
        for d in self.cursor.fetchall():
            yield d

    def get_result(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def populate_source_keys(self):
        for result in self.get_bulk_results(SOURCE_KEYS):
            unique = ':'.join([str(res) for res in result[:-1]])
            print unique,result[-1]
            self.redis_con.set(unique, result[-1])

    '''
    def populate_redis(self):
        queries = { PARTICIPANTS: 'participant', TOURNAMENTS: '' }
        for key, value in queries.iteritems():
            for result in self.get_bulk_results(key):
                if value:
                    query = GAMES_SOURCE + ' and entity_type="%s"'
                    query = query %(result[0], value)
                else:
                    query = GAMES_SOURCE % result[0]
                self.cursor.execute(query)
                data = self.cursor.fetchall()
                if not data:
                    continue
    '''

    def clean(self):
        self.sports_conn.close()

if __name__=="__main__":
    obj = SportsRedis()
    obj.populate_source_keys()
    obj.clean()
