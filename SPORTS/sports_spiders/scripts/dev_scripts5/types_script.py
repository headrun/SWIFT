import MySQLdb

MAX_ID_QUERY = 'select id, gid from sports_types where id in \
                (select max(id) from sports_types)'

TYPE_QUERY = "insert into sports_types(id, gid, title, aka, keywords, \
             created_at, modified_at) \
             values (%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

class SportsTypes:

    def __init__(self):
        #self.conn    = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
        self.conn    = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor  = self.conn.cursor()

    def main(self):
        tou_games = 'select distinct(game) from sports_tournaments'
        self.cursor.execute(tou_games)
        data = self.cursor.fetchall()
        for game in data:
            game_ = game[0]
            game_query = 'select id from sports_types where title= "%s"'

            self.cursor.execute(game_query)
            game_data = self.cursor.fetchall()
            if game_data:
                continue
            else:
                self.cursor.execute(MAX_ID_QUERY)
                tm_data = self.cursor.fetchall()
                max_id, max_gid = tm_data[0]
                next_id = max_id + 1
                next_gid = 'SPORT' + str(int(max_gid.replace('SPORT', '')) + 1)

                values = (next_id, next_gid, game_, '', '')
                self.cursor.execute(TYPE_QUERY, values)



if __name__ == '__main__':
    OBJ = SportsTypes()
    OBJ.main()


