import MySQLdb


class Types:

    def __init__(self):
        self.conn = MySQLdb.connect(db='SPORTSDB', host='10.4.18.183', user='root')
        self.cursor = self.conn.cursor()

        self.types_dict = {}

    def main(self):
        query = 'select id, title from sports_types'
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            type_id, game_type = row
            self.types_dict[game_type.lower()] = type_id
        for table in ['leagues']:#['games', 'participants', 'tournaments']:
            query = 'select distinct game from sports_%s'
            self.cursor.execute(query% table)
            rows = self.cursor.fetchall()
            for row in rows:
                game = row[0]
                type_id = self.types_dict.get(game, '')
                if type_id:
                    query = 'update sports_%s set sport_id="%s" where game="%s"'
                    values = (table, type_id, game)
                    self.cursor.execute(query % values)
                else:
                    print game

if __name__ == '__main__':
    OBJ = Types()
    OBJ.main()
