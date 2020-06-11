import MySQLdb

class MultipleGameid:

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", \
                passwd='veveo123', db="SPORTSDB", charset='utf8')
        self.cursor = self.conn.cursor()

    def main(self):
        sel_query = 'select distinct(entity_id) from sports_source_keys where entity_type = "game" \
                and source_key not like "%_Hole%" group by entity_id having count(*) > 1'
        self.cursor.execute(sel_query)
        data = self.cursor.fetchall()
        for data_ in data:
            game_id = data_[0] 
            sk_query = 'select id, source_key from sports_source_keys where entity_type = "game"  \
                    and source = "cycling" and source_key  like %s and entity_id=%s'
            #title = '%'+ 'Australian_Open' + '%'
            title = '%'+ 'Tour_Down_Under' + '%'
            values = (title, game_id)
            self.cursor.execute(sk_query, values)
            data = self.cursor.fetchall()
            if data:
                id_ = data[0][0]
                sk = data[0][1]
                up_qury = 'update sports_source_keys set source_key=%s where id = %s'
                sks = sk + "_Hole"
                values = (sks, id_)
                self.cursor.execute(up_qury, values)



if __name__ == '__main__':
    OBJ = MultipleGameid()
    OBJ.main()

