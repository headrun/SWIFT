import MySQLdb

class Titleregion:
    def __init__(self):
        #self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def main(self):
        sel_query = 'select id, gid from sports_titles_regions'
        self.cursor.execute(sel_query)
        data = self.cursor.fetchall()
        for data_ in data:
            id_ = str(data_[0])
            gid = str(data_[1])
            if "TEAM" in gid or "PL" in gid:
                par_query = 'select id, participant_type from sports_participants where gid=%s'
            if "TOU" in gid:
                par_query = 'select id, type from sports_tournaments where gid=%s' 
            values = (gid)
            self.cursor.execute(par_query, values)
            data = self.cursor.fetchone()
            if data:
                entity_id = data[0]
                enitity_type = data[1]
                up_query = 'update sports_titles_regions set entity_id=%s, entity_type=%s where id=%s'
                values = (entity_id, enitity_type, id_)
                self.cursor.execute(up_query, values)




if __name__ == '__main__':
    OBJ = Titleregion()
    OBJ.main()
