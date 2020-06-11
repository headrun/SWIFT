import MySQLdb

class AwardsSportid:

    def __init__(self):
        #self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.aw_conn = MySQLdb.connect(host="10.4.2.187", user="root", db="AWARDS", charset='utf8', use_unicode=True) 
        #self.aw_conn = MySQLdb.connect(host="10.4.18.34", user="root", db="AWARDS", charset='utf8', use_unicode=True)
        self.aw_cursor = self.aw_conn.cursor()

    def main(self):
        sel_query = 'select id, genre from sports_awards'
        #sel_query = 'select id, genre from sports_awards_category'
        self.cursor.execute(sel_query)
        data = self.cursor.fetchall()
        for data_ in data:
            aw_id = str(data_[0])
            aw_genre = str(data_[1])
            if aw_genre:
                aw_genre = aw_genre.split('{')[0].strip()
                sp_qury =  'select id from sports_types where title = %s'
                values = (aw_genre)
                self.cursor.execute(sp_qury, values)
                data = self.cursor.fetchone()
                if data:
                    sport_id = data[0]
            else:
                sport_id = '96'
                aw_genre = ''
            if sport_id:
                up_query = 'update sports_awards set sport_id=%s, genre=%s where id=%s'
                #up_query = 'update sports_awards_category set sport_id=%s, genre=%s where id=%s'
                values = (sport_id, aw_genre, aw_id)
                self.cursor.execute(up_query, values)

        #aw_re_query = 'select id, participants, genre from sports_awards_results'
        aw_re_query = 'select id, participants, genre from sports_awards_history'
        #self.cursor.execute(aw_re_query)
        self.aw_cursor.execute(aw_re_query)
        aw_data = self.aw_cursor.fetchall()
        for aw_dat in aw_data:
            aw_re_id = str(aw_dat[0]) 
            aw_par = str(aw_dat[1])
            genre = str(aw_dat[2])
            if genre:
                genre = genre.split('{')[0].strip()
            else:
                genre = ''
            game_quer = 'select game from sports_participants where gid=%s'
            values = (aw_par) 
            self.cursor.execute(game_quer, values)
            data = self.cursor.fetchone()
            if data:
                sport_name = str(data[0])
                sp_qury =  'select id from sports_types where title = %s'
                values = (sport_name)
                self.cursor.execute(sp_qury, values)
                data = self.cursor.fetchone()
                if data:
                    sport_id = str(data[0])
                    #up_query = 'update sports_awards_results set sport_id=%s, genre= %s where id=%s'
                    up_query = 'update sports_awards_history set sport_id=%s, genre= %s where id=%s'
                    values = (sport_id, genre, aw_re_id)
                    #self.cursor.execute(up_query, values)
                    self.aw_cursor.execute(up_query, values)
            

if __name__ == '__main__':
    OBJ = AwardsSportid()
    OBJ.main()



