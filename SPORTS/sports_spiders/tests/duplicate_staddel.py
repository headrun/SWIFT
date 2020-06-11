from vtv_task import VtvTask, vtv_task_main
import sys 
import MySQLdb

class DuplicateStadiumDel(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.file_name = open('stadiums_list.txt', 'r')
        

    def duplicatestadlist(self):
        st_id = self.file_name
        for st_deta in st_id:
            st_ids = st_deta.split(',')
            if len(st_ids) !=4:
                continue
            original_id = st_ids[0].strip()
            oroginal_title = st_ids[1].strip()
            dup_id = st_ids[2].strip()
            dup_title = str(st_ids[3]).strip()
            print oroginal_title
            print dup_title
            del_qry = 'delete from sports_stadiums where id = %s limit 1'
            self.cursor.execute(del_qry %(dup_id))
            up_qry = 'update sports_stadiums set aka=%s where id=%s limit 1'
            values = (dup_title, original_id)
            self.cursor.execute(up_qry, values)
            up_tm_qry = "update sports_teams set stadium_id=%s where stadium_id=%s"
            self.cursor.execute(up_tm_qry %(original_id, dup_id))
            up_game_qry = "update sports_games set stadium_id=%s where stadium_id=%s"
            self.cursor.execute(up_game_qry %(original_id, dup_id))
            up_tou_qry = "update sports_tournaments set stadium_ids=%s where stadium_ids=%s"
            self.cursor.execute(up_game_qry %(original_id, dup_id))



    def run_main(self):
        self.duplicatestadlist()




if __name__ == '__main__':
    vtv_task_main(DuplicateStadiumDel)


