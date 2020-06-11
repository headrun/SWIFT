from vtv_task import VtvTask, vtv_task_main
import MySQLdb


class PopulateTouTest(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_ip = '10.28.218.81' #'10.4.2.187'
        self.db_name = 'GUIDMERGE'
        self.sports_db = '10.28.218.81'
        self.sports_dbname = 'SPORTSDB'
        self.tets_db = "DATATESTDB"
        self.test_ip = '10.28.218.81' #'10.4.2.187'
        self.stadiums_merge = {}
        self.testcases_list = []


    def check_stadium_mege(self):
        self.open_cursor(self.db_ip, self.db_name)
        gid_gry = 'select exposed_gid, child_gid from sports_wiki_merge where child_gid like "%TOU%"'
        self.cursor.execute(gid_gry)
        gids = self.cursor.fetchall()
        for gid in gids:
            wiki_gid, sports_gid = gid
            self.stadiums_merge[sports_gid] = wiki_gid

    def populate_testcases(self):
        suite_id = '37'
        self.conn = MySQLdb.connect(host="10.28.218.81", user="root", db="DATATESTDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        for row in self.testcases_list:
            gid = row[0]
            title = row[1]
            query = 'select id, record from test_cases where suite_id=%s and record like %s'
            val = '%' + row[0] + '#%'
            values = (suite_id, val)
            self.cursor.execute(query, values)
            data = self.cursor.fetchone()
            if data:
                test_id = data[0]
                up_qry = 'update test_cases set record=%s, suite_id="163" where id=%s'
                record = gid + "#<>#" +title + "#<>#phrase#<>#wikiconcept"
                test_values = (record, test_id)
                self.cursor.execute(up_qry, test_values)




    def toudetails(self):
        tou_query = 'select id, gid, title from sports_tournaments where affiliation ="obsolete"'
        self.cursor.execute(tou_query)
        data = self.cursor.fetchall()
        for data_ in data:
            tou_gid = data_[1] 
            tou_id = data_[0]
            title = data_[2]  
            wiki_gid = self.stadiums_merge.get(tou_gid, '')
            if not wiki_gid:
                wiki_gid = tou_gid
            values = [wiki_gid, title]
            self.testcases_list.append(values)
            
        
    def run_main(self):
        self.check_stadium_mege()
        self.open_cursor(self.sports_db, self.sports_dbname)
        self.toudetails()
        self.populate_testcases()




if __name__ == '__main__':
    vtv_task_main(PopulateTouTest)

