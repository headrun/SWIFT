import MySQLdb


class Founded:

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDBBKP")
        self.cursor = self.conn.cursor()

        self.m_conn = MySQLdb.connect(host="10.4.2.187", user="root", db="GUIDMERGE")
        self.m_cursor = self.m_conn.cursor()

        self.all_ids = []
        self.merge_dict = {}

    def get_team_list(self):
        query = 'select participant_id from sports_teams where formed like "%0000-00-00%"'
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        for record in records:
            record = str(record[0])
            self.all_ids.append(record)

    def get_merge_list(self):
        query = 'select exposed_gid, child_gid from sports_wiki_merge'
        self.m_cursor.execute(query)
        records = self.m_cursor.fetchall()
        for record in records:
            exposed_gid, child_gid = record
            if 'TEAM' in child_gid:
                self.merge_dict[exposed_gid] = child_gid


    def main(self):
        self.get_team_list()
        self.get_merge_list()
        rows = open('sports_package_data1', 'r')
        for row in rows:
            wiki_gid, founded = row.strip().split('<>') 
            if self.merge_dict.get(wiki_gid, ''):
                if len(founded) == 4:
                    founded = '%s-01-01' % founded
                elif ' ' in founded:
                    continue
                gid = self.merge_dict.get(wiki_gid, '')
                query = 'select id from sports_participants where gid=%s'
                self.cursor.execute(query, gid)
                data = self.cursor.fetchone()
                if data:
                    data = str(data[0])
                else:
                    print row
                    continue
                if data and data in self.all_ids:
                    query = 'update sports_teams set formed=%s where participant_id=%s'
                    values = (founded, data)
                    self.cursor.execute(query, values)
                else:
                    print data
    
if __name__ == '__main__':
    OBJ = Founded()
    OBJ.main()
