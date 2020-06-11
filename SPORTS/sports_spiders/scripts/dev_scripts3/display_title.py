import MySQLdb

QUERY = 'INSERT IGNORE INTO sports_wiki_merge \
(wiki_gid, sports_gid, action, modified_date)\
values(%s, %s, %s, now())'

class PopulateMerge:

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.4.2.187", user="root", db="AWARDS")
        self.cursor = self.conn.cursor()

        self.merge = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.m_cursor = self.merge.cursor()

    def main(self):
        query = 'select participants from sports_awards_history'
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        for record in records:
            pl_gid = record[0]
            query = 'select id from sports_participants where gid=%s'
            values = (str(pl_gid))
            self.m_cursor.execute(query, values)
            data = self.m_cursor.fetchone()
            if not data:
                print 'player missed>>>>', pl_gid


if __name__ == '__main__':
    OBJ = PopulateMerge()
    OBJ.main()
