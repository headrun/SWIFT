import MySQLdb

QUERY = 'INSERT IGNORE INTO sports_wiki_merge \
(exposed_gid, child_gid, action, modified_date)\
values(%s, %s, %s, now())'

class PopulateMerge:

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.4.2.187", user="root", db="GUIDMERGE")
        self.cursor = self.conn.cursor()

    def main(self):
        data = open('merge', 'r').readlines()
        import pdb; pdb.set_trace()
        for record in data:
            record = [dt for dt in record.strip().split('<>') if dt.strip()]
            if len(record) == 2:
                team_gid, wiki_gid = record
                if 'WIKI' not in wiki_gid:
                    wiki_gid = 'WIKI' + wiki_gid
                self.cursor.execute(QUERY, (wiki_gid, team_gid, 'override'))

                print 'populated'


if __name__ == '__main__':
    OBJ = PopulateMerge()
    OBJ.main()
