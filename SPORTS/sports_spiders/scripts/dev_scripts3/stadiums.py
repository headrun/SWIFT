import MySQLdb

QUERY = 'INSERT IGNORE INTO sports_wiki_merge \
(wiki_gid, sports_gid, action, modified_date)\
values(%s, %s, %s, now())'

class PopulateMerge:

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.cursor = self.conn.cursor()

        self.merge = MySQLdb.connect(host="10.4.2.187", user="root", db="GUIDMERGE")
        self.m_cursor = self.merge.cursor()

        self.test = MySQLdb.connect(host="10.4.2.187", user="root", db="DATATESTDB")
        self.test_cursor = self.test.cursor()

        self.test_cases_dict = {}

    def main(self):
        stadiums = open('stadium_without_merge', 'r')
        for stadium in stadiums:
            record = [rec.strip() for rec in stadium.split('<>')]
            stad_gid, wiki_gid, title, type_, country, state, city = record
            wiki_gid = 'WIKI' + wiki_gid
            query = 'select exposed_gid from sports_wiki_merge where child_gid="%s"'
            self.m_cursor.execute(query % stad_gid)
            wiki_gid_db = self.m_cursor.fetchone()
            test_case = '#<>#'.join([wiki_gid, title, type_, country, state, city])
            key = stad_gid + '<>' + wiki_gid
            if wiki_gid_db and wiki_gid_db[0] == wiki_gid:
                self.test_cases_dict[key] = test_case
            elif not wiki_gid_db:
                query = 'insert into sports_wiki_merge (exposed_gid, child_gid, action, modified_date) values (%s, %s, %s, now())'
                values = (wiki_gid, stad_gid, 'override')
                self.m_cursor.execute(query, values)
                self.test_cases_dict[key] = test_case
            else:
                print record
        self.add_testcases()

    def add_testcases(self):
        query = 'select record from test_cases where suite_id=121'
        self.test_cursor.execute(query)
        records = self.test_cursor.fetchall()
        test_case_list = []
        for record in records:
            record = record[0]
            gid = record.split('#<>#')[0].strip()
            test_case_list.append(gid)

        import pdb; pdb.set_trace()
        for key, value in self.test_cases_dict.iteritems():
            stad_gid, wiki_gid = key.split('<>')
            if stad_gid in test_case_list:
                print value
            elif wiki_gid in test_case_list:
                print value
            else:
                query = 'insert into test_cases (suite_id, record, created_at, last_modified, flag) values(%s, %s, now(), now(), %s)'
                values = (121, value, 'active')
                self.test_cursor.execute(query, values)


if __name__ == '__main__':
    OBJ = PopulateMerge()
    OBJ.main()
