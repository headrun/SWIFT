from vtv_task import VtvTask, vtv_task_main

INSERT_TESTCASE = 'insert into test_cases (suite_id, record, created_at, last_modified, flag) values(%s, %s, now(), now(), %s)'

class PopulateTestcases(VtvTask):

	def __init__(self):
		VtvTask.__init__(self)
		self.db_ip        = '10.4.2.187'
		self.db_name      = 'DATATESTDB'
		self.sports_db    = '10.4.18.183'
		self.sports_dbname = 'SPORTSDB'
		self.test_file    = open('testcases_file', 'r')
		self.out_file     = open('write_testcases', 'w')
		self.stadiums_file= open('stadiums_merge', 'r')    
		self.stadiums     = {}
		self.stadiums_gids = {}
		self.duplicate_list = []
		self.duplicates_file = open('duplicate_stadiums', 'w')

	def get_suite_id(self):
		query = "select id from test_suites where suite_name= 'stadiums'"
		self.cursor.execute(query)
		result = self.cursor.fetchone()
		if result:
			result = result[0]
		return result
		

	def populate_testcases(self):
		suite_id = self.get_suite_id()
		for row in self.test_file:
			row = [rw.strip() for rw in row.strip().split(',')]
			if len(row) == 6:
				query = 'select record from test_cases where suite_id=%s and record like %s'
				val = '%' + row[0] + '#%'
				values = (suite_id, val)
				self.cursor.execute(query, values)
				data = self.cursor.fetchone()
				if data:
					continue
				record = '#<>#'.join(row)
				print record
				#inserting test cases
				values = (suite_id, record, 'active')
				self.cursor.execute(INSERT_TESTCASE, values)

				self.out_file.write('%s\n' % record)
				wiki_gid = row[0]
				title = row[1]
				self.stadiums[wiki_gid] = title
	
	def get_stadium_gids(self):
		self.open_cursor(self.sports_db, self.sports_dbname)
		import pdb; pdb.set_trace()
		for record in self.stadiums_file.readlines():
		    if ',' in record:
		        stadium_gid, wiki_gid = [rec.strip() for rec in record.split(',')]
		    else:
		        stadium_gid, wiki_gid = [rec.strip() for rec in record.split('<>')]
		    if not 'WIKI' in wiki_gid:
		        wiki_gid = 'WIKI' + wiki_gid
		    if self.stadiums_gids.get(wiki_gid, ''):
		        if stadium_gid not in self.duplicate_list:
		            self.duplicate_list.append(stadium_gid)
    		        stadiums = '<>'.join([stadium_gid, wiki_gid])
	    	        self.duplicates_file.write('%s\n' % stadiums)
		    else:
    			self.stadiums_gids[wiki_gid] = stadium_gid

	def populate_stadiums_merge(self):
		self.open_cursor('10.4.2.187', 'GUIDMERGE')
		query = 'select exposed_gid, child_gid from sports_wiki_merge'
		self.cursor.execute(query)
		stadium_list = {}

		for row in self.get_fetchmany_results():
			exposed_gid, child_gid = row
			if 'STAD' in child_gid:
				stadium_list[exposed_gid] = child_gid

		for wiki_gid, stad_gid in self.stadiums_gids.iteritems():
			if (stad_gid not in stadium_list.values()) and (wiki_gid not in stadium_list.keys()):	
				query = 'insert ignore into sports_wiki_merge (exposed_gid, child_gid, action, modified_date) values(%s, %s, %s, now())'
				values = (wiki_gid, stad_gid, 'override')
				self.cursor.execute(query, values)


	def run_main(self):
		self.open_cursor(self.db_ip, self.db_name)
		self.get_stadium_gids()
		self.populate_stadiums_merge()
		#self.populate_testcases()


if __name__ == '__main__':
	vtv_task_main(PopulateTestcases)
