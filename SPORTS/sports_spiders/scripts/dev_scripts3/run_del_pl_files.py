import MySQLdb

class Files:

	def __init__(self):
		self.conn = MySQLdb.connect(user='root', db='SPORTSDB', host='10.4.18.183')
		self.cursor = self.conn.cursor()
		#self.file_ = open('player_names', 'w')

	def main(self):
		'''lines = open('deleted_dup_pl', 'r').readlines()
		for line in lines:
			gids = line.strip().split('<>')
			record = ''
			for gid in gids:
				query = 'select title from sports_participants where gid="%s"'
				self.cursor.execute(query % gid)
				title = self.cursor.fetchone()
				if title:
					title = title[0]
				if record:
					record += '<>%s' % title
				else:
					record = title
			self.file_.write('%s\n' % record)
		'''
		files = ['awards_list_2015-11-17', 'del_pl_list_2015-11-17', 'insert_list_2015-11-17', 'update_pl_list_2015-11-17']
		files = ['insert_list_2015-11-17']
		for file_ in files:
			records = open(file_, 'r').readlines()
			for query in records:
				try:
					self.cursor.execute(query.strip())
				except:
					print query
					#import pdb; pdb.set_trace()

if __name__ == "__main__":
	OBJ = Files()
	OBJ.main()
