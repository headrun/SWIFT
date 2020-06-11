import MySQLdb

class CountryTeamSTitle():
        def __init__(self):
                #self.conn   = MySQLdb.connect(host="10.28.216.45", user="veveo", db="SPORTSDB_DEV", charset='utf8', use_unicode=True)
                self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
                self.cursor = self.conn.cursor()

        def main(self):
		#sel_query = 'select id, participant_id, short_title, display_title from sports_teams where participant_id in (select id from sports_participants where title like "%team%" and game = "hockey")'
		#sel_query = 'select id, participant_id, short_title, display_title from sports_teams where participant_id in (select id from sports_participants where title like "%team%" and game = "curling")'
		sel_query = 'select id, participant_id, short_title, display_title from sports_teams where participant_id in (select id from sports_participants where title like "%team%" and game = "field hockey")'
		self.cursor.execute(sel_query)
		data = self.cursor.fetchall()
		for data_ in data:
			id_ = data_[0]
			p_id = data_[1]
			s_title = data_[2]
			up_query = 'update sports_teams set short_title=%s where id = %s limit 1'
			values = (s_title.title(), id_)
			self.cursor.execute(up_query, values)

if __name__ == '__main__':
    OBJ = CountryTeamSTitle()
    OBJ.main()



