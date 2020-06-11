# -*- coding: utf-8 -*-
import MySQLdb


class SoccerLocations():
	def __init__(self):
                #self.conn   = MySQLdb.connect(host="10.28.216.45", user="veveo", passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True)
                self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
                self.cursor = self.conn.cursor()

	def main(self):
		f = open('locations.txt', 'r+')
		for record in f:
			data = record.strip().split(',')
			if len(data) == 7:
				id_, continent, country, state, city, latlong, iso = data
				if "/" not in latlong:
					latlong= latlong.replace('N ', 'N / ')
				up_qry = 'update sports_locations set continent=%s, country=%s, state=%s, city=%s, latlong=%s, iso=%s where id = %s limit 1'
				values = (continent.strip(), country.strip(), state.strip(), city.strip(), latlong.strip(), iso.strip(), id_.strip())
				try:
					self.cursor.execute(up_qry, values)
				except:
					print "error", id_, continent, country, state, city, latlong, iso


if __name__ == '__main__':
    OBJ = SoccerLocations()
    OBJ.main()
