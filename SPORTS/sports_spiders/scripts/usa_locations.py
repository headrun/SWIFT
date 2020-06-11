import MySQLdb

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', \
                    'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
                    'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', \
                    'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
                    'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', \
                    'WV' : 'West Virgina', 'FL' : 'Florida', \
                    'KS' : 'Kansas', 'TN' : 'Tennessee', \
                    'LA' : 'Louisiana', 'MO' : 'Missouri', \
                    'AR' : 'Arkansas', 'SD' : 'South Dakota', \
                    'MS' : 'Mississippi', 'MI' : 'Michigan', \
                    'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', \
                    'ID' : 'Idaho', 'RI' : 'Rhode Island', \
                    'NM' : 'New Mexico', 'MN' : 'Minnesota', \
                    'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
                    'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', \
                    'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado', \
                    "ON" : "Ontario", "AK": "Alaska", "BC": "British Columbia", \
                    "SK": "Saskatchewan", "QC": "Quebec", "AB": "Alberta", \
                    "NS" : "Nova Scotia", "MB": "Manitoba", "WA" : "Washington",  \
                    "NB" : "New Brunswick", 'AZ': "Arizona", \
                    'PE': "Prince Edward Island", 'NV' : "Nevada", \
                    'NL': "Newfoundland and Labrador", \
                    'ND': "North Dakota", "DC": "District of Columbia", \
                    'NH': "New Hampshire", 'ME': "Maine", 'LA': "Louisiana", \
                    "HI" : "Hawaii"}


class USALocations():
	def __init__(self):
        	#self.conn   = MySQLdb.connect(host="10.28.216.45", user="veveo", db="SPORTSDB_DEV", charset='utf8', use_unicode=True)
		self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        	self.cursor = self.conn.cursor()
		
	def main(self):
		loc_query = 'select id, birth_place from sports_players where birth_place like "%, USA%"'
		self.cursor.execute(loc_query)
		data = self.cursor.fetchall()
		for data_ in data:
		    	id_ = str(data_[0])
		    	birth_place = str(data_[1])
			if len(birth_place.split(','))==3:
				import pdb;pdb.set_trace()
				city   = birth_place.split(',')[0].strip()
				state_ = birth_place.split(',')[1].strip()
				country = birth_place.split(',')[2].strip()
				state  = REPLACE_STATE_DICT.get(state_, '')
				if not state:
				    state = state_
				birth_data = city, state
				birth_place = ", ".join(birth_data)
				up_qry = 'update sports_players set birth_place=%s where id=%s limit 1'
				values = (birth_place, id_)
				self.cursor.execute(up_qry, values)
				bt_loc_qry = 'select id from sports_locations where city=%s and state=%s and country=%s limit 1'
				values_ = (city, state, country)
				self.cursor.execute(bt_loc_qry, values_)
				data = self.cursor.fetchall()
				if data:
					loc_id =str(data[0][0])
					loc_up_qry = 'update sports_players set birth_place_id=%s where id = %s limit 1'
					values_ = (loc_id, id_)
					self.cursor.execute(loc_up_qry, values_)

			if len(birth_place.split(','))==2:	
				city   = birth_place.split(',')[0].strip()
				country = birth_place.split(',')[1].strip()			 
				state_qurey = 'select id, state from sports_locations where city=%s and country=%s'
				values = (city, country)
				self.cursor.execute(state_qurey, values)
				data = self.cursor.fetchall()
				if data and len(data)==1:
					loc_id = str(data[0][0])
					state = str(data[0][1])
				        birth_data = city, state
					birth_place = ", ".join(birth_data)
					up_qry = 'update sports_players set birth_place=%s where id=%s limit 1'
					values = (birth_place, id_)
					self.cursor.execute(up_qry, values)
		
					loc_up_qry = 'update sports_players set birth_place_id=%s where id = %s limit 1'
                                        values_ = (loc_id, id_)
                                        self.cursor.execute(loc_up_qry, values_)
if __name__ == '__main__':
    OBJ = USALocations()
    OBJ.main()
	
