import MySQLdb
from vtvspider import get_country, get_birth_place_id, get_state

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', \
                    'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
                    'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', \
                    'NC' : 'North Carolina', 'SC' : 'South Carolina', 'GA' : 'Georgia', \
                    'OR' : 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', \
                    'WV' : 'West Virginia', 'FL' : 'Florida', \
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
                    "SK": "Saskatchewan", "QC": "Montreal", "AB": "Alberta", \
                    "NS" : "Nova Scotia", "MB": "Manitoba", "WA" : "Washington",  \
                    "NB" : "New Brunswick", 'AZ': "Arizona", \
                    'PE': "Prince Edward Island", 'NV' : "Nevada", \
                    'NL': "Newfoundland and Labrador", \
                    'ND': "North Dakota", "DC": "District of Columbia", \
                    'NH': "New Hampshire", 'ME': "Maine", 'LA': "Louisiana", \
                    "HI" : "Hawaii", 'WY': "Wyoming", 'NS': 'New South Wales', \
		    "D.C." : "District of Columbia", 'D. C.': "District of Columbia", \
		    "West Virgina" : "West Virginia", 'VT': "Vermont"}


class PlayersBirthPlaces():
	def __init__(self):
		self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset="utf8", use_unicode=True)
		#self.conn = MySQLdb.connect(host="10.28.216.45", user="veveo", passwd="veveo123", db="SPORTSDB_DEV", charset="utf8", use_unicode=True)
		self.cursor = self.conn.cursor()
	def main(self):
		'''sel_qurey = 'select id, birth_place from sports_players where participant_id in (select player_id from sports_roster where status="active" and team_id in (select participant_id from sports_tournaments_participants where tournament_id =88)) and birth_place not like "%USA%" and birth_place not like "%canada%"'
		self.cursor.execute(sel_qurey)
		data = self.cursor.fetchall()
		for data_ in data:
			id_ = data_[0]
			birth_place = data_[1]
			state = birth_place.split(',')[-1].strip().replace('West Virgina', 'West Virginia')
			country_qry = 'select country from sports_locations where city=%s and state =%s and country !=""'
			values = (birth_place.split(',')[0].strip(), state)
			self.cursor.execute(country_qry, values)
			country = self.cursor.fetchone()
			if not country:
				country_qry = 'select country from sports_locations where state=%s and state !="" and country !=""'
				values = (state)
				self.cursor.execute(country_qry, values)
				country = self.cursor.fetchone()
			if country and len(birth_place.split(','))==2:
				country = str(country[0])
				b_data = birth_place.split(',')[0].strip(), state, country
                        	birth_place = ", ".join(b_data)
				up_qry = 'update sports_players set birth_place=%s where id=%s limit 1'
				values = (birth_place, id_)
				self.cursor.execute(up_qry, values)'''
		'''sel_query = 'select id, birth_place from sports_players where birth_place like "%United States%"'
		self.cursor.execute(sel_query)
                data = self.cursor.fetchall()
                for data_ in data: 
                        id_ = data_[0]
                        birth_place = data_[1]
			if len(birth_place.split(','))==3:
				city, state, country = birth_place.split(',')[0].strip(), birth_place.split(',')[1].strip(), birth_place.split(',')[-1].strip()
				country = country.replace('United States', 'USA')
				state_ = REPLACE_STATE_DICT.get(state, '')
				if not state_:
					state_ = state
			        b_data = city, state_, country
				birth_place = ", ".join(b_data)
                                up_qry = 'update sports_players set birth_place=%s where id=%s limit 1'
                                values = (birth_place, id_)
                                self.cursor.execute(up_qry, values)'''
	
		sel_query = 'select id, birth_place from sports_players where participant_id in (select player_id from sports_roster where team_id in (select participant_id  from sports_tournaments_participants where tournament_id in (35, 562, 32, 33, 579, 216, 215, 573)))'
		self.cursor.execute(sel_query)
                data = self.cursor.fetchall()
                for data_ in data: 
                        id_ = data_[0]
                        birth_place = data_[1]
                        if len(birth_place.split(','))==2:
                                city, state = birth_place.split(',')[0].strip(), birth_place.split(',')[1].strip()
                          	city = city.replace('Saint Albans', 'St. Albans') 
                                state_ = REPLACE_STATE_DICT.get(state, '')
                                if not state_:
                                        state_ = state
				country = get_country(city=city, state=state_)
				if country:
                                	b_data = city, state_, country
					birth_place_id = get_birth_place_id(city, state_, country)
					birth_place = ", ".join(b_data)
					up_qry = 'update sports_players set birth_place=%s, birth_place_id=%s where id=%s limit 1'
					values = (birth_place, birth_place_id, id_)
					self.cursor.execute(up_qry, values)
				else:
					city, country = birth_place.split(',')[0].strip(), birth_place.split(',')[1].strip()
					state_ = get_state(city=city, country=country)
					if state_:
						b_data = city, state_, country
						birth_place_id = get_birth_place_id(city, state_, country)
						birth_place = ", ".join(b_data)
						up_qry = 'update sports_players set birth_place=%s, birth_place_id=%s where id=%s limit 1'
						values = (birth_place, birth_place_id, id_)
						self.cursor.execute(up_qry, values)
					else:

						print city, state
					
if __name__ == '__main__':
    OBJ = PlayersBirthPlaces()
    OBJ.main()	
