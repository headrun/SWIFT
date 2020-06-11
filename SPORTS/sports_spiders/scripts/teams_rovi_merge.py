# -*- coding: utf-8 -*-
import MySQLdb

TEAM_QRY = 'select P.id from sports_participants P, sports_tournaments_participants ST, sports_tournaments T where P.id=ST.participant_id and ST.tournament_id=T.id and T.title=%s and P.title like %s and ST.status="active" and P.sport_id="7" and P.participant_type="team"'

NCAA_TEAM_QRY = 'select P.id from sports_participants P, sports_tournaments_participants ST, sports_tournaments T where P.id=ST.participant_id and ST.tournament_id=T.id and T.title=%s and P.title like %s and ST.status="active" and P.sport_id="2" and P.participant_type="team"'

ROVi_QRY = 'select id from sports_rovi_merge where rovi_id="%s" and entity_type="team"'

MERGE_QRY = "insert into sports_rovi_merge(entity_type, entity_id, rovi_id, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

LEAGUE_LIST = ['Italian Soccer League', 'German Soccer League', 'Ecuadorian Soccer League', 'English Soccer League', 'French Soccer League', 'Spanish Soccer League', 'Brazilian Soccer League', 'Turkish Soccer League', 'Croatian Soccer League', 'Danish Soccer League', 'Croatian Soccer Leagu', 'Swedish Soccer League', 'Peruvian Soccer League', 'Top 14 Rugby','Dutch Soccer League', 'Brazilian Soccer League', 'Argentine Soccer League', 'German Soccer League', 'Argentine Soccer League', 'Italian Soccer League', 'Norwegian Soccer League', 'English Soccer League', 'Ukrainian Soccer League', 'Belgian Soccer League', 'Scottish Soccer League', 'Russian Soccer League', 'Swiss Soccer League', 'Belarus Soccer League', 'Hungarian Soccer League', 'Bulgarian Soccer League', 'Czech Soccer League', 'Kazakhstan Soccer League', 'Cypriot Soccer League', 'Slovakian Soccer League']

LEAGUE_LIST = ['Norwegian Soccer League', 'Belgian Soccer League', 'Scottish Soccer League', 'Russian Soccer League', 'Swiss Soccer League', 'Belarus Soccer League', 'Hungarian Soccer League', 'Bulgarian Soccer League', 'Czech Soccer League', 'Kazakhstan Soccer League', 'Cypriot Soccer League', 'Slovakian Soccer League']

NA_LIST = ['FIFA']

NA_TEAM_QRY = 'select id from sports_participants where title=%s'

NCAA_LEAGUE_LIST = ['NCAA']

GAME_LIST = ['Basketball', 'Baseball', 'Football', 'Soccer', 'volleyball', 'softball']

NCAA_TEAMS_DICT = {'Maryland (Baltimore County) Retrievers': "UMBC Retrievers men's basketball", \
		   'St. Francis (N.Y.) Terriers': "St. Francis Brooklyn Terriers men's basketball", \
		   "UNLV Rebels": "UNLV Runnin' Rebels basketball", \
		   "Texas (San Antonio) Roadrunners": "UTSA Roadrunners men's basketball", \
		   "Delaware Blue Hens": "Delaware Fightin' Blue Hens men's basketball", \
		   "Virginia Commonwealth Rams" : "VCU Rams men's basketball", \
		   "Missouri (Kansas City) Kangaroos": "UMKC Kangaroos men's basketball", \
		   "Massachusetts Minutemen": "UMass Minutemen basketball", \
		   "Illinois (Chicago) Flames" : "UIC Flames men's basketball", \
		   "Central Florida Knights": "UCF Knights men's basketball", \
		   "Alabama (Birmingham) Blazers" : "UAB Blazers men's basketball", \
		   "Southern Mississippi Golden Eagles": "Southern Miss Golden Eagles basketball", \
		   "Southern Illinois (Edwardsville) Cougars": "SIU Edwardsville Cougars men's basketball", \
		   "St. Mary's (Cal.) Gaels": "Saint Mary's Gaels men's basketball", \
		   "St. Francis (Pa.) Red Flash" : "Saint Francis Red Flash men's basketball", \
		   "Pennsylvania Quakers": "Penn Quakers men's basketball", \
		   "LIU (Brooklyn) Blackbirds": "Long Island Blackbirds men's basketball", \
		   "Middle Tennessee State Blue Raiders": "Middle Tennessee Blue Raiders men's basketball", \
		   "Florida International Panthers": "FIU Panthers men's basketball", \
                   "Rosemont Ravens": "Rosemont Ravens men's basketball", \
                   "Merchant Marine Mariners": "U.S. Merchant Marine Academy Mariners men's basketball", \
                   "Nazareth Golden Flyers": "Nazareth Golden Flyers men's basketball", \
                   "Michigan (Dearborn) Wolverines" :"Michigan-Dearborn Wolverines men's basketball", \
                   "Oneonta Red Dragons": "SUNY Oneonta Red Dragons men's basketball", \
                   "Goucher Gophers": "Goucher Gophers men's basketball", \
                   "Cornell College Rams": "Cornell Rams men's basketball", \
                   "Rockford Regents": "Rockford Regents men's basketball", \
                   "Alma College Scots": "Alma Scots men's basketball", \
                   "Colorado College Tigers": "Colorado College Tigers men's basketball"

		    }

TOU_LIST = ['Bundesliga', 'Serie A', 'La Liga', 'Ligue 1', 'Campeonato Brasileiro Série A', 'Ecuadorian Serie A', 'Peruvian Primera División', 'Premier League', 'Sky Bet League 1', 'Sky Bet Championship', 'Sky Bet League 2', 'Liga 1', 'Championnat National', 'Super Lig', 'Allsvenskan', 'Superliga']


class TeamsRoviMerge():
	def __init__(self):
                
            	#self.conn   = MySQLdb.connect(host="10.28.216.45", user="veveo", passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True)
                self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
                self.cursor = self.conn.cursor()
		self.merge_file = open('Teams.txt', 'r+')
		self.missing_merge = open('missing_teams_merge', 'w+')

	def check_rovi_id(self, rovi_id):
		self.cursor.execute(ROVi_QRY %rovi_id)
		data = self.cursor.fetchone()
		if data:
			rovi_id = data[0]
			tm_exists = True
		else:   
			tm_exists = False
		return tm_exists, rovi_id
		

        def main(self):
		for team_list in self.merge_file:
			team_list = team_list.strip()
			team_data = team_list.split('|')
			if team_data:
				rovi_id = team_data[0]
				league = team_data[11]
				tournament = team_data[12].replace('Sky Bet', 'EFL').replace('1', 'One').replace('2', 'Two')
				gender = team_data[9]
				game = team_data[8]
				ncaa_team_name = team_data[4]
				if tournament == "Primera División":
					tournament = "La Liga"

				if league == "Brazilian Soccer League" and tournament == "Série A":
					tournament = "Campeonato Brasileiro Série A"
				if league == "Ecuadorian Soccer League" and tournament =="Serie A":
					tournament = "Ecuadorian Serie A"
				if tournament == "Segunda B":
					tournament = "Segunda División B"
                                if league == "Top 14 Rugby":
                                        tournament = "France Top 14"

                                if "France Top 14" in tournament:
                                    team_name  = team_data[5].replace('Sky Bet', 'EFL')
                                    if "Bordeaux" in team_name:
                                        team_name = "Union Bordeaux Bègles"
                                    if "Castres" in team_name:
                                        team_name = "Castres Olympique"
                                    if "Clermont-Ferrand" in team_name:
                                        team_name = "ASM Clermont Auvergne"
                                    if "Montpellier" in team_name:
                                        team_name = "Montpellier Hérault Rugby"
                                    if "Lyon" in team_name:
                                        team_name = "Lyon OU"
                                    if "Nanterre" in team_name:
                                        team_name = "Racing 92"
                                    if "Paris" in team_name:
                                        team_name = "Stade Français"
                                    if "Pau" in team_name:
                                        team_name= "Section Paloise"
                                    if "Agen" in team_name:
                                        team_name = "SU Agen Lot-et-Garonne"
                                    if "La Rochelle" in team_name:
                                        team_name = "Stade Rochelais"
                                    if "Toulon" in team_name:
                                        team_name = "RC Toulonnais"
                                    if "Toulouse" in team_name:
                                        team_name = "Stade Toulousain"
                                  
                                else:  

                                    team_name = team_data[2].replace('Santa Cruz-PE', 'Santa Cruz Futebol Clube')
                                    if "América" in team_name and tournament == "Campeonato Brasileiro Série A":
                                            team_name =  "América Futebol Clube"
                                    if "Sport" in team_name and "Recife" in team_data[5] and tournament == "Campeonato Brasileiro Série A":
                                            team_name = "Sport Club do Recife"

                                    if team_name == "Cologne":
                                            team_name =  "1. FC Köln"
                                    if team_name == "Hertha Berlin":
                                            team_name = "Hertha BSC"
                                    if team_name == "Bayer Leverkusen":
                                            team_name = "Bayer 04 Leverkusen"
                                    if team_name =="Celta Vigo":
                                            team_name = "Celta de Vigo"
                                    if team_name == "Rennes":
                                            team_name = "Stade Rennais F.C."
                                    if team_name == "St. Etienne":
                                            team_name = "AS Saint-Étienne"
                                    if team_name == "Gazelec Ajaccio":
                                            team_name = "Ajaccio GFCO"
                                    if team_name == "Deportivo La Coruña":		
                                            team_name = "Deportivo de La Coruña"
                                    if team_name == "Liga Deportiva Universitaria de Quito":
                                            team_name = "LDU Quito"
                                    if team_name == "Universidad Católica Quito":
                                            team_name = "U. Catolica"
                                    if team_name == "Independiente del Valle":
                                            team_name = "Independiente Teran"
                                    if team_name == "River Ecuador":
                                            team_name = "Club Deportivo River Plate Ecuador"
                                    if team_name == "Universidad César Vallejo":
                                            team_name = "Cesar Vallejo"
                                    if team_name == "Ayacucho FC":
                                            team_name = "Inti Gas Deportes"
                                    if team_name == "Real Garcilaso":
                                            team_name = "Real Atletico Garcilaso"
				    if team_name == "UCF" and "Football" in team_data[8]:
					    team_name  = "UCF Knights football"
				    if team_name == "Middle Tennessee State" and "Football" in team_data[8]:
					    team_name = "Middle Tennessee Blue Raiders football"		
				    if team_name == "UAB" and "Football" in team_data[8]:
					    team_name = "UAB Blazers football"
				    if team_name == "Southern Mississippi" and "Football" in team_data[8]:
					    team_name = "Southern Miss Golden Eagles football"
				    if team_name == "Cal State (Bakersfield)" and "Football" in team_data[8]:
					    team_name = "UTSA Roadrunners football"
				    if team_name == "Rosenborg":
					    team_name = "Rosenborg BK"
				    if team_name  == "Dynamo Kyiv":
					    team_name = "FC Dynamo Kyiv"
			 	    if team_name  == "Dinamo Zagreb":
					    team_name = "GNK Dinamo Zagreb"
				    if team_name == "KRC Genk":
					    team_name  = "K.R.C. Genk"
				    if team_name == "Rapid Vienna":
					    team_name = "SK Rapid Wien"
				    if team_name  == "Rangers" and "Glasgow" in team_data[5]:
					    team_name  = "Rangers F.C."
				    if team_name == "Krasnodar":
					    team_name  = "FC Krasnodar"			
				    if team_name  == "PAOK" and game == "Soccer":
					    team_name = "PAOK FC"
				    if team_name == "Zurich" and "Zurich" in team_data[4] and game == "Soccer":
					    team_name = "FC Zurich"
				    if team_name == "BATE Borisov":
					    team_name  = "FC BATE Borisov"
				    if team_name == "Jablonec":
					    team_name == "FK Jablonec"
				    if team_name == "Astana":
					    team_name = "FC Astana"
				    if team_name == "Apollon Limassol":
					    team_name = "Apollon Limassol FC"
				    if team_name == "Vorskla Poltava":
					    team_name = "FC Vorskla Poltava"
				    if team_name  == "Slavia Prague":
					    team_name = "SK Slavia Prague"
				    if team_name  == "Sarpsborg 08":
					    team_name = "Sarpsborg 08 FF"
				    if team_name == "Fenerbahç":
					    team_name = "Fenerbahçe S.K. (football)"
                                    if team_name == "LIU" and "LIU Sharks" in team_data[4] and "Baseball" in team_data[8]:
                                            team_name = "LIU Sharks Baseball"
                                    if team_name == "LIU" and "LIU Sharks" in team_data[4] and "Basketball" in team_data[8] and "Male" in team_data[9]:
                                            team_name = "LIU Sharks Men's Basketball"
                                    if team_name == "LIU" and "LIU Sharks" in team_data[4] and "Basketball" in team_data[8] and "Male" in team_data[9]:
                                            team_name = "LIU Sharks Women's Basketball"
                                    if team_name  == "LIU" and "LIU Sharks" in team_data[4] and "Softball" in team_data[8] and "Female" in team_data[9]:
                                            team_name = "LIU Sharks Women's Softball"
                                    if team_name  == "LIU" and "LIU Sharks" in team_data[4] and "Soccer" in team_data[8] and "Male" in team_data[9]:
                                            team_name  = "LIU Sharks Men's Soccer"
				    if team_name  == "LIU" and "LIU Sharks" in team_data[4] and "Soccer" in team_data[8] and "Female" in team_data[9]:
                                            team_name  = "LIU Sharks women's Soccer"
                                    if team_name  == "LIU" and "LIU Sharks" in team_data[4] and "Football" in team_data[8] and "Male" in team_data[9]:
                                            team_name = "LIU Sharks Football"
                                    if team_name  == "LIU" and "LIU Sharks" in team_data[4] and "Volleyball" in team_data[8] and "Male" in team_data[9]:
                                            team_name = "LIU Sharks women's volleyball"

				tm_exists, rovi_id = self.check_rovi_id(rovi_id)

				'''if league in NA_LIST and "Soccer" in game and "Male" in gender:
					team_name = team_name.replace('Bosnia-Herzegovina', 'Bosnia and Herzegovina'). \
					replace('Ireland National Team', 'Republic of Ireland National Team')
					if "U.S. National Team" in team_name:
						team_name = "United States men's national soccer team"
					elif "National Team" not in team_name:
						team_name = team_name + " national football team"
					else:
						team_name = team_name.replace('National Team', 'national football team')
					team_title = team_name
                                        values = (team_title)
                                        self.cursor.execute(NA_TEAM_QRY, values)
                                        data = self.cursor.fetchall()
					if data and len(data) ==1 and tm_exists == False:
                                                team_id = str(data[0][0])
                                                values = ("team", team_id, rovi_id)
                                                self.cursor.execute(MERGE_QRY, values)'''


			        #if league in LEAGUE_LIST and "Soccer" in game:
                                '''if league in LEAGUE_LIST and "Rugby" in game:
                                        team_title = '%' + team_name + '%'
					values = (tournament, team_title)
					self.cursor.execute(TEAM_QRY, values)
				        data = self.cursor.fetchall()
                                        #import pdb;pdb.set_trace()
					if not data:
						team_title = '%' + team_name.split(' ')[-1] + " " + team_name.split(' ')[0] +'%'	
						values = (tournament, team_title)
						self.cursor.execute(TEAM_QRY, values)
						data = self.cursor.fetchall()
					if not data and "(" in team_title:
                                                team_title = team_name.split('(')[0] + team_name.split(')')[-1].strip()
                                                values = (tournament, team_title)
                                                self.cursor.execute(TEAM_QRY, values)
                                                data = self.cursor.fetchall()
				       	if not data and "AC " in team_name:
                                               team_title = '%' + team_name.replace('AC ', 'A.C. ') + '%'
                                               values = (tournament, team_title)
                      	                       self.cursor.execute(TEAM_QRY, values)
                                               data = self.cursor.fetchall()
					if data and len(data) ==1 and tm_exists == False:
						team_id = str(data[0][0])
						values = ("team", team_id, rovi_id)
                                                print values
						self.cursor.execute(MERGE_QRY, values)
                                        if not data and len(data) !=1:
						self.missing_merge.write('%s\t%s\t%s\t%s\n' %(rovi_id, tournament, team_name, league))'''
				
				if league in NCAA_LEAGUE_LIST and game in GAME_LIST and "Male" in gender:
                                    if team_data[8] == "Basketball" and team_data[9] == "Male":
                                        team_name  = ncaa_team_name
                                        team_name = ncaa_team_name + " men's Basketball"

                                        tournament = "NCAA Men's Division I Basketball"
                                        team_title = '%' + team_name + '%'
                                        team_title = team_title.replace("Cowboys men's basketball", "Cowboys basketball"). \
                                        replace('Wisconsin (', '').replace(')', ''). \
					replace("Southern California Trojans", "USC Trojans"). \
					replace("Southern Mississippi", "Southern Miss"). \
					replace('Choctaw', 'Choctaws').replace('Albany State', 'Albany'). \
					replace('St.', 'Saint').replace('North Carolina (', 'UNC '). \
					replace(')', '').replace('College of ', ''). \
					replace('Central Connecticut State', 'Central Connecticut'). \
					replace("Michigan (Dearborn) Wolverines", "Michigan-Dearborn Wolverines") 
                                        values = (tournament, team_title)
                                        self.cursor.execute(NCAA_TEAM_QRY, values)
                                        data = self.cursor.fetchall()
                                        if not data:
                                                team_title = team_title.replace('North Carolina', 'NC')
                                                values = (tournament, team_title)
                                                self.cursor.execute(NCAA_TEAM_QRY, values)
                                                data = self.cursor.fetchall()
                                        if not data and "(" in team_title:
                                                team_title = team_name.split('(')[0] + team_name.split(')')[-1].strip()
                                                values = (tournament, team_title)
                                                self.cursor.execute(NCAA_TEAM_QRY, values)
                                                data = self.cursor.fetchall()
                                        if not data:
                                                team_title = NCAA_TEAMS_DICT.get(ncaa_team_name, '')
                                                values = (tournament, team_title)
                                                self.cursor.execute(NCAA_TEAM_QRY, values)
                                                data = self.cursor.fetchall()

                                                
                                        if data and len(data) ==1 and tm_exists == False:
                                                team_id = str(data[0][0])
                                                values = ("team", team_id, rovi_id)
                                                self.cursor.execute(MERGE_QRY, values)
                                                print values
                                        elif not data and tm_exists == False:
                                                self.missing_merge.write('%s\t%s\t%s\t%s\n' %(rovi_id, tournament, team_name, league))
            
					



if __name__ == '__main__':
    OBJ = TeamsRoviMerge()
    OBJ.main()

