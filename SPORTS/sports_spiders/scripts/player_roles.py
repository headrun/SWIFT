import MySQLdb

class PlayerRoles():
        def __init__(self):
                #self.conn   = MySQLdb.connect(host="10.28.216.45", user="veveo", passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True)
                self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
                self.cursor = self.conn.cursor()

        def main(self):
                #sel_query = 'select id, main_role from sports_players'
		sel_query = 'select id,player_role from sports_roster'
                self.cursor.execute(sel_query)
                data = self.cursor.fetchall()
                for data_ in data:
			id_ = data_[0]	
			main_role = data_[1]
			main_role = main_role.title().replace('-Back', '-back'). \
			replace('-Distance', '-distance').replace('-Rounder', '-rounder'). \
			replace('-Keeper', '-keeper').replace('-Handed', '-handed').replace('-Order', '-order'). \
			replace('-Row', '-row').replace('-Half', '-half').replace('-Eighth', '-eighth'). \
			replace('Pitchers', 'Pitcher').replace('Central ', '').replace('Center Back', 'Centre-back'). \
			replace('Centre Back', 'Centre-back').replace('Left Back', 'Left-back'). \
			replace('Full Back', 'Full-back').replace('Half Back', 'Half-back'). \
			replace('Right Back', 'Right-back')
			if main_role == "Defence" or main_role == "Defense":
				main_role = "Defender"

			if "wicket" in main_role.lower():
				main_role = "Wicket-keeper"
			if "runningback" in main_role.lower():
				main_role = "Running Back"
			if "winger" in main_role.lower():
				main_role = main_role.lower().replace('winger', 'Wing')
			if "rounder" in main_role.lower():
				main_role = "All-rounder"
			if "athlet" in main_role.lower():
				main_role = "Athlete"
			if "Infielders" in main_role:
				main_role = "Infielder"
			if "Long Jump" in main_role:
				main_role = "Long Jumper"
			if "chess grandmaster" in main_role.lower():
				main_role = "Grand Master"
			if "defensiveend" in main_role.lower():
				main_role = "Defensive End"
			if "defensivetackle" in main_role.lower():
				main_role = "Defensive Tackle"
			if "defensive line" in main_role.lower():
				main_role = "Defensive Linemen"
			if "offensive line" in main_role.lower():
				main_role = "Offensive Linemen"
			if "Middle Distance" in main_role:
				main_role = "Middle-distance Runner"
			#up_qry = 'update sports_players set main_role=%s where id=%s limit 1'
			up_qry = 'update sports_roster set player_role=%s where id=%s limit 1'
			values = (main_role, id_)
			self.cursor.execute(up_qry, values)
			

if __name__ == '__main__':
    OBJ = PlayerRoles()
    OBJ.main()

