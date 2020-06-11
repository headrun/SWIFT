import MySQLdb

name_dict = {
'MLB': 88,
'NFL': 197,
'NBA': 229,
'NHL': 240,
}

class SportsStatsTool:

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTS_COMMONDB", charset='utf8', use_unicode=True)
        self.cur = self.conn.cursor()
        self.prod_conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.prod_cur = self.prod_conn.cursor()

    def get_tou(self):
        qry = 'select id, name from Crawler'
        self.cur.execute(qry)
        rows = self.cur.fetchall()
        return rows

    def get_game_count(self, tou):
        qry = 'select season_start, season_end from sports_tournaments where id = %s'
        vals = (tou)
        self.prod_cur.execute(qry, vals)
        sea_start, sea_end = self.prod_cur.fetchall()[0]

        gam_qry = 'select count(*) from sports_games where tournament_id = %s and game_datetime <= %s and game_datetime >= %s and status != "Hole"'
        gam_vals = (tou, sea_end, sea_start)
        self.prod_cur.execute(gam_qry, gam_vals)
        count = self.prod_cur.fetchall()[0][0]
        self.cur.close()
        self.conn.close()
        self.prod_cur.close()
        self.prod_conn.close()

        return count

    def get_team_roster(self, tou):
        qry = 'select count(*) from sports_tournaments_participants where tournament_id  = %s and status = "active"'
        vals = (tou)
        self.prod_cur.execute(qry, vals)
        count = self.prod_cur.fetchall()[0][0]
        self.cur.close()
        self.conn.close()
        self.prod_cur.close()
        self.prod_conn.close()

        return count

    def get_player_roster(self, tou):
        qry = 'select participant_id from sports_tournaments_participants where tournament_id  = %s and status = "active"'
        vals = (tou)
        self.prod_cur.execute(qry, vals)
        rows = self.prod_cur.fetchall()
        temp_list = []
        for row in rows:
            team_id = row[0]
            team_qry = 'select count(*) from sports_roster where team_id = %s and status = "active"'
            team_vals = (team_id)
            self.prod_cur.execute(team_qry, team_vals)
            count = self.prod_cur.fetchall()[0][0]
            temp_list.append(count)
        player_count = sum(temp_list)
        self.cur.close()
        self.conn.close()
        self.prod_cur.close()
        self.prod_conn.close()

        return player_count

    def get_standings(self, tou):
        qry = 'select count(*) from sports_tournaments_results where tournament_id = %s'
        vals = (tou)
        self.prod_cur.execute(qry, vals)
        count = self.prod_cur.fetchall()[0][0]
        if count > 0 : return 1
        else: return 0
        self.cur.close()
        self.conn.close()
        self.prod_cur.close()
        self.prod_conn.close()


    def main(self):
        tou_names = self.get_tou()
        for i in tou_names:
            tou_id = name_dict[i[1]]
            crl_id = i[0]
            tou_games_count = self.get_game_count(tou_id)
            tou_team_count = self.get_team_roster(tou_id)
            tou_player_count = self.get_player_roster(tou_id)
            is_standing = self.get_standings(tou_id)
            qry = 'insert into Stats (crawler_id, games, team_roster_count, player_roster_count, is_standings, created_at, modified_at) values (%s, %s, %s, %s, %s, now(), now()) on duplicate key update games = %s, team_roster_count = %s, player_roster_count = %s, is_standings = %s, modified_at = now()'
            vals = (crl_id, tou_games_count, tou_team_count, tou_player_count, is_standing, tou_games_count, tou_team_count,tou_player_count,is_standing,)
            self.cur.execute(qry, vals)
        self.cur.close()
        self.conn.close()
        self.prod_cur.close()
        self.prod_conn.close()


if __name__ == '__main__':
    OBJ = SportsStatsTool()
    OBJ.main()
