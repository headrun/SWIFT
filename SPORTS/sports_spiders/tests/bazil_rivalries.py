import MySQLdb

QUERY = 'INSERT IGNORE INTO sports_tournaments_groups \
(id, gid, group_name, keywords, aka, tournament_id, group_type, info, base_popularity, image_link, created_at, modified_at)\
values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())'


class BrazillRivalries:

    def __init__(self):
        #self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.brazil_list = open('brazil_rivalries', 'r+')
       

    def main(self):
        data = self.brazil_list.readlines()
        for record in data:
            ri_data = record
            if len(ri_data.split('#<>#')) == 3:
                teams = ri_data.split('#<>#')[:2]
                derby = ri_data.split('#<>#')[-1].strip().replace('"', '')
                aka = ''
                if "[" in derby:
                    derby = derby.split('[')[0].strip()
                if "(" in derby:
                    derby_ = derby.split('(')[-1].strip().replace(')', '')
                    aka   = derby.split('(')[0].strip()
                    if aka == '':
                        derby_ = ''
                    derby = derby_
                derby =  derby.replace('The ', '')
                aka = aka.replace('The ', '')
                if aka == derby:
                    aka = ''
            elif len(ri_data.split('#<>#')) ==2:
                teams = ri_data.split('#<>#')[:2]
                derby = " vs. ".join(teams)
                derby = derby + " Derby"
                aka = ''
            else:
                derby = ''
                teams = []
            tou_id = '573'
            teams = self.get_team_ids(teams)
            query = 'select id from sports_tournaments_groups where group_type="rivalry" and group_name like %s'
            name = '%' + derby + '%'
            self.cursor.execute(query, name)
            data = self.cursor.fetchone()
            if data:
                continue
            else:
                if len(teams) == 2:
                    grp_id, grp_gid = self.get_id_gid()
                    values = (grp_id, grp_gid, derby, '', aka, tou_id, 'Rivalry', '', '100', '')
                    self.cursor.execute(QUERY, values)
                    print 'populated>>>>>>', derby
                    for team in teams:
                        query = 'insert into sports_groups_participants (group_id, participant_id, season, created_at, modified_at) values (%s, %s, %s, now(), now())'
                        values = (grp_id, team, '2016')
                        self.cursor.execute(query, values)
                elif len(teams) ==1:
                    print derby
                    print teams


    def get_id_gid(self):
        query = "select auto_increment from information_schema.TABLES where TABLE_NAME='sports_tournaments_groups' and TABLE_SCHEMA='SPORTSDB'"
        self.cursor.execute(query)
        count = self.cursor.fetchone()
        grp_id =  count[0]
        grp_gid = 'GR' + str(grp_id)
        return grp_id, grp_gid


    def get_team_ids(self, teams):
        team_ids = []
        for team in teams:
            team = team
            query = 'select id from sports_participants where title like %s and participant_type not in ("player", "retired") and game ="soccer"'
            name = '%' + team + '%'
           
            self.cursor.execute(query, name)
            data = self.cursor.fetchone()
            if not data:
                query = 'select id from sports_participants where aka like %s and participant_type = "team" and game ="soccer"'
                name = '%' + team + '%'
                self.cursor.execute(query, name)
                data = self.cursor.fetchone()
            if data:
                team_id = data[0]
                if team_id:
                    tm_query = 'select participant_id from sports_tournaments_participants where tournament_id =573 and participant_id=%s'
                    self.cursor.execute(tm_query, team_id)
                    data = self.cursor.fetchone()
                    if not data:
                        tm_query = 'select participant_id from sports_teams where tournament_id =573 and participant_id=%s'
                        self.cursor.execute(tm_query, team_id)
                        data = self.cursor.fetchone()
                    if data:
                        team_id = data[0]
                        team_ids.append(team_id)

        return team_ids



if __name__ == '__main__':
    OBJ = BrazillRivalries()
    OBJ.main()

