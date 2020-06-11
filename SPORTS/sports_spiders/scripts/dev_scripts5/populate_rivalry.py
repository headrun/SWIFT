import MySQLdb

QUERY = 'INSERT IGNORE INTO sports_tournaments_groups \
(id, gid, group_name, keywords, aka, tournament_id, group_type, info, base_popularity, image_link, created_at, modified_at)\
values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())'
WIKI_QUERY = 'INSERT IGNORE INTO sports_wiki_merge (exposed_gid, child_gid, action, modified_date) values (%s, %s, %s, now())'

#SPORTS_WIKI_MERGE = 'INSERT IGNORE INTO sports_wiki_merge (wiki_gid, sports_gid, action, modified_date) values (%s, %s, %s, now())'

class PopulateMerge:

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        #self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.gid_con      = MySQLdb.connect(host="10.4.18.34", user="root", db="GUIDMERGE")
        #self.gid_con      = MySQLdb.connect(host="10.4.2.187", user="root", db="GUIDMERGE")
        self.gid_cursor    = self.gid_con.cursor()

        self.merge_dict = {}
        self.get_merge()

    def get_merge(self):
        merges = open('sports_to_wiki_guid_merge.list', 'r').readlines()
        for merge in merges:
            if 'TEAM' in merge.strip():
                wiki_gid, team_gid = merge.strip().split('<>')
                self.merge_dict[wiki_gid] = team_gid

    def get_team_ids(self, teams):
        team_ids = []
        for team in teams:
            team = 'WIKI' + team
            if team == "WIKI4998626":
                team = "WIKI7958395"
            if team == "WIKI12678334":
                team = "WIKI15164948"

            if self.merge_dict.get(team, ''):
                team_gid = self.merge_dict[team]
            else:
                team_gid = ''
            if team_gid:
                query = 'select id from sports_participants where gid=%s'
                self.cursor.execute(query, team_gid)
                data = self.cursor.fetchone()
                team_id = data[0]
                team_ids.append(team_id)

        return team_ids


    '''def get_team_ids(self, teams):
        team_ids = []
        for team in teams:
            team = team
            query = 'select id from sports_participants where title=%s and participant_type = "team"'
            self.cursor.execute(query, team)
            data = self.cursor.fetchone()
            team_id = data[0]
            team_ids.append(team_id)

        return team_ids'''

    def get_id_gid(self):
        query = "select auto_increment from information_schema.TABLES where TABLE_NAME='sports_tournaments_groups' and TABLE_SCHEMA='SPORTSDB_BKP'"
        self.cursor.execute(query)
        count = self.cursor.fetchone()
        grp_id =  count[0]
        grp_gid = 'GR' + str(grp_id)
        print grp_gid
        return grp_id, grp_gid

    def main(self):
        data = open('derby', 'r').readlines()
        for record in data:
            rivalry_data = [rec.replace('"', '').strip() for rec in record.strip().split(',')]
            title = rivalry_data[0]
            if title == "Clean":
                title = "Clean, Old-Fashioned Hate"
                teams = self.get_team_ids(rivalry_data[2:4])
                tou_id = rivalry_data[4]
                aka = rivalry_data[6]
                rivalry_gid = rivalry_data[7]
            else:
                import pdb;pdb.set_trace()
                teams = self.get_team_ids(rivalry_data[3:5])
                print teams
                tou_id = rivalry_data[5]
                aka = rivalry_data[7]
                rivalry_gid = rivalry_data[8]
                print rivalry_gid

            query = 'select id from sports_tournaments_groups where group_type="rivalry" and group_name like %s'
            name = '%' + title + '%'
            self.cursor.execute(query, name)
            data = self.cursor.fetchone()
            print data
            if data and data[0] != 743:
                continue
            else:
                grp_id, grp_gid = self.get_id_gid()
                if len(teams) == 2 and rivalry_gid:
                    import pdb;pbd.set_trace()
                    wiki_gid = "WIKI" + rivalry_gid
                    values = (grp_id, grp_gid, title, '', aka, tou_id, 'Rivalry', '', '100', '')
                    self.cursor.execute(QUERY, values)
                    print 'populated>>>>>>', title
                    if wiki_gid !="WIKI0":
                        values = (wiki_gid, grp_gid, "override")
                        self.gid_cursor.execute(WIKI_QUERY, values)
                    #self.cursor.execute(SPORTS_WIKI_MERGE, values)


                    for team in teams:
                        query = 'insert into sports_groups_participants (group_id, participant_id, season, created_at, modified_at) values (%s, %s, %s, now(), now())'
                        values = (grp_id, team, '2015')
                        self.cursor.execute(query, values)
                else:
                    print title


if __name__ == '__main__':
    OBJ = PopulateMerge()
    OBJ.main()
