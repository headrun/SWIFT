import MySQLdb
from vtv_task import VtvTask, vtv_task_main


def mysql_connection():
    conn = MySQLdb.connect(host="10.28.218.81", user="veveo",passwd='veveo123', db="SPORTSDB")
    cursor = conn.cursor()
    return conn, cursor

def mysql_awarsdb():
    #con = MySQLdb.connect(host="10.4.17.32", user="root", db="AWARDS")
    con =  MySQLdb.connect(host="10.4.2.187", user="root", db="AWARDS")
    cursor = con.cursor()
    return con, cursor

sk_update_query = 'update sports_source_keys set entity_id = %s where entity_id = %s limit 10;'
sprts_gams_update_qry = 'update sports_games_participants set participant_id = %s where participant_id = %s limit 10;'
sprt_gams_res_update_qry = 'update sports_games_results set participant_id = %s where participant_id = %s limit 10;'
sprts_gams_res_val_up_qry = 'update sports_games_results set result_value = %s where result_value = %s limit 10;'
sprts_tou_upd_qry = 'update sports_tournaments_results set result_value = %s where result_value = %s limit 10;'
sprts_grps_part_up_qry = 'update sports_groups_participants set participant_id = %s where participant_id = %s limit 10;'
sprts_par_up_qry = 'update sports_tournaments_participants set participant_id = %s where participant_id = %s limit 10;'
team_rostr_up_qry = 'update sports_rosters set team_id = %s where team_id = %s limit 10;'
sprt_par_del_qry = 'delete from sports_participants where id = %s limit 1;'
sprt_teams_del_qry = 'delete from sports_teams where participant_id = %s limit 1;'


class DuplicateTeamDeletion(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.d_teams = open('duplicate_teams.txt', 'r').readlines()
        self.update_tm_list = open('update_duplicate_teams_qries.txt', 'w')
        self.del_team_list     = open('del_team_list.txt', 'w')
        self.insert_team_list     = open('insert_team_list.txt', 'w')
        self.more_dup_teams = open('more_duplicate_teams.txt', 'w')

    '''def get_awards_list(self, pl_gid, del_gid):
        con, cursor = mysql_awarsdb()
        update_part = 'update sports_awards_history set participants= "%s" where participants= "%s"' %(pl_gid, del_gid)
        #cursor.execute(update_part)
        self.awards_list.write("%s\n" %(update_part))
        con.close()'''

    def get_tmid(self, tm_gid):
        conn, cursor = mysql_connection()
        tm_id_query = 'select id from sports_participants where gid = "%s"' %(tm_gid)
        cursor.execute(tm_id_query)
        tm_id = cursor.fetchone()
        if tm_id:
            tm_id = str(tm_id[0])
        conn.close()
        return tm_id

    def get_roster(self, del_id, pl_id):
        conn, cursor = mysql_connection()
        roster_query = 'select team_id, status from sports_roster where player_id = %s' %(del_id)
        cursor.execute(roster_query)
        pl_id = pl_id
        del_id = del_id
        roster_data = cursor.fetchall()
        if roster_data:
            for roster_ in roster_data:
                team_id = str(roster_[0])
                status = str(roster_[1])
                roster_active = 'select id from sports_roster where player_id = %s and team_id = %s and status = "%s"' %(pl_id, team_id, status)
                cursor.execute(roster_active)
                roster_act = cursor.fetchall()
                if roster_act:
                    for roster_ac in roster_act:
                        roster_delete = 'delete from sports_roster where player_id = %s' %(del_id)

                        #cursor.execute(roster_delete)
                else:
                    roster_delete = SPORTS_ROSTER %(pl_id , del_id)
                    #cursor.execute(SPORTS_ROSTER %(pl_id , del_id))
        else:
            roster_delete = ''
        return roster_delete


    def run_main(self):
#Final Multiple Source: WIKI859774 TEAM36879 1.0    
        conn, cursor = mysql_connection()
        temp_teams = {}
        for d_team in self.d_teams:
            w_g = d_team.split('Source:')[-1].strip().split(' ')[0].strip()
            t_g = d_team.split('Source:')[-1].strip().split(' ')[1].strip()
            if temp_teams.has_key(w_g):
                temp_teams[w_g].append(t_g)
            else:
                temp_teams[w_g] = [t_g]
        for i in temp_teams:
            fi_tms = list(set(temp_teams[i]))
            if len(fi_tms) == 1: continue
            elif len(fi_tms) > 2: self.more_dup_teams.write('%s<>%s\n'%(i, temp_teams[i]))
            print i, fi_tms
            if len(fi_tms) == 2:
                wiki_gid = i 
                tm_gid1, tm_gid2 = fi_tms

                if int(tm_gid1.replace('PL', '').replace('TEAM', '')) > int(tm_gid2.replace('PL', '').replace('TEAM', '')):
                    tm_gid  = tm_gid2
                    del_gid = tm_gid1
                else:
                    tm_gid  = tm_gid1
                    del_gid = tm_gid2

                #self.get_awards_list(pl_gid, del_gid)
                tm_id = self.get_tmid(tm_gid)
                del_id = self.get_tmid(del_gid)
                if del_id and tm_id:

                    self.update_tm_list.write("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n" %(sk_update_query %(tm_id,del_id), \
                    sprts_gams_update_qry %(tm_id, del_id), sprt_gams_res_update_qry %(tm_id, del_id), sprts_gams_res_val_up_qry %(tm_id,del_id), \
                    sprts_tou_upd_qry %(tm_id , del_id), sprts_grps_part_up_qry %(tm_id , del_id), \
                    sprts_par_up_qry %(tm_id , del_id), sprt_teams_del_qry %(del_id), \
                    sprt_par_del_qry %(del_id), team_rostr_up_qry %(tm_id, del_id)))

                    self.del_team_list.write("%s\n" %(del_id))

        conn.close()


if __name__ == '__main__':
    vtv_task_main(DuplicateTeamDeletion)
