#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import MySQLdb
from vtv_task import VtvTask, vtv_task_main
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def mysql_connection():
    conn = MySQLdb.connect(host="10.28.218.81", user="veveo",passwd='veveo123', db="SPORTSDB", charset='utf8', use_unicode=True)
#    conn = MySQLdb.connect(host="10.28.216.45", user="veveo",passwd='veveo123', db="SPORTSDB_DEV", charset='utf8', use_unicode=True)
    cursor = conn.cursor()
    return conn, cursor

def mysql_awarsdb():
    #con = MySQLdb.connect(host="10.4.17.32", user="root", db="AWARDS")
    con =  MySQLdb.connect(host="10.28.218.81", user="veveo", passwd='veveo123', db="AWARDS", charset='utf8', use_unicode=True)
    cursor = con.cursor()
    return con, cursor

PAR_DEL_QRY         = 'delete from sports_participants where gid = "%s" limit 1;'
PL_DEL_QRY          = 'delete from sports_players where participant_id = %s limit 1;'
TOU_PAR_QRY         = 'delete from sports_tournaments_participants where participant_id = %s;'
SOURCEKEY_DEL_QRY   = 'update sports_source_keys set entity_id = %s where entity_id = %s and entity_type = "participant";'
SOURCEKEY_INSERT    = 'insert into sports_source_keys (entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, "%s", now(), now()) on duplicate key update modified_at = now();'
SPORTS_GAMES_PAR    = 'update sports_games_participants set participant_id = %s where participant_id = %s;'
SPORTS_GAMES_RES    = 'update sports_games_results set participant_id = %s where participant_id = %s;'
GAMES_RESULT_VALUE  = 'update sports_games_results set result_value = %s where result_value = %s;'
SPORTS_ROSTER       = 'update sports_roster set player_id= %s where player_id= %s;'
TOU_RESULTS         = 'delete from sports_tournaments_results where participant_id = %s;'
GROUP_RESULTS       = 'delete from sports_groups_results where participant_id = %s;'
SPORTS_RADAR_QRY    = 'update sports_radar_images_mapping set entity_id = %s where entity_id = %s and entity_type = "player";'
SPORTS_RADAR_TAGS   = 'update sports_radar_tags set entity_id = %s where entity_type = "player" and entity_id = %s;'
AWARDS_RESULTS      = 'update sports_awards_results set participants = "%s" where participants = "%s";'

class DuplicatePlayerDeletion(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
#        self.players_list    = open("sports_multiple_merge.txt", "r+")
        
        self.del_pl_list     = open('del_pl_list.txt', 'w')
        self.update_pl_list  = open('update_pl_list.txt', 'w')
        self.awards_list     = open('awards_list.txt', 'w')
        self.insert_list     = open('insert_list.txt', 'w')
        self.test_file = open('player_test_file.txt', 'w')
 

    def get_awards_list(self, pl_gid, del_gid):
        con, cursor = mysql_awarsdb()
        update_part = 'update sports_awards_history set participants= "%s" where participants= "%s"' %(pl_gid, del_gid)
        self.awards_list.write("%s\n" %(update_part))
        con.close()

    def get_plid(self, pl_gid):
        conn, cursor = mysql_connection()
        pl_id_query = 'select id from sports_participants where gid = "%s"' %(pl_gid)
        cursor.execute(pl_id_query)
        pl_id = cursor.fetchone()
        if pl_id:
            pl_id = str(pl_id[0])
        conn.close()
        return pl_id

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
        conn, cursor = mysql_connection()
        player_records = []
        player_qry = "select P.title, P.sport_id, SP.birth_date from sports_participants P, sports_players SP where P.participant_type = 'player' and P.id = SP.participant_id and P.title not in ('TBD') group by P.title, P.sport_id having count(*) > 1;"
        cursor.execute(player_qry)
        player_rows = cursor.fetchall()
        for row in player_rows:
            p_name = row[0]
            p_sport = row[1]
            p_dob = row[2]
            pqry = 'select a.id, a.gid, a.title, b.birth_date from sports_participants a, sports_players b where a.title = %s and a.sport_id = %s and a.id = b.participant_id'
            vals = (p_name, p_sport)
            cursor.execute(pqry, vals)
            res_rec = cursor.fetchall()
            if len(res_rec) < 2: continue
            temp_list = []
            final_temp_list = []
            for i in res_rec:
                titl = i[2].encode('utf-8')
                mqry = 'select exposed_gid from GUIDMERGE.sports_wiki_merge where child_gid = %s'
                m_vals = (i[1])
                cursor.execute(mqry, m_vals)
                _gid = cursor.fetchone()
                self.test_file.write('%s<>%s<>%s<>%s<>%s<>%s\n'%(i[0],p_sport,i[1],_gid,titl, i[3]))
            self.test_file.write('**************************************************************************************************\n')
        self.test_file.flush()
            
        conn.close()


if __name__ == '__main__':
    vtv_task_main(DuplicatePlayerDeletion)
