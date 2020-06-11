#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import MySQLdb
from vtv_task import VtvTask, vtv_task_main


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
        player_qry = "select P.title, P.sport_id from sports_participants P, sports_players SP where P.participant_type = 'player' and P.id = SP.participant_id and P.title not in ('TBD') group by P.title, SP.birth_date, P.sport_id having count(*) > 1;"
        cursor.execute(player_qry)
        player_rows = cursor.fetchall()
        for row in player_rows:
            p_name = row[0]
            p_sport = row[1]
            pqry = 'select id, gid from sports_participants where title = %s and sport_id = %s'
            vals = (p_name, p_sport)
            cursor.execute(pqry, vals)
            res_rec = cursor.fetchall()
            if len(res_rec) < 2: continue
            temp_list = []
            final_temp_list = []
            for i in res_rec:
#                temp_list.append('\"'+i[-1]+'\"')
                final_temp_list.append(i[-1])
            player_records.append(final_temp_list)
            '''
            for i in temp_list:
                wiki_qry = 'select distinct exposed_gid from GUIDMERGE.sports_wiki_merge where child_gid in (%s)'%','.join(temp_list)
                cursor.execute(wiki_qry)
                wiki_res = cursor.fetchall()
                if len(wiki_res)  == 1:
                    wiki_gid_t = wiki_res[0][0]                        
                    player_records.append((wiki_gid_t, final_temp_list))
            '''
        for i in player_records:
            if len(i) == 2:
                pl_gid1, pl_gid2 = i

                if int(pl_gid1.replace('PL', '').replace('TEAM', '')) > int(pl_gid2.replace('PL', '').replace('TEAM', '')):
                    pl_gid  = pl_gid2
                    del_gid = pl_gid1
                else:
                    pl_gid  = pl_gid1
                    del_gid = pl_gid2

                self.get_awards_list(pl_gid, del_gid)
                pl_id = self.get_plid(pl_gid)
                conn, cursor = mysql_connection()
                pl_del_id = 'select id from sports_participants where gid = "%s"' %(del_gid)
                cursor.execute(pl_del_id)
                del_id = cursor.fetchone()
                if del_id and pl_id:
                    del_id = str(del_id[0])
                    pl_del_sk = 'select source, source_key from sports_source_keys where entity_id= %s and entity_type="participant"' %(del_id)
                    cursor.execute(pl_del_sk)
                    sk_data = cursor.fetchall()

                    for sks_data in sk_data:
                        source = sks_data[0].encode('utf-8')
                        source_key = sks_data[1].encode('utf-8')
                        values = (pl_id, "participant", source, source_key)
                        self.insert_list.write("%s\n" %(SOURCEKEY_INSERT %(values)))
                    roster_delete = self.get_roster(del_id, pl_id)

                    self.update_pl_list.write("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n" %(PAR_DEL_QRY %(del_gid), \
                    PL_DEL_QRY %(del_id), TOU_PAR_QRY %(del_id), SOURCEKEY_DEL_QRY %(pl_id, del_id), \
                    SPORTS_GAMES_PAR %(pl_id , del_id), SPORTS_GAMES_RES %(pl_id , del_id), \
                    GAMES_RESULT_VALUE %(pl_id , del_id), TOU_RESULTS %(del_id), \
                    GROUP_RESULTS %(del_id), AWARDS_RESULTS %(pl_gid, del_gid), SPORTS_RADAR_QRY %(pl_id , del_id), SPORTS_RADAR_TAGS %(pl_id , del_id),roster_delete))

                    self.del_pl_list.write("%s\n" %(del_gid))

            elif len(i) == 3:
                pl_gid1, pl_gid2 , pl_gid3= i

                if int(pl_gid1.replace('PL', '').replace('TEAM', '')) > int(pl_gid2.replace('PL', '').replace('TEAM', '')) and \
                    int(pl_gid1.replace('PL', '').replace('TEAM', '')) > int(pl_gid3.replace('PL', '').replace('TEAM', '')):
                    pl_gid  = pl_gid1
                    del_gid1, del_gid2  = pl_gid2, pl_gid3
                elif int(pl_gid2.replace('PL', '').replace('TEAM', '')) > int(pl_gid3.replace('PL', '').replace('TEAM', '')) and \
                    int(pl_gid2.replace('PL', '').replace('TEAM', '')) > int(pl_gid1.replace('PL', '').replace('TEAM', '')):
                    pl_gid = pl_gid2
                    del_gid1, del_gid2 = pl_gid1, pl_gid3
                else:
                    pl_gid  = pl_gid3
                    del_gid1, del_gid2 = pl_gid2, pl_gid1

                self.get_awards_list(pl_gid, del_gid1)
                self.get_awards_list(pl_gid, del_gid2)
                pl_id = self.get_plid(pl_gid)
                conn, cursor = mysql_connection()
                if del_gid1 and pl_id:
                    pl_del_id = 'select id from sports_participants where gid = "%s"' %(del_gid1)
                    cursor.execute(pl_del_id)
                    del_id = cursor.fetchone()
                    
                    del_id = str(del_id[0])
                    pl_del_sk = 'select source, source_key from sports_source_keys where entity_id= %s and entity_type="participant"' %(del_id)
                    cursor.execute(pl_del_sk)
                    sk_data = cursor.fetchall()

                    for sks_data in sk_data:
                        source = sks_data[0].encode('utf-8')
                        source_key = sks_data[1].encode('utf-8')
                        values = (pl_id, "participant", source, source_key)
                        self.insert_list.write("%s\n" %(SOURCEKEY_INSERT %(values)))
                    roster_delete = self.get_roster(del_id, pl_id)

                    self.update_pl_list.write("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n" %(PAR_DEL_QRY %(del_gid1), \
                    PL_DEL_QRY %(del_id), TOU_PAR_QRY %(del_id), SOURCEKEY_DEL_QRY %(pl_id, del_id), \
                    SPORTS_GAMES_PAR %(pl_id , del_id), SPORTS_GAMES_RES %(pl_id , del_id), \
                    GAMES_RESULT_VALUE %(pl_id , del_id), TOU_RESULTS %(del_id), \
                    GROUP_RESULTS %(del_id), AWARDS_RESULTS %(pl_gid, del_gid1), SPORTS_RADAR_QRY %(pl_id , del_id), SPORTS_RADAR_TAGS %(pl_id , del_id),roster_delete))
                    self.del_pl_list.write("%s\n" %(del_gid1))
                if del_gid2 and pl_id:
                    pl_del_id = 'select id from sports_participants where gid = "%s"' %(del_gid2)
                    cursor.execute(pl_del_id)
                    del_id = cursor.fetchone()

                    del_id = str(del_id[0])
                    pl_del_sk = 'select source, source_key from sports_source_keys where entity_id= %s and entity_type="participant"' %(del_id)
                    cursor.execute(pl_del_sk)
                    sk_data = cursor.fetchall()

                    for sks_data in sk_data: 
                        source = sks_data[0].encode('utf-8')
                        source_key = sks_data[1].encode('utf-8')
                        values = (pl_id, "participant", source, source_key)
                        self.insert_list.write("%s\n" %(SOURCEKEY_INSERT %(values)))
                    roster_delete = self.get_roster(del_id, pl_id)

                    self.update_pl_list.write("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n" %(PAR_DEL_QRY %(del_gid2), \
                    PL_DEL_QRY %(del_id), TOU_PAR_QRY %(del_id), SOURCEKEY_DEL_QRY %(pl_id, del_id), \
                    SPORTS_GAMES_PAR %(pl_id , del_id), SPORTS_GAMES_RES %(pl_id , del_id), \
                    GAMES_RESULT_VALUE %(pl_id , del_id), TOU_RESULTS %(del_id), \
                    GROUP_RESULTS %(del_id), AWARDS_RESULTS %(pl_gid, del_gid1), SPORTS_RADAR_QRY %(pl_id , del_id), SPORTS_RADAR_TAGS %(pl_id , del_id),roster_delete))
                    self.del_pl_list.write("%s\n" %(del_gid1))

        conn.close()


if __name__ == '__main__':
    vtv_task_main(DuplicatePlayerDeletion)
