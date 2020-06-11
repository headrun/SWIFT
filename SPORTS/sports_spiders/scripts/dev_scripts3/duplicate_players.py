import MySQLdb
import datetime
from vtv_task import VtvTask, vtv_task_main


PAR_DEL_QRY         = 'delete from sports_participants where gid = "%s" limit 1;'
PL_DEL_QRY          = 'delete from sports_players where participant_id = %s limit 1;'
TOU_PAR_QRY         = 'delete from sports_tournaments_participants where participant_id = %s;'
SOURCEKEY_DEL_QRY   = 'delete from sports_source_keys where entity_id = %s and entity_type = "participant";'
SOURCEKEY_INSERT    = 'insert into sports_source_keys (entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, "%s", "%s", "%s", now(), now()) on duplicate key update modified_at = now();'
SPORTS_GAMES_PAR    = 'update sports_games_participants set participant_id = %s where participant_id = %s;'
SPORTS_GAMES_RES    = 'update sports_games_results set participant_id = %s where participant_id = %s;'
GAMES_RESULT_VALUE  = 'update sports_games_results set result_value = %s where result_value = %s;'
SPORTS_ROSTER       = 'update sports_roster set player_id= %s where player_id= %s;'
TOU_RESULTS         = 'delete from sports_tournaments_results where participant_id = %s;'
GROUP_RESULTS       = 'delete from sports_groups_results where participant_id = %s;'
AWARDS_RESULTS      = 'update sports_awards_results set participants = "%s" where participants = "%s";'

class DuplicatePlayerDeletion(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.sports_ip       = '10.4.18.183'
        self.sports_db       = 'SPORTSDB'
        self.merge_ip        = '10.4.2.187'
        self.merge_db        = 'GUIDMERGE'
        self.players_wiki_merge = {}
        self.deleted_dup_pl  = open('deleted_dup_pl', 'a+')
        self.players_not_in_sportsdb = open('players_not_in_sportsdb', 'a+')
        self.today           = datetime.datetime.now().date()
        self.del_pl_list     = open('del_pl_list_%s' % self.today, 'a+')
        self.update_pl_list  = open('update_pl_list_%s' % self.today, 'a+')
        self.awards_list     = open('awards_list_%s' % self.today, 'a+')
        self.awards_dict     = {}
        self.insert_list     = open('insert_list_%s' % self.today, 'a+')

    def remove_gids_players_not_in_sportsdb(self):
        self.open_cursor(self.merge_ip, self.merge_db)
        rows = open('players_not_in_sportsdb', 'r')
        for row in rows:
            query = 'delete from sports_wiki_merge where child_gid=%s limit 1'
            self.cursor.execute(query, row.strip())
            print 'removed gid', row.strip()

    def get_awards_list(self):
        db_ip = '10.4.2.187'
        db_name = 'AWARDS'
        self.open_cursor(db_ip, db_name)
        for pl_gid, del_gid in self.awards_dict.iteritems():
            query = 'select id from sports_awards_history where participants=%s'
            self.cursor.execute(query, del_gid)
            result = self.cursor.fetchone()
            if result:
                awards_history = 'update sports_awards_history set participants= "%s" where participants= "%s"' %(pl_gid, del_gid)
                self.cursor.execute(awards_history)

    def get_plid(self, pl_gid):
        pl_id_query = 'select id from sports_participants where gid = "%s"' %(pl_gid)
        self.cursor.execute(pl_id_query)
        pl_id = self.cursor.fetchone()
        if pl_id:
            pl_id = str(pl_id[0])
        return pl_id

    def get_roster(self, del_id, pl_id):
        roster_query = 'select team_id, status from sports_roster where player_id = %s' %(del_id)
        self.cursor.execute(roster_query)
        pl_id = pl_id
        del_id = del_id
        roster_data = self.cursor.fetchall()
        roster_delete = ''
        if roster_data:
            for roster_ in roster_data:
                team_id = str(roster_[0])
                status = str(roster_[1])
                roster_active = 'select id from sports_roster where player_id = %s and team_id = %s and status = "%s"' %(pl_id, team_id, status)
                self.cursor.execute(roster_active)
                roster_act = self.cursor.fetchall()
                if not roster_act:
                    roster_delete = SPORTS_ROSTER %(pl_id , del_id)
                    #self.cursor.execute(SPORTS_ROSTER %(pl_id , del_id))
        return roster_delete

    def get_player_gids(self):
        self.open_cursor(self.merge_ip, self.merge_db)
        query = 'select exposed_gid, child_gid from sports_wiki_merge'
        self.cursor.execute(query)
        for row in self.get_fetchmany_results():
            wiki_gid, sports_gid = row
            if 'PL' in sports_gid:
                self.players_wiki_merge.setdefault(wiki_gid, []).append(sports_gid)


    def run_main(self):
        self.get_player_gids()
        self.open_cursor(self.sports_ip, self.sports_db)
        #for data in self.players_wiki_merge.values():
        for data in open('sample_dp', 'r'):
            data = [rec.strip() for rec in data.replace('[', '').replace(']', '').strip().split(',')]
            if len(data) < 2:
                continue
            if len(data) > 2:
                data = data[:2]
            pl_gid1, pl_gid2 = data

            if int(pl_gid1.replace('PL', '').replace('TEAM', '')) > int(pl_gid2.replace('PL', '').replace('TEAM', '')):
                pl_gid  = pl_gid2
                del_gid = pl_gid1
            else:
                pl_gid  = pl_gid1
                del_gid = pl_gid2

            self.deleted_dup_pl.write('%s\n' % '<>'.join(data))

            awards_history = '%s<>%s' % (pl_gid, del_gid)
            self.awards_list.write("%s\n" % awards_history)

            self.awards_dict[pl_gid] = del_gid
            pl_id = self.get_plid(pl_gid)
            if not pl_id:
                self.players_not_in_sportsdb.write('%s\n' % pl_gid)

            pl_del_id = 'select id from sports_participants where gid = "%s"' %(del_gid)
            self.cursor.execute(pl_del_id)
            del_id = self.cursor.fetchone()
            if not del_id:
                self.players_not_in_sportsdb.write('%s\n' % del_gid)
                continue

            if del_id and pl_id:
                del_id = str(del_id[0])
                pl_del_sk = 'select source, source_key from sports_source_keys where entity_id= %s and entity_type="participant"' %(del_id)
                self.cursor.execute(pl_del_sk)
                sk_data = self.cursor.fetchall()
                for sks_data in sk_data:
                    source = str(sks_data[0])
                    source_key = str(sks_data[1])
                    values = (pl_id, "participant", source, source_key)
                    self.cursor.execute(SOURCEKEY_INSERT % values)
                    self.insert_list.write("%s\n" %(SOURCEKEY_INSERT %(values)))
                roster_delete = 'delete from sports_roster where player_id = %s;' %(del_id)
                roster_update = self.get_roster(del_id, pl_id)

                #sports_games_participants table ---
                query = 'select game_id from sports_games_participants where participant_id=%s'
                self.cursor.execute(query % del_id)
                result = self.cursor.fetchall()
                if result:
                    for row in result:
                        game_id = row[0]
                        query = 'select game_id from sports_games_participants where game_id=%s and participant_id=%s'
                        self.cursor.execute(query , (game_id, pl_id))
                        res = self.cursor.fetchone()
                        if res:
                            query = 'delete from sports_games_participants where game_id=%s and participant_id=%s'
                            self.cursor.execute(query , (game_id, del_id))
                        else:
                            query = 'update sports_games_participants set participant_id=%s where game_id=%s and participant_id=%s'
                            self.cursor.execute(query %(pl_id , game_id, del_id))
                #sports_games_results table ----
                query = 'select game_id from sports_games_results where participant_id=%s' % del_id
                self.cursor.execute(query)
                result = self.cursor.fetchall()
                if result:
                    for row in result:
                        game_id = row[0]
                        query = 'select game_id from sports_games_results where game_id=%s and participant_id=%s'
                        self.cursor.execute(query , (game_id, pl_id))
                        res = self.cursor.fetchone()
                        if res:
                            query = 'delete from sports_games_results where game_id=%s and participant_id=%s'
                            self.cursor.execute(query , (game_id, del_id))
                        else:
                            query = 'update sports_games_results set participant_id=%s where game_id=%s and participant_id=%s'
                            self.cursor.execute(query %(pl_id , game_id, del_id))
                #checking result_value field in sports_games_results
                query = 'select id from sports_games_results where result_type="winner" and result_value=%s'
                self.cursor.execute(query, (del_id))
                result = self.cursor.fetchone()
                if result:
                    query = 'update sports_games_results set result_value=%s where result_type="winner" and result_value=%s'
                    self.cursor.execute(query, (pl_id, del_id))
                #sports_tournaments_participants table
                query = 'select id from sports_tournaments_participants where participant_id=%s' % del_id
                self.cursor.execute(query)
                result = self.cursor.fetchone()
                if result:
                    self.cursor.execute(TOU_PAR_QRY %(del_id))

                #sports_awards_results
                query = 'select id from sports_awards_results where participants="%s"' % del_gid
                self.cursor.execute(query)
                result = self.cursor.fetchone()
                if result:
                    self.cursor.execute(AWARDS_RESULTS %(pl_gid, del_gid))

                #sports_tournaments_results
                query = 'select tournament_id from sports_tournaments_results where participant_id=%s'
                self.cursor.execute(query, del_id)
                result = self.cursor.fetchall()
                for row in result:
                    tou_id = row[0]
                    query = 'select tournament_id from sports_tournaments_results where tournament_id=%s and participant_id=%s'
                    self.cursor.execute(query, (tou_id, pl_id))
                    res = self.cursor.fetchone()
                    if res:
                        self.cursor.execute(TOU_RESULTS % del_id)
                    else:
                        query = 'update sports_tournaments_results set participant_id=%s where participant_id=%s and tournament_id=%s'
                        self.cursor.execute(query, (pl_id, del_id, tou_id))

                #checking result_type filed in sports_tournaments_results
                query = 'select id from sports_tournaments_results where result_type="winner" and result_value=%s'
                self.cursor.execute(query, del_id)
                result = self.cursor.fetchall()
                for row in result:
                    id_ = row[0]
                    query = 'update sports_tournaments_results set result_value=%s where id=%s'
                    values = (pl_id, id_)
                    self.cursor.execute(query, values)
                
                self.cursor.execute(PAR_DEL_QRY %(del_gid))
                self.cursor.execute(PL_DEL_QRY %(del_id))
                self.cursor.execute(SOURCEKEY_DEL_QRY %(del_id))
                self.cursor.execute(roster_delete)
                if roster_update:
                    self.cursor.execute(roster_update)

                #checking sports_groups_participants table
                query = 'select id from sports_groups_participants where participant_id=%s'
                self.cursor.execute(query, del_id)
                result = self.cursor.fetchone()
                if result:
                    query = 'update sports_groups_participants set participant_id=%s where participant_id=%s'
                    values = (pl_id, del_id)
                    self.cursor.execute(query, values)

                #checking sports_groups_result table
                query = 'select id from sports_groups_results where participant_id=%s'
                self.cursor.execute(query, del_id)
                result = self.cursor.fetchone()
                if result:
                    query = 'update sports_groups_results set participant_id=%s where participant_id=%s'
                    values = (pl_id, del_id)
                    self.cursor.execute(query, values)
                
                print 'Deleted player >>>' , del_gid
                self.update_pl_list.write("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n" %(PAR_DEL_QRY %(del_gid), \
                PL_DEL_QRY %(del_id), TOU_PAR_QRY %(del_id), SOURCEKEY_DEL_QRY %(del_id), \
                SPORTS_GAMES_PAR %(pl_id , del_id), SPORTS_GAMES_RES %(pl_id , del_id), \
                GAMES_RESULT_VALUE %(pl_id , del_id), TOU_RESULTS %(del_id), \
                GROUP_RESULTS %(del_id), AWARDS_RESULTS %(pl_gid, del_gid), roster_delete, roster_update))

                self.del_pl_list.write("%s\n" %(del_gid))
        self.get_awards_list()
        self.remove_gids_players_not_in_sportsdb()

if __name__ == '__main__':
    vtv_task_main(DuplicatePlayerDeletion)
