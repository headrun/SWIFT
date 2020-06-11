from scrapy.http import Request
from scrapy.selector import Selector
import unicodedata
from StringUtil import cleanString
from difflib import SequenceMatcher
import MySQLdb
import datetime
import urllib
import time
from vtv_db import get_mysql_connection
from vtv_utils import VTV_CONTENT_VDB_DIR, copy_file, execute_shell_cmd, make_dir_list
from vtv_task import VtvTask, vtv_task_main
from redis_utils import get_redis_data


GAME = "7"
GAME_ID = '7'
PLAYERS_PERMUTATIONS = {}
PLAYERS_IN_DB        = {}
TEAMS_IN_DB          = {}
SPACE = ' '
THRESHOLD   = 0.70


def permutations(iterable, r=None):
    # permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
    # permutations(range(3)) --> 012 021 102 120 201 210
    pool = tuple(iterable)
    n = len(pool)
    if r is None:
        r = n
   
    if r > n:
        return
    indices = range(n)
    cycles = range(n, n-r, -1)
    yield tuple(pool[i] for i in indices[:r])
    while n:
        for i in reversed(range(r)):
            cycles[i] -= 1
            if cycles[i] == 0:
                indices[i:] = indices[i+1:] + indices[i:i+1]
                cycles[i] = n - i
            else:
                j = cycles[i]
                indices[i], indices[-j] = indices[-j], indices[i]
                yield tuple(pool[i] for i in indices[:r])
                break
        else:
            return


def get_player_permutations(existing_players):
    for existing_player in existing_players:
        if SPACE in existing_player:
            existing_player = cleanString(existing_player)
            existing_player_list = existing_player.split(SPACE)
            if len(existing_player_list) <= 5:
                existing_player_set = [SPACE.join(i) for i in \
                                      permutations(existing_player_list)]
            else:
                existing_player_set = [existing_player]

        else:
            existing_player_set = [existing_player]

        PLAYERS_PERMUTATIONS[existing_player] = existing_player_set


def find_players_to_add(db_players, tou_players):
    titles_present = {}
    mstly_matched_titles = {}
    for player in tou_players:
        player = player
        if player in db_players:
            titles_present[player] = (player, 'EXACTLY PRESENT')
            continue
        check_flag = False
        seqmatcher = SequenceMatcher(None, player)
        for existing_player in db_players:
            if SPACE in player and SPACE in existing_player:
                existing_player_set = PLAYERS_PERMUTATIONS.\
                                      get(existing_player, [existing_player])
            else:
                existing_player_set = [existing_player]

            for ex_player in existing_player_set:
                seqmatcher.set_seq2(ex_player)
                if seqmatcher.ratio() >= THRESHOLD:
                    mstly_matched_titles[player] = existing_player
                    titles_present[player] = (player, 'MOSTLY PRESENT')
                    check_flag = True
                    break

            if check_flag: break
        else:
            titles_present[player] = (player, 'NOT PRESENT')
    mostly_present = [player for player, details in titles_present.items() \
                     if details[1] == 'MOSTLY PRESENT']
    exactly_present = [player for player, details in titles_present.items() \
                     if details[1] == 'EXACTLY PRESENT']
    not_present = [player for player, details in titles_present.items() \
                     if details[1] == 'NOT PRESENT']
    return exactly_present, mostly_present, not_present, mstly_matched_titles

def check_title(self, name, dob, match_title):
    name = name
    match_title = match_title
    self.cursor.execute(PL_NAME_QUERY % (name, GAME, dob))
    pl_id = self.cursor.fetchone()

    if (not pl_id and dob == "0000-00-00 00:00:00"):
        PL_QRY = 'select id from sports_participants where title="%s" and sport_id="%s"'
        self.cursor.execute(PL_QRY % (name, GAME))
        pl_id = self.cursor.fetchone()

    if not pl_id:
        dob = "0000-00-00 00:00:00"
        self.cursor.execute(PL_NAME_QUERY % (name, GAME, dob))
        pl_id = self.cursor.fetchone()

    if not pl_id:
        PL_QRY = 'select id from sports_participants where aka="%s" and sport_id="%s"'
        self.cursor.execute(PL_QRY % (name, GAME))
        pl_id = self.cursor.fetchone()

    if not pl_id:
        PL_QRY = 'select id from sports_participants where title like %s and sport_id=%s'
        values = ('%' + name + '%', GAME)
        self.cursor.execute(PL_QRY, values)
        pl_id = self.cursor.fetchone()

    if match_title.title() == name and not pl_id:
        PL_QRY = 'select id from sports_participants where title="%s" and sport_id="%s"'
        self.cursor.execute(PL_QRY % (name, GAME))
        pl_id = self.cursor.fetchone()
        
    return pl_id

PL_NAME_QUERY = 'select P.id from sports_participants P, sports_players PL where P.title="%s" and P.sport_id="%s" and P.id=PL.participant_id and PL.birth_date="%s"'

TEAM_LINK = 'https://api.sportradar.us/%s/teams/%s/profile.json?api_key=%s'

INST_QRY = 'insert into SPORTSRADARDB.sports_radar_merge(radar_id, sportsdb_id, type, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at = now()'

class SportsRadarMerge(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)

        self.conn = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSDB', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()


    def get_db_players(self):
        query = 'select id, title from sports_participants where participant_type = "player" and sport_id=7'
        self.cursor.execute(query)
        players_data = self.cursor.fetchall()
        for player_data in players_data:
            id_, title = player_data
            title = title.strip().lower()
            PLAYERS_IN_DB[title] = int(id_)
        get_player_permutations(PLAYERS_IN_DB.keys())

    def get_db_teams(self):
        query = 'select id, title from sports_participants where participant_type = "team"'
        self.cursor.execute(query)
        players_data = self.cursor.fetchall()
        for player_data in players_data:
            id_, title = player_data
            title = title.strip().lower()
            TEAMS_IN_DB[title] = int(id_)
        get_player_permutations(TEAMS_IN_DB.keys())


    def players_data(self):
        sel_qry = 'select id, title, birth_date from SPORTSRADARDB.sports_players where reference_url like "%/soccer-p2/%"'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for pl_data in data:
            pl_id = str(pl_data[0])
            pl_name = str(pl_data[1])
            birthdate = str(pl_data[2])

            if birthdate == "None":
                birthdate = "0000-00-00 00:00:00"

            match_title = ''
            pl_ids = check_title(self, pl_name, birthdate, match_title)
            if pl_ids:
                if len(pl_ids) == 1:
                    values = (pl_id, str(pl_ids[0]), 'player')
                    self.cursor.execute(INST_QRY, values)
                else:
                    print "More than one player", pl_name
       
            else:
                exactly_present, mostly_present, not_present, mstly_matched_titles = find_players_to_add(PLAYERS_IN_DB.keys(), [pl_name])
                if exactly_present or mostly_present or mstly_matched_titles:
                    name = mstly_matched_titles[pl_name]
                    pl_ids = check_title(self, name, birthdate, pl_name)
                    if pl_ids:
                        if len(pl_ids) == 1:
                            values = (pl_id, str(pl_ids[0]), 'player')
                            self.cursor.execute(INST_QRY, values)
                        else:
                            print "More than one player", pl_name
                    else:
                        print "birth_date id not matching", pl_name, name, birthdate
                else:
                    print "Player name not Mactched", pl_name, birthdate

    def teams_data(self):
        sel_qry = "select id, title, tournament_id from SPORTSRADARDB.sports_teams where tournament_id not in ('4353138d-4c22-4396-95d8-5f587d2df25c', 'fd560107-a85b-4388-ab0d-655ad022aff7', '2fa448bc-fc17-4d3d-be03-e60e080fdc26', 'NFL')"
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for tm_data in data:
            tm_id = str(tm_data[0])
            tm_name = str(tm_data[1]).replace('Los Angeles Angels', 'Los Angeles Angels of Anaheim').replace(' FC', ' F.C.')
            
            sp_qry = 'select id from sports_participants where title=%s and participant_type="team" and sport_id=7'
            values = (tm_name)
            self.cursor.execute(sp_qry, values)
            data = self.cursor.fetchone()
            if not data:
                sp_qry = 'select id from sports_participants where aka=%s and participant_type="team" and sport_id=7'
                values = (tm_name) 
                self.cursor.execute(sp_qry, values)
                data = self.cursor.fetchone()

            if not data:
                sp_qry = 'select id from sports_participants where title like %s and participant_type="team" and sport_id=7'
                values = ('%' + tm_name + '%') 
                self.cursor.execute(sp_qry, values)
                data = self.cursor.fetchone()


            if data:
                values = (str(tm_id), str(data[0]), 'team')
                self.cursor.execute(INST_QRY, values)
            else:
                exactly_present, mostly_present, not_present, mstly_matched_titles = find_players_to_add(TEAMS_IN_DB.keys(), [tm_name])
                if exactly_present or mostly_present or mstly_matched_titles:
                    print "mostly_present", mstly_matched_titles
                    name = mstly_matched_titles[tm_name]
                    sp_qry = 'select id from sports_participants where title=%s and participant_type="team" and sport_id=7'
                    values = (name)
                    self.cursor.execute(sp_qry, values)
                    data = self.cursor.fetchone()

                    if data:
                        values = (str(tm_id), str(data[0]), 'team')
                        self.cursor.execute(INST_QRY, values)
                else:
                    print tm_name, str(tm_data[2])     

    def groups_data(self):
        sel_qry = 'select id, group_name from SPORTSRADARDB.sports_tournaments_groups'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for gp_data in data:
            gp_id = str(gp_data[0])
            gp_name = str(gp_data[1]).replace(',', '').strip()

            if "Group" not in gp_name:
                continue

            sp_qry = 'select id from sports_tournaments_groups where group_name=%s and sport_id=7'
            values = (gp_name)
            self.cursor.execute(sp_qry, values)
            data = self.cursor.fetchone()
            if data:
                values = (str(gp_id), str(data[0]), 'group')
                self.cursor.execute(INST_QRY, values)
            else:
                print gp_name

    def tournaments_data(self):
        sel_qry = 'select id, title from SPORTSRADARDB.sports_tournaments'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for to_data in data:
            to_id = str(to_data[0])
            to_name = str(to_data[1])

            sp_qry = 'select id from sports_tournaments where title=%s'
            values = (to_name)
            self.cursor.execute(sp_qry, values)
            data = self.cursor.fetchone()

            if not data:
                sp_qry = 'select id from sports_tournaments where aka=%s'
                values = (to_name)
                self.cursor.execute(sp_qry, values)
                data = self.cursor.fetchone()
            if data:
                values = (str(to_id), str(data[0]), 'tournament')
                self.cursor.execute(INST_QRY, values)
            else:
                print to_name

        
    def run_main(self):
        self.get_db_players() 
        self.players_data()
        #self.get_db_teams()
        #self.teams_data()
        #self.groups_data()
        #self.tournaments_data()



if __name__ == '__main__':
    vtv_task_main(SportsRadarMerge)






