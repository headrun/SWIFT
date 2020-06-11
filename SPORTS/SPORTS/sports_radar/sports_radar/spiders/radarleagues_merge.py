#!/usr/bin/env python
# -*- coding: utf-8 -*- 
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
        PL_QRY = 'select P.id from sports_participants P, sports_players PL where P.aka="%s" and P.sport_id="%s" and P.id=PL.participant_id and PL.birth_date="%s"'

        #PL_QRY = 'select id from sports_participants where aka="%s" and sport_id="%s"'
        self.cursor.execute(PL_QRY % (name, GAME, dob))
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
        query = 'select id, title from sports_participants where participant_type = "team" and sport_id=2 and title not like "%women%"'
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


    def ncaa_teams(self):
        #sel_qry = "select id, title, tournament_id, sport_id, market from SPORTSRADARDB.sports_teams where tournament_id in ('ncaafb')"
        sel_qry = "select id, title, tournament_id, sport_id, market from SPORTSRADARDB.sports_teams where tournament_id in ('ncaamb')"
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for tm_data in data:
            tm_id = str(tm_data[0])
            sport_id = tm_data[3]
            tm_name = tm_data[1]
            tm_market = tm_data[4].replace('Pennsylvania', 'Penn')
            #tm_title = tm_market + ' ' + tm_name + ' football'
            tm_title = tm_market + " " + tm_name + " men's basketball"
            tm_title = tm_title.replace('Central Connecticut State', 'Central Connecticut').replace('Lehigh Hawks', 'Lehigh Mountain Hawks').replace('Virginia-Wise', 'Virginia Wise').replace('Charleston (WV)', 'Charleston Univ').replace('Minnesota State Moorhead', 'Minnesota State Univ.-Moorhead').replace('Gustavus', 'Gustavus Adolphus Golden').replace('St. Johns Johnnies', "St. John's (Minn.) Johnnies").replace('St. Thomas Tommies', "St. Thomas (Minn.) Tommies").replace("Thomas (TX)", "St. Thomas (TX)").replace('Mountain Lions Concord', 'Concord Mountain Lions').replace('Concordia University-Paul', 'Concordia, St. Paul').replace('Bonaventure', 'St. Bonaventure').replace('College of Charleston', 'Charleston').replace('Virginia Commonwealth', 'VCU').replace('Central Connecticut State', 'Central Connecticut').replace('Lehigh Hawks', 'Lehigh Mountain Hawks').replace('Virginia-Wise', 'Virginia Wise').replace('Charleston (WV)', 'Charleston Univ').replace('Minnesota State Moorhead', 'Minnesota State Univ.-Moorhead').replace('Gustavus', 'Gustavus Adolphus Golden').replace('St. Johns Johnnies', "St. John's (Minn.) Johnnies").replace('St. Thomas Tommies', "St. Thomas (Minn.) Tommies").replace('Pennsylvania', 'Penn').replace('Miami (OH)', 'Miami').replace('Miami (FL) ', 'Miami ').replace('Francis (PA)', 'Saint Francis').replace('Nicholls', 'Nicholls State').replace('Delaware', "Delaware Fightin'").replace('Tennessee-Martin', 'UT Martin').replace('FIU Golden', 'FIU').replace('Anselm', 'St. Anselm').replace('North Carolina State', 'NC State').replace('Virginia Military', 'VMI').replace('Massachusetts Minutemen', 'UMass Minutemen').replace('Brigham Young', 'BYU').replace("Delaware Fightin' State", "Delaware State Hornets").replace('Southern University', 'Southern').replace('Southern Methodist', 'SMU').replace('Mississippi Rebels', 'Ole Miss Rebels').replace('Eastern Tennessee', 'East Tennessee').replace('Hornets Hornets', 'Hornets').replace('Boston Eagles', 'Boston College Eagles').replace('Northern Colorado', 'Northern Colorado Bears').replace('Citadel', 'The Citadel').replace('Texas-San Antonio', 'UTSA').replace('Idaho Coyotes', 'Idaho Yotes').replace('Brevard College', 'Brevard').replace('Limestone College', 'Limestone').replace('FK Qarabag', 'Qarabag FK').replace('Apoel Nicosia', 'APOEL FC').replace('Sporting Lisbon', 'Sporting Clube de Portugal').replace('Sporting Braga', 'S.C. Braga').replace('Partizan Belgrade', 'FK Partizan').replace('Dynamo Kiev', 'FC Dynamo Kyiv').replace('FC Copenhagen', 'F.C. Copenhagen').replace('Slavia Prague', 'SK Slavia Praha').replace('Steaua Bucharest', 'FC Steaua Bucuresti').replace('Zenit St Petersburg', 'FC Zenit').replace('Crvena Zvezda', 'Red Star Belgrade').replace('SV Zulte Waregem', 'S.V. Zulte Waregem').replace('American University', 'American').replace('Blue Mountain College', 'Blue Mountain').replace('Vikings Augustana (IL)', 'Augustana (IL) Vikings').replace('Valley University', 'Valley').replace('California Riverside', 'UC Riverside').replace("Utah Runnin' Utes", 'Utah Utes').replace('Charleston', 'College of Charleston').replace('Morgan State Golden Bear', 'Morgan State Bear').replace("Mount Mary's", "Mount St. Mary's").replace("Cal State Bakersfield", "CSU Bakersfield").replace('UC-Irvine', 'UC Irvine').replace('Loyola (MD)', 'Loyola').replace('Nebraska Omaha Mavericks', 'Omaha Mavericks').replace('Fighting Sioux', 'Fighting Hawks').replace('University of Maryland Baltimore County', 'UMBC Retrievers').replace('City College of New York', 'CCNY').replace("John's Red Storm", "St. John's Red Storm").replace("Joseph's (PA) Hawks", "Saint Joseph's Hawks").replace("Panthers Kentucky Wesleyan", "Kentucky Wesleyan Panthers").replace("Peter's Peacocks", "Saint Peter's Peacocks").replace("Shore", "Shore Hawks").replace('Bears Bears', 'Bears').replace('North Carolina Greensboro', 'UNC Greensboro').replace('IPFW Mastodons', 'Fort Wayne Mastodons')

            if "Point University" in tm_title or 'Union College Bulldogs' in tm_title or 'Bethany College Swedes' in tm_title or 'Bluefield College Rams' in tm_title or 'Cumberland (TN) Bulldogs' in tm_title or 'Concordia (AL) Hornets' in tm_title:
                continue
            sp_qry = 'select id from sports_participants where title=%s and participant_type="team" and sport_id=%s'
            values = (tm_title, sport_id)
            self.cursor.execute(sp_qry, values)
            data = self.cursor.fetchone()
            if not data:
                tm_title =tm_title.replace(' College', '').replace(' University', '').strip().replace(' Univ ', ' Univ. ').replace('(TX)', '(Texas)').replace('International', 'Intl')
                values = (tm_title, sport_id)
                self.cursor.execute(sp_qry, values)
                data = self.cursor.fetchone()

            if not data:
                sp_qry = 'select id from sports_participants where aka=%s and participant_type="team" and sport_id=%s'
                values = (tm_title, sport_id)
                self.cursor.execute(sp_qry, values)
                data = self.cursor.fetchone()
            if not data:
                #tm_title = tm_id + " " + tm_name + ' football'
                tm_title = tm_id + " " + tm_name + " men's basketball"
                sp_qry = 'select id from sports_participants where title=%s and participant_type="team" and sport_id=%s'
                values = (tm_title, sport_id)
                self.cursor.execute(sp_qry, values)
                data = self.cursor.fetchone()

            if data:
                values = (str(tm_id), str(data[0]), 'team')
                self.cursor.execute(INST_QRY, values)
            else:
                #tm_title = tm_market + ' ' + tm_name + ' football'
                tm_title = tm_market + ' ' + tm_name + " men's basketball"
                exactly_present, mostly_present, not_present, mstly_matched_titles = find_players_to_add(TEAMS_IN_DB.keys(), [tm_title])
                if exactly_present or mostly_present or mstly_matched_titles:
                    print "mostly_present", mstly_matched_titles
                    continue
                    name = mstly_matched_titles[tm_title]
                    sp_qry = 'select id from sports_participants where title=%s and participant_type="team" and sport_id=%s'
                    values = (name, sport_id)
                    self.cursor.execute(sp_qry, values)
                    data = self.cursor.fetchone()

                    if data:
                        values = (str(tm_id), str(data[0]), 'team')
                        self.cursor.execute(INST_QRY, values)



    def teams_data(self):
        sel_qry = "select id, title, tournament_id, sport_id from SPORTSRADARDB.sports_teams where tournament_id not in ('4353138d-4c22-4396-95d8-5f587d2df25c', 'fd560107-a85b-4388-ab0d-655ad022aff7', '2fa448bc-fc17-4d3d-be03-e60e080fdc26', 'NFL')"
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for tm_data in data:
            tm_id = str(tm_data[0])
            sport_id = tm_data[3]
            tm_name = tm_data[1].replace('Los Angeles Angels', 'Los Angeles Angels of Anaheim').replace(' FC', ' F.C.')
            
            sp_qry = 'select id from sports_participants where title=%s and participant_type="team" and sport_id=%s'
            values = (tm_name, sport_id)
            self.cursor.execute(sp_qry, values)
            data = self.cursor.fetchone()
            if not data:
                sp_qry = 'select id from sports_participants where aka=%s and participant_type="team" and sport_id=%s'
                values = (tm_name, sport_id) 
                self.cursor.execute(sp_qry, values)
                data = self.cursor.fetchone()

            if not data:
                sp_qry = 'select id from sports_participants where title like %s and participant_type="team" and sport_id=%s'
                tm_val = '%' + tm_name + '%'
                values = (tm_val, sport_id) 
                self.cursor.execute(sp_qry, values)
                data = self.cursor.fetchone()

            if not data:
                sp_qry = 'select id from sports_participants where aka like %s and participant_type="team" and sport_id=%s'
                tm_val = '%' + tm_name + '%'
                values = (tm_val, sport_id)
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
                    sp_qry = 'select id from sports_participants where title=%s and participant_type="team" and sport_id=%s'
                    values = (name, sport_id)
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
        sel_qry = 'select id, title from SPORTSRADARDB.sports_tournaments where sport_id=5'
        self.cursor.execute(sel_qry)
        data = self.cursor.fetchall()
        for to_data in data:
            to_id = str(to_data[0])
            to_name = str(to_data[1]).replace(',', '').strip().replace('Qualification', 'Qualifying').replace('Knockout stage', 'Knockout Phase').replace(" Men", " Men's").replace(' Double', ' Doubles').replace(' Women', " Women's").replace('Roland Garros', 'French Open').replace('IPTL', 'International Premier Tennis League')

            sp_qry = 'select id, title from sports_tournaments where title=%s and sport_id=5'
            values = (to_name)
            self.cursor.execute(sp_qry, values)
            data = self.cursor.fetchone()

            if not data:
                sp_qry = 'select id, title from sports_tournaments where aka=%s and sport_id=5'
                values = (to_name)
                self.cursor.execute(sp_qry, values)
                data = self.cursor.fetchone()
            if data:
                values = (str(to_id), str(data[0]), 'tournament')
                print str(to_id), str(data[0]), to_name
                self.cursor.execute(INST_QRY, values)
            else:
                print "not_matches", to_name

        
    def run_main(self):
        #self.get_db_players() 
        #self.players_data()
        self.get_db_teams()
        #self.teams_data()
        self.ncaa_teams()
        #self.groups_data()
        #self.tournaments_data()
        self.cursor.close()
        self.conn.close()



if __name__ == '__main__':
    vtv_task_main(SportsRadarMerge)






