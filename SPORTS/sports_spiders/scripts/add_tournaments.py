import sys
import os
import json
from vtv_utils import copy_file
from vtv_task import VtvTask, vtv_task_main
from vtv_dir import VtvDataDir
from vtv_db import get_mysql_connection
from ssh_utils import scp


tou_id_qry = 'select id, sport_id from sports_tournaments where gid=%s'
WIKI_LINK = 'https://en.wikipedia.org/?curid=%s'
TM_MAX_ID_QUERY = 'select id, gid from sports_tournaments where id in \
                (select max(id) from sports_tournaments)'
wiki_inst_qry = "insert into GUIDMERGE.sports_wiki_merge(exposed_gid, child_gid, action, modified_date) values(%s, %s, 'override', now()) on duplicate key update modified_date=now()"

wiki_gid_qry = 'select child_gid from GUIDMERGE.sports_wiki_merge where exposed_gid=%s'

IN_CON_QRY = "insert into sports_concepts_tournaments (concept_id, tournament_id, sequence_num, status, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), concept_id=%s, tournament_id=%s"

IN_TOU = "insert into sports_tournaments(id, gid, title, type, subtype, aka, sport_id, affiliation, gender, genre, season_start, season_end, base_popularity, keywords, image_link, reference_url, schedule_url, scores_url, standings_url, location_ids, stadium_ids, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"

AFF_DICT = {'basketball': 'FIBA', 'soccer': "club-football", 'cricket': 'ICC', 'baseball': 'IBAF', 'softball': 'ISF', 'handball': "IHF", 'volleyball': "FIVB", 'rugby sevens': 'IRB', 'rugby union': 'IRB', 'rugby league': 'RLIF', 'american football': 'IFAF', 'ice hockey': 'IIHF', 'cycling': 'uci', 'floorball': 'IFF', 
'futsal': 'AMF', 'golf': 'USGA', 'horse racing': 'ntra', 'association football': 'club-football', 'australian rules football': 'AFL', 'chess': 'fide', 'field hockey': 'FIH', 'waterpolo': 'FINA', 'water polo': 'FINA', 'table tennis': 'ITTF', 'rules football': 'AFL', 'roller hockey': 'FIRS'}

DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'

class SportsTournamentsInfo(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name    = "SPORTSDB"
        self.db_ip      = "10.28.218.81"
        self.machine_ip = '10.28.216.45'
        self.wiki_db    = "GUIDMERGE"
        self.logs_path  = "/var/tmp/"
        self.log_pat    = "wiki_sports_tournaments_from_templates.json"

        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')
        self.season_start = '2017-01-01'
        self.season_end   = '2017-10-01'
        self.mismatching_list = open('mismatching_list', 'w+')
   
    def copy_latest_file(self):
        mc_path  = "%s%s" %(self.logs_path, self.log_pat)
        source   = '%s@%s:%s' % ("veveo", self.machine_ip, mc_path)
        status   = scp("veveo123", source, self.logs_path)
        if status != 0:
             self.logger.info('Failed to copy the file: %s:%s' %(self.machine_ip, self.log_pat))
             sys.exit()
 

    def get_tournement(self):
        _data = open('wiki_sports_tournaments_from_templates.json', 'r+')
        for data in _data:
            _data = json.loads(data.strip())
            con_name =  _data.keys()[0].split('{')[0]. \
            replace('Template:Association football', 'Template:Football'). \
            replace('Template:Soccer in ', 'Template:Football in '). \
            replace('Template:Canada Soccer', 'Template:Football in Canada'). \
            replace('Template:USSoccer', 'Template:Football in United States'). \
            replace('Template:', '').strip()
            country = _data.values()[0].get('country', '')
            sport   = _data.values()[0].get('sport', '')

            if sport == "football":
                sport = "soccer"
            if sport == "rugby":
                sport = "rugby union"
            if country == '':
                title = 'missing country'+ ' ' + con_name.encode('utf-8')
                self.mismatching_list.write('%s\n' %title)
                continue

            game = _data.keys()[0].split('{')[0].split(' in the ')[0].split(' in ')[0].strip()
            game = game.replace('Template:', '').strip().replace('Football', 'Soccer').lower()
            try:
                aff = AFF_DICT[sport] 
            except:
                aff = ''
                title =  'missing aff'+ ' ' + sport + ' ' +con_name.encode('utf-8')
                self.mismatching_list.write('%s\n' %title)
                continue

            sel_qry = 'select id from sports_types where title=%s'
            sp_values = (sport)
            self.cursor.execute(sel_qry, sp_values)
            sp_data = self.cursor.fetchone()
            if sp_data:
                sport_id = sp_data[0]
            else:
                sport_id = ''
                title = 'missing sport_id' + ' ' + sport + ' '+con_name.encode('utf-8') 
                self.mismatching_list.write('%s\n' %title)
                continue
            
            aka = ''
            country_name = con_name.replace('Football', '').replace(game, '').strip().\
            replace(' in the ', '').replace(' in ', '').strip()
            country_name = con_name.replace('Rugby League in the ', '').strip().\
            replace('Rugby League in ', '').strip().replace('Rugby Union in ', '').strip().\
            replace('Football in the ', '').replace('Soccer in the ', '').\
            replace('Football in ', '').replace('Soccer in ', '').strip().\
            replace('Cricket in the ', '').replace('Cricket in ', '').strip().\
            replace('Basketball in the ', '').replace('Basketball in ', '').strip()

            con_name = country.title() + " " + sport.title().replace('Soccer', 'Football')
            
            if sport == "soccer":
                
                con_name = con_name.replace('England', 'English').\
            replace('Spain', 'Spanish').replace('Italy', 'Italian').\
            replace('France', 'French').replace('Germany', 'German').\
            replace('Argentina', 'Argentine').replace('Brazil', 'Brazilian').\
            replace('Portugal', 'Portuguese').replace('Netherlands', 'Dutch').\
            replace('Mexico', 'Mexican').replace('Scotland', 'Scottish Professional Football League')

            con_gid = _data.keys()[0].split('{')[-1].strip().replace('}', '')
            tou_type = 'sportsconcept' 
            con_id, con_gid_ = self.get_tou_id(con_gid, con_name, tou_type, sport_id)
            if not con_id and 'template' not in con_name:
                self.add_tou(con_name, con_gid, tou_type, country, aka, aff, sport_id)
            
            leagues_list = ['league', 'domestic']
            for league in leagues_list:
                major_leagues =  _data.values()[0].get('tournament', '').get(league, '').get('men', '')
                for mg_leagues in major_leagues:
                    league_name = mg_leagues 
                    leagune_nm = league_name.split('{')[0].strip()
                    leagune_gid = league_name.split('{')[-1].strip().replace('}', '')
                    if not leagune_gid or "WIKI" not in leagune_gid or u'\u2013' in leagune_nm \
                    or '19' in leagune_nm or "playoffs" in leagune_nm or "association football" in leagune_nm \
                    or '17' in leagune_nm or '16' in leagune_nm or " clubs in " in leagune_nm \
                        or "Women" in leagune_nm or "Conference" in leagune_nm or " top scorers" in leagune_nm \
                        or "Commonwealth" in leagune_nm or "Under 20" in leagune_nm or "Olympics" in leagune_nm or \
                        "women" in leagune_nm or "History" in leagune_nm or 'All-time LNFS table' in leagune_nm:

                        continue

                    if 'Futsal' in leagune_nm:
                        game = 'Futsal'
                        sport_id = '158'
                        aff = 'AMF'

                    tou_type = 'tournament'
                    aka = ''
                    if " (" in leagune_nm:
                        aka  = leagune_nm
                        leagune_nm = leagune_nm.split('(')[0].strip()


                    league_id, league_gid = self.get_tou_id(leagune_gid, leagune_nm, tou_type, sport_id)

                    if not league_id:
                        self.add_tou(leagune_nm, leagune_gid, tou_type, country, aka, aff, sport_id)
                    if con_id and league_id:
                        values = (con_id, league_id, '', '', con_id, league_id)
                        self.cursor.execute(IN_CON_QRY, values)
                            
    def get_tou_id(self, leagune_gid, leagune_nm, tou_type, sport_id):
        tm_values = (leagune_gid)
        leagune_nm = leagune_nm
        if "sevens" in leagune_nm.lower():
            sport_id = 167
        else:
            sport_id = sport_id
        self.cursor.execute(wiki_gid_qry, tm_values )
        tm_data = self.cursor.fetchone()
        tou_id = tou_gid = ''
        if tm_data:
            tou_gid = tm_data[0]
            values = (tou_gid)
            self.cursor.execute(tou_id_qry, values)
            t_data = self.cursor.fetchone()
            if t_data:
                tou_id = t_data[0]
        else:
            values = (leagune_nm, sport_id)
            sel_qry = 'select id, gid from sports_tournaments where title =%s and sport_id=%s'
            self.cursor.execute(sel_qry, values)
            tm_data = self.cursor.fetchall()
            if not tm_data:
                sel_qry = 'select id, gid from sports_tournaments where aka =%s and sport_id=%s'

                self.cursor.execute(sel_qry, values)
                tm_data = self.cursor.fetchall()
            if len(tm_data) == 1:
                tou_id = tm_data[0][0]
                tou_gid = tm_data[0][1]
                merge_values = (leagune_gid, tou_gid)
                if "WIKI" in leagune_gid and tou_type == "tournament":
                    self.cursor.execute(wiki_inst_qry, merge_values)
            elif len(tm_data) >1:
                print tm_data

        return tou_id, tou_gid 

    def add_tou(self, con_name, con_gid, tou_type, country, aka, aff, sport_id):
        self.cursor.execute(TM_MAX_ID_QUERY)
        pl_data = self.cursor.fetchall()
        max_id, max_gid = pl_data[0]
        next_id = max_id + 1
        ref_url = WIKI_LINK %(con_gid.replace('WIKI', ''))
        next_gid = 'TOU' + str(int(max_gid.replace('TOU', '')) + 1)
        sel_location = 'select id from sports_locations where country=%s and city="" and state=""'
        loc_values = (country.replace('united states', 'USA'))
        self.cursor.execute(sel_location, loc_values)
        loc_data = self.cursor.fetchone()
        location_id = ''
        if loc_data:
            location_id = loc_data[0]
        else:
            return
        if "sevens" in con_name.lower():
            sport_id = 167
        else:
            sport_id = sport_id
        tou_values = (next_id, next_gid, con_name, tou_type, '', aka, sport_id, aff, 'male', '', self.season_start, self.season_end, '200', '', '', ref_url, '', '', '', location_id, '')
        self.cursor.execute(IN_TOU, tou_values)
        if tou_type == "tournament" and "WIKI" in con_gid:
            values = (con_gid)
            self.cursor.execute(wiki_gid_qry, values)
            gid_data = self.cursor.fetchone()
            if not gid_data and "WIKI" in con_gid:
                in_values = (con_gid, next_gid)
                self.cursor.execute(wiki_inst_qry, in_values)
            else:
                print gid_data, con_name

    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','add_tournaments*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


    def run_main(self):
        self.copy_latest_file()
        self.get_tournement()


if __name__ == '__main__':
    vtv_task_main(SportsTournamentsInfo)

