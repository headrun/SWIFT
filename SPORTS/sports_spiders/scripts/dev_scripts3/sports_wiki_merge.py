import os, sys, re

from collections import defaultdict
from datetime import datetime
from vtv_task import VtvTask, vtv_task_main
from vtv_utils import tar_zip_file, copy_dir_files_based_on_pattern
from data_constants import CONTENT_TYPE_PERSON, CONTENT_TYPE_TEAM, CONTENT_TYPE_TOURNAMENT, CONTENT_TYPE_STADIUM
from foldFileInterface import dataFileDictIterator
from StringUtil import cleanString

#COMPATIBLE_WIKI_TYPES = (CONTENT_TYPE_PERSON, CONTENT_TYPE_TEAM, CONTENT_TYPE_TOURNAMENT)

#these are user-defined types
CONTENT_PLACE_PERSON = "place"
CONTENT_BUILDING_PERSON = "building"

COMPATIBLE_WIKI_TYPES = ( CONTENT_TYPE_STADIUM, CONTENT_PLACE_PERSON, CONTENT_BUILDING_PERSON)

BD_MATCH_SCORE = 40
TITLE_MATCH_SCORE = 20
IK_THRESHOLD = 70
KWD_REGX = re.compile('(?P<text>[^{]*)({(?P<wt>\d+)})?', re.I|re.U)
WIKI_TAG_FILE           = 'DATA_WIKIPEDIA_ENTERTAINMENT_MC_FILE'

class SportsMerge(VtvTask):

    DEFAULT_CONFIG_FILE     = '%s_wiki_merge.cfg'
    PLAYERS_MC_FILE         = '%s_PLAYERS_MC_FILE'
    TEAMS_MC_FILE           = '%s_TEAMS_MC_FILE'
    PLAYERS_FOLD_FILE       = '%s_PLAYERS_FOLD_FILE'
    TEAMS_FOLD_FILE         = '%s_TEAMS_FOLD_FILE'
    TOURNAMENTS_MC_FILE     = '%s_TOURNAMENTS_MC_FILE'
    STADIUMS_MC_FILE        = '%s_STADIUMS_MC_FILE.new'

    DEFAULT_THRESHOLD       = 10
    DEFAULT_DATA_ID         = 'sports_merge'
    SPORTS                  = 'sports'

    source_to_prefix        = {'sports': 'DATA', 'espn': 'ESPN'}

    def __init__(self):
        VtvTask.__init__(self)

        self.init_config(self.options.config_file)
        self.name               = self.get_default_config_value('NAME')
        self.source             = self.get_default_config_value('MERGE_CANDIDATE')
        self.source             = self.options.source.lower()
        self.threshold          = self.get_default_config_value('THRESHOLD') or self.DEFAULT_THRESHOLD
        self.threshold          = int(self.DEFAULT_THRESHOLD)
        self.file_              = open('missed_teams_sports_merge', 'a+')

        self.get_dirs_and_files()

        self.options.reports = self.REPORTS_DIR
        self.wiki_title_to_gid_set      = defaultdict(set)
        self.wiki_gid_to_rec            = {}
        self.player_merge_dict = defaultdict(list)
        self.gid_to_score = {}
        self.stats = {'merge_count': 0 , 'team': 0, 'person': 0}

        self.set_data_version()

    def set_data_version(self):
        """
        Set data version.
        """
        dir_list = [
                    self.local_data_dirs.sports_data_dir,\
                    self.current_data_dirs.wiki_archival_data_dir,\
                    self.current_data_dirs.wiki_incr_data_dir,\
                   ]
        self.set_source_data_version(dir_list)

    def get_dirs_and_files(self):
        """Get directories and filenames from config"""
        if not self.options.output_dir:
            self.options.output_dir = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, self.name)

        self.OUT_DIR = self.options.output_dir
        self.OUT_DATA_DIR = os.path.join(self.options.output_dir, self.get_default_config_value('DATA_DIR'))
        self.REPORTS_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR, self.name)
        self.options.report_file_name = os.path.join(self.REPORTS_DIR, '%s.html' % self.name_prefix)

        self.define_files()

    def define_files(self):
        """Define files from config"""
        self.output_file_path = os.path.join(self.OUT_DIR, self.get_default_config_value('OUTPUT_FILENAME'))
        self.out_file = os.path.join(self.OUT_DIR, self.get_default_config_value('MERGE_FILE'))
        self.tool_out_file = os.path.join(self.OUT_DIR, self.get_default_config_value('TOOL_MERGE_FILE'))
        self.output_title_file = os.path.join(self.OUT_DIR, self.get_default_config_value('OUTPUT_TITLE_FILE'))

        prefix = self.source_to_prefix.get(self.source, 'DATA')
        sports_data_dir = getattr(self.datagen_dirs, '%s_data_dir' % self.source)
        if os.path.exists(getattr(self.local_data_dirs, '%s_data_dir' % self.source)):
            sports_data_dir = getattr(self.local_data_dirs, '%s_data_dir' % self.source)

        self.options.players_mc_file        = os.path.join(sports_data_dir, self.PLAYERS_MC_FILE % prefix)
        self.options.players_fold_file      = os.path.join(sports_data_dir, self.PLAYERS_FOLD_FILE % prefix)
        self.options.teams_mc_file          = os.path.join(sports_data_dir, self.TEAMS_MC_FILE % prefix)
        self.options.teams_fold_file        = os.path.join(sports_data_dir, self.TEAMS_FOLD_FILE % prefix)
        self.options.tournaments_mc_file    = os.path.join(sports_data_dir, self.TOURNAMENTS_MC_FILE % prefix)
        self.options.stadiums_mc_file       = os.path.join(sports_data_dir, self.STADIUMS_MC_FILE %prefix)

    def set_options(self):
        """Options"""
        wiki_data_dirs  = '%s,%s' % (self.datagen_dirs.wiki_incr_data_dir, self.datagen_dirs.wiki_archival_data_dir)

        self.parser.add_option('-c', '--config-file', default = None,\
                                help = 'Sports merge config file')
        self.parser.add_option('-w', '--wiki-data-dirs', default = wiki_data_dirs,\
                                help = 'Wiki data directories separated by comma')
        self.parser.add_option('-o', '--output-dir', default =None,\
                                help = 'output-dir')
        self.parser.add_option('', '--source', default=self.SPORTS,
                                help = 'merges candidate source name - sports, espn, etc')
        self.parser.add_option('', '--no-sanity', default=False, action='store_true',\
                                help = 'Enable this option to avoid sanity')
        self.parser.add_option('-t', '--type-team-or-player', default = 'player', \
                                help = 'type player or team') 

    def post_options_init(self):
        """Option defaulting"""
        if not self.options.config_file:
            config_file_name = self.DEFAULT_CONFIG_FILE % self.options.source.lower()
            self.options.config_file = os.path.join(self.system_dirs.VTV_ETC_DIR, config_file_name)

    def load_wiki_records(self):
        """Iterates over wiki records and collects candidates"""
        self.logger.info("Started Iterating over wiki")
        count = 0
        schema = {'Gi': 0, 'Ti': 1, 'Ge': 2, 'Bd': 3, 'Vt': 4, 'Ak': 5, 'Ik': 6, 'Va': 7}

        for directory in self.options.wiki_data_dirs.split(','):
            filename = os.path.join(directory, WIKI_TAG_FILE)

            self.logger.info('Started iterating file %s' % filename)
            for count, record in enumerate(dataFileDictIterator(filename, schema)):
                typ = record[schema['Vt']]
                genre = record[schema['Ge']]
                birth_date = record[schema['Bd']]

                if (typ not in COMPATIBLE_WIKI_TYPES):
                   continue

                gid     = record[schema['Gi']]
                title   = record[schema['Ti']]
                self.wiki_gid_to_rec[gid] = (title, genre, birth_date)

                self.wiki_title_to_gid_set[title].add(gid)
                self.wiki_title_to_gid_set[cleanString(title)].add(gid)
                self.populate_aka(gid, record[schema['Ak']])
                self.populate_aka(gid, record[schema['Va']])
                self.populate_ikw(gid, record[schema['Ik']])

                if count % 10000 == 0:
                    self.logger.info("Processed %s records" % count)
            self.logger.info('Completed iterating over %s' % filename)
            self.wiki_title_to_gid_set.pop('', None)

    def populate_ikw(self, gid, ikw):
        for ik in ikw.split('<>'):
            if ik:
                match_obj = KWD_REGX.match(ik)
                if match_obj:
                    data_dict = match_obj.groupdict()
                    text = data_dict['text']
                    wt = data_dict['wt']
                    if (wt and int(wt)< IK_THRESHOLD):
                        continue
                    self.wiki_title_to_gid_set[text].add(gid)
                    self.wiki_title_to_gid_set[cleanString(text)].add(gid)

    def populate_aka(self, gid, aka):
        for ak in aka.split('<>'):
            if ak:
                ak = ak.split('{')[0]
                self.wiki_title_to_gid_set[ak].add(gid)
                self.wiki_title_to_gid_set[cleanString(ak)].add(gid)

    def process_cand_data(self):
        schema = {'Gi': 0, 'Ti': 1, 'Ak': 2, 'Bd': 3, 'Sp': 4 }

        self.logger.info('Iterating file %s' % self.options.players_mc_file)
        for count, rec in enumerate(dataFileDictIterator(self.options.players_mc_file, schema)):
            gid = rec[schema['Gi']]
            self.merge_record(rec, schema)

            if count % 10000 == 0:
                self.logger.info("Processed %s records" % count)

        self.logger.info('Completed iterating file %s' % self.options.players_mc_file)

    def process_team_data(self):
        team_schema = {'Gi': 0, 'Ti': 1, 'Ak': 2, 'Sp': 3}

        self.logger.info('Iterating file %s' % self.options.teams_mc_file)
        for count, rec in enumerate(dataFileDictIterator(self.options.teams_mc_file, team_schema)):
            gid = rec[team_schema['Gi']]
            self.merge_record(rec, team_schema)
            if count % 10000 == 0:
                self.logger.info("Processed %s records" % count)

        self.logger.info('Completed iterating file %s' % self.options.teams_mc_file)

    def process_tou_data(self):
        tou_schema = {'Gi': 0, 'Ti': 1, 'Ak': 2, 'Sp': 3}
        self.logger.info('Iterating file %s' % self.options.tournaments_mc_file)

        for count, rec in enumerate(dataFileDictIterator(self.options.tournaments_mc_file, tou_schema)):
            gid = rec[tou_schema['Gi']]
            self.merge_record(rec, tou_schema)
            if count % 10000 == 0:
                self.logger.info("Processed %s records" % count)
        self.logger.info('Completed iterating file %s' % self.options.tournaments_mc_file)
   
    def process_stad_data(self):
        stad_schema = {'Gi': 0, 'Ti': 1, 'Ak': 2, 'Sp': 3}
        self.logger.info('Iterating file %s' % self.options.tournaments_mc_file)

        for count, rec in enumerate(dataFileDictIterator(self.options.stadiums_mc_file, stad_schema)):
            gid = rec[stad_schema['Gi']]
            self.merge_record(rec, stad_schema)
            if count % 10000 == 0:
                self.logger.info("Processed %s records" % count)
        self.logger.info('Completed iterating file %s' % self.options.stadiums_mc_file)    

    def merge_record(self, record, schema):
        """For the given wiki record, finds candidates and evaluates them"""

        gid = record[schema['Gi']]
        title = record[schema['Ti']]
        clean_title = cleanString(title)
        if self.options.type_team_or_player == 'player':
            bd = record[schema['Bd']]
        else:
            bd = ''
        aka = record[schema['Ak']]
        sports = record[schema['Sp']]

        wiki_gid_set = self.wiki_title_to_gid_set.get(title, set())
        wiki_gid_set.update(self.wiki_title_to_gid_set.get(clean_title, set()))
        if not wiki_gid_set:
            record = '<>'.join([gid, title])

        for ak in aka.split('<>'):
            if ak:
                wiki_gid_set.update(self.wiki_title_to_gid_set.get(ak, set()))
                wiki_gid_set.update(self.wiki_title_to_gid_set.get(cleanString(ak), set()))

        for wiki_gid in wiki_gid_set:
            wiki_rec = self.wiki_gid_to_rec[wiki_gid]
            if (wiki_rec):
                if (bd and wiki_rec[-1]):
                    #bd = datetime.strptime(bd, "%Y-%m-%d").date()
                    #bd = bd.strftime('%Y-%-m-%-d')

                    if (wiki_rec[-1] == bd):
                        self.player_merge_dict[gid] = [(BD_MATCH_SCORE, wiki_gid)]
                        self.gid_to_score[wiki_gid] = BD_MATCH_SCORE
                        break
                elif sports in wiki_rec[1]:
                    if (wiki_rec[0] == title or wiki_rec[0] == clean_title):
                        if (self.gid_to_score.get(wiki_gid, 0) < TITLE_MATCH_SCORE):
                            self.player_merge_dict[gid].append((TITLE_MATCH_SCORE, wiki_gid))
                            self.gid_to_score[wiki_gid] = TITLE_MATCH_SCORE
                    else:
                        self.player_merge_dict[gid].append((0, wiki_gid))
                    print title, wiki_gid

    def write_merge_file(self):
        fp = open(self.out_file, 'w')
        fp_tool = open(self.tool_out_file, 'w')
        self.stats['merge_count'] = len(self.player_merge_dict)
        self.stats['person'] = len(self.player_merge_dict)

        for sp_gid, merge_tuple in self.player_merge_dict.iteritems():
            if len(merge_tuple) == 1:
                score, wiki_gid = merge_tuple[0]
                confidance_flag = 1 if score == BD_MATCH_SCORE else 2
                if (score == self.gid_to_score.get(wiki_gid, 0)):
                    data = '%s<>%s\n' % (wiki_gid, sp_gid)
                    fp.write(data.encode('utf8'))
                    data_tool = '%s<>%s<>person<>%s<>%s\n' % (wiki_gid, sp_gid, score, confidance_flag)
                    fp_tool.write(data_tool.encode('utf8'))
            else:
                merge_tuple.sort(reverse=True)
                score, wiki_gid = merge_tuple[0]
                confidance_flag = 1 if score == BD_MATCH_SCORE else 2
                if (score == self.gid_to_score.get(wiki_gid, 0)):
                    data = '%s<>%s\n' % (wiki_gid, sp_gid)
                    fp.write(data.encode('utf8'))
                    data_tool = '%s<>%s<>person<>%s<>%s\n' % (wiki_gid, sp_gid, score, confidance_flag)
                    fp_tool.write(data_tool.encode('utf8'))

        fp.close()
        fp_tool.close()

    def run_main(self):
        """Main"""

        self.load_wiki_records()
        if self.options.type_team_or_player == 'player':
            self.process_cand_data()
        elif self.options.type_team_or_player == 'team':
            self.process_team_data()
        elif self.options.type_team_or_player == 'tournament':
            self.process_tou_data()
        elif self.options.type_team_or_player == "stadiums":
            self.process_stad_data()
        self.write_merge_file()

        self.add_stats('Sports Merge Statistics', self.stats)

        if not self.options.no_sanity:
            self.run_sanity()

        tar_file_prefix = 'data_%s_merge' % self.source
        tar_files = map(os.path.basename, [ self.out_file, self.tool_out_file])
        self.archive_data_files(self.OUT_DIR, self.OUT_DATA_DIR, tar_files, tar_file_prefix)

    def run_sanity(self):
        """Calls sanity script"""
        script_name = '%s/merge_sanity.py' % self.system_dirs.VTV_SERVER_DIR
        options = '--merge-type %s --output-dir %s --new-merge-file %s --wiki-column-name %s' % (self.name, self.OUT_DIR, self.out_file, 'exposed_gid')
        command = 'python %s %s --report-file-name %s' % (script_name,\
                                options, self.options.report_file_name)
        self.start_process(script_name, command)

    def post_print_stats(self):
        self.print_report_link()

    def cleanup(self):
        self.move_logs(self.OUT_DIR, [('.', '*log')])
        self.remove_old_dirs(self.OUT_DIR, self.logs_dir_prefix, self.log_dirs_to_keep)

if __name__ == '__main__':
    vtv_task_main(SportsMerge)
