#!/usr/bin/env python

################################################################################
#$Id: datagen_summary.py,v 1.736 2016/09/21 06:24:06 vinodhini.p Exp $
#Copyright(c) 2005 Veveo.tv
################################################################################

import sys
import os
import re
import types
import glob
import operator
import time
from datetime import date, datetime, timedelta

import ast
import urllib
import jinja2

from dateutil import parser
from vtv_utils import VTV_PY_EXT, VTV_SERVER_DIR, vtv_unpickle, make_dir, make_dir_list, copy_file
from vtv_task import VtvTask, vtv_task_main
from vtv_user import get_vtv_user_name_and_password, vigenere_crypt
import ssh_utils
from data_schema import WIKI_EPISODE_FILEDS
from data_constants import CONTENT_XT_ACTOR, CONTENT_XT_DIRECTOR, CONTENT_XT_PRODUCER, CONTENT_XT_HOST, \
                           CONTENT_XT_PLAYER, CONTENT_XT_MUSIC_ARTIST, CONTENT_XT_PERSON, CONTENT_XT_MUSIC_BAND, \
                           CONTENT_XT_PRIZE, CONTENT_XT_SUBGENRE, CONTENT_XT_MOOD, CONTENT_XT_DUO, CONTENT_TYPE_MOVIE, \
                           CONTENT_TYPE_TVSERIES, CONTENT_TYPE_EPISODE, CONTENT_TYPE_PERSON, CONTENT_TYPE_TEAM, \
                           CONTENT_TYPE_TOURNAMENT


NAME, VALUE     = range(2)

ONE_GB  = 1
FIVE_GB = 5 * ONE_GB
TEN_GB  = 10 * ONE_GB

TIMEFORMAT  = '%Y-%m-%d'

JINJA_FILE_NAME   = 'sports_crawler_summary.jinja'
HTML_FILE_NAME    = 'sports_crawler_summary.html'

MERGE_STATS_HEADER = [ 'Total', 'Correct', 'Wrong', 'New', 'Missing' ]
MERGE_STATS_FIELDS = [ 'total', 'correct', 'wrong', 'new', 'missing' ]

JINJA_LOOP_FORMAT_STR = """
{%% if loop.index == %d %%}
   <tr>
   <th colspan=%d><a href="http://%s/REPORTS/">%s</a></th>
   </tr>
{%% endif %%}
"""

JINJA_ROW_FORMAT_STR = """
{%% if key.startswith('%s') %%}
<tr>
%s
</tr>
{%% endif %%}
"""

JINJA_COLUMN_FORMAT_STR = '<th colspan=%d>%s</th>'


MAX_DAYS = 30
NAME_LIST = [ 'SERVER_NAME', 'SERVER_IP', 'FREQUENCY', 'REPORT_PREFIX', 'REMOTE_DIR', 'DATE_FORMAT', 'TITLE' ]
SERVER_NAME_INDEX = NAME_LIST.index('SERVER_NAME')


class Report(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)

        self.init_config(self.options.config_file)

        my_name = self.get_default_config_value('NAME')

        self.OUT_DIR = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, my_name)

        self.PICKLE_FILES_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR, 'PICKLE_FILES')

        self.REPORTS_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR, my_name)
        self.SUMMARY_REPORTS_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR,'SUMMARY','CRAWLER_SUMMARY')

        self.options.report_file_name = os.path.join(self.REPORTS_DIR, '%s.html' % self.name_prefix)

        make_dir_list([ self.OUT_DIR, self.REPORTS_DIR, self.PICKLE_FILES_DIR, self.SUMMARY_REPORTS_DIR ], self.logger)

        self.BASE_DATE = (datetime.now() - timedelta(MAX_DAYS)).date()
        self.yesterday = (datetime.now() - timedelta(1)).date()

        self.run_date, self.run_graph_date = self.get_run_dates(self.yesterday)
        print 'Run Dates: ', self.run_date, self.run_graph_date

        if self.options.ip_list:
            self.scp_ip_list = self.options.ip_list.split(',')
        else:
            self.scp_ip_list = []

        self.get_stats_dir_list()

    def set_options(self):
        config_file_name = os.path.join(self.system_dirs.VTV_ETC_DIR, 'sports_crawler_summary.cfg')
        self.parser.add_option('-c', '--config-file', default = config_file_name, help = 'config file')
        self.parser.add_option('', '--run', default='all', help='all, report, scp, html')
        self.parser.add_option('', '--run-only', help='If provided, this section alone will be executed')
        self.parser.add_option('', '--only-scp', help='if some directory names are given, only those directories will be scped in scp phase')
        self.parser.add_option('', '--ip-list', default='', help = 'config file')
        self.parser.add_option('', '--scp-days', default='1', help = 'config file')

    def get_run_dates(self, run_date):
        run_date_str = run_date.strftime("%Y-%m-%d")
        run_graph_date_str = run_date.strftime("%Y%m%d")
        return run_date_str, run_graph_date_str

    def get_stats_dir_list(self):
        self.STATS_DIR_LIST = []
        self.USER_PASS_LIST = [] # a one to one relationship with self.STATS_DIR_LIST
        self.STATS_FUNC_DICT = {}

        section_regex = re.compile('\[(?P<section>\w+)\]')
        text = open(self.options.config_file).read()
        ordered_sections = section_regex.findall(text)
        ordered_sections.pop(0) # remove DEFAULT

        for section in ordered_sections:
            section_dict = self.config_parser.sections[section]

            args_list = [ section_dict[x.lower()] for x in NAME_LIST ]
            server_name, server_ip = args_list[:2]
            if not server_name:
                server_name = server_ip
            args_list[0] = server_name
            args_list.insert(0, section)
            self.STATS_DIR_LIST.append(args_list)

            # get user and password
            config_user_name = self.get_config_value("USER", section)
            config_pass_name = self.get_config_value("PASSWORD", section)
            config_pass_name = vigenere_crypt(config_pass_name, False)
            self.USER_PASS_LIST.append((config_user_name, config_pass_name))

            stats_func_name = self.get_config_value('STATS_FUNC_NAME', section)
            stats_func_obj = getattr(self, stats_func_name, None)
            stats_func_args = self.get_config_value('STATS_FUNC_ARGS', section)
            final_script = self.get_config_value('FINAL_SCRIPT', section) or ''
            self.STATS_FUNC_DICT[section] = (stats_func_obj, stats_func_args, '', '', final_script, '')
            new_stats_func_name = self.get_config_value('NEW_STATS_FUNC_NAME', section)
            new_stats_date = self.get_config_value('NEW_STATS_DATE', section)
            new_final_script = self.get_config_value('NEW_FINAL_SCRIPT', section) or ''
            if new_stats_func_name and new_stats_date:
                new_stats_func_obj = getattr(self, new_stats_func_name, None)
                self.STATS_FUNC_DICT[section] = (stats_func_obj, stats_func_args, new_stats_func_obj, new_stats_date, final_script, new_final_script)

    def get_latest_file_remote(self, host, path_pattern):
        ls_cmd = 'ls -t %s' % path_pattern
        status, process = ssh_utils.ssh_cmd_output(host, self.vtv_username, self.vtv_password, cmd = ls_cmd)
        if status == 0:
            file_list = process.before.split('\r\n')
            if len(file_list) > 1:
                return file_list[1]

        return None

    def run_report(self):
        report_cmd = 'cd %s; python data_report.%s --report-dir /data/REPORTS' % (VTV_SERVER_DIR, VTV_PY_EXT)

        ip_list = [ dir_info[SERVER_NAME_INDEX + 1] for dir_info in self.STATS_DIR_LIST ]
        auth_list = [(user, passwd) for user,passwd in self.USER_PASS_LIST ]
        ip_auth_list = list(set(zip(ip_list, auth_list)))

        for ip, auth_tuple in ip_auth_list:
            print 'Run Report: %s' % ip
            self.vtv_username, self.vtv_password = auth_tuple
            #Remotely running of data_report.py
            status, process = ssh_utils.ssh_cmd_output(ip, self.vtv_username, self.vtv_password, report_cmd)
            if status != 0:
               ssh_utils.ssh_cmd_output(ip, self.vtv_username, self.vtv_password, report_cmd.replace(VTV_PY_EXT, 'pyc'))

            # Deleting log file
            status_del = ssh_utils.ssh_cmd(ip, self.vtv_username, self.vtv_password, 'rm -f %s/data_report*.log' % VTV_SERVER_DIR)

        print 'Done - HTML Generation'

    def run_scp(self):
        SCP_SRC_DIR = self.system_dirs.VTV_REPORTS_DIR
        SCP_DST_DIR = self.PICKLE_FILES_DIR

        days = int(self.options.scp_days) + 1
        print 'SCP SRC DIR: %s DST DIR: %s Days: %d' % (SCP_SRC_DIR, SCP_DST_DIR, days)

        for day in range(1, days):
            run_date, run_graph_date = self.get_run_dates(datetime.now() - timedelta(day))
            print 'SCP SRC DIR: %s DST DIR: %s Dates: %s %s' % (SCP_SRC_DIR, SCP_DST_DIR, run_date, run_graph_date)

            for dir_info, auth_tuple in zip(self.STATS_DIR_LIST, self.USER_PASS_LIST):
                local_dir_name, server_name, ip, frequency, report_prefix, remote_dir_name, date_opt, title = dir_info
                self.vtv_username, self.vtv_password = auth_tuple

                if self.options.only_scp and local_dir_name not in self.options.only_scp:
                    continue

                if date_opt == 'N':
                    date_str = run_graph_date
                else:
                    date_str = run_date

                pickle_file_pattern = os.path.join(SCP_SRC_DIR, remote_dir_name, '%s%s*.pickle' % (report_prefix, date_str))

                if self.scp_ip_list and ip not in self.scp_ip_list:
                    #print 'Skipping SCP: %s : %s to %s' % (ip, pickle_file_pattern, local_dir_name)
                    continue

                #print 'SCP: %s : %s to %s' % (ip, pickle_file_pattern, local_dir_name)

                pickle_file_name = self.get_latest_file_remote(ip, pickle_file_pattern)
                if not pickle_file_name:
                    self.logger.error('Cannot SCP : %s : %s to %s' % (ip, pickle_file_pattern, local_dir_name))
                    #print 'Error', 'SCP: %s : %s to %s' % (ip, pickle_file_pattern, local_dir_name)
                    continue

                src = '-p %s@%s:%s' % (self.vtv_username, ip, pickle_file_name)
                dst = os.path.join(SCP_DST_DIR, local_dir_name)
                make_dir(dst)

                status = ssh_utils.scp(self.vtv_password, src, dst)
                if status:
                    self.logger.error('SCP failed for: %s %s' % (src, dst))
                else:
                    self.logger.info('SCP succeeded for: %s %s' % (src, dst))

        print 'Done - SCP Pickle files from machines'

    def init_summary(self):
        pass

    def get_files_in_date_range(self, base_file_name, start_date, end_date = None, num_of_days = 0, suffix = '', time_format = TIMEFORMAT):
        start_date = datetime.strptime(start_date, time_format)
        if end_date:
            end_date = datetime.strptime(end_date, time_format)
        else:
            end_date = start_date - timedelta(days=num_of_days)
        file_list = []
        while start_date >= end_date:
            filename = "%s%s%s" % (base_file_name, start_date.strftime(time_format), suffix)
            file_list.append(filename)
            start_date -= timedelta(days=1)
        return file_list

    def get_stats_map(self, stats_dict, key):
       KEY_MAP = { 'PersonalityFolding'         : 'person',
                   'personalityfolding'         : 'person',
                   'SequelFolding'              : 'sequel',
                   'sequelfolding'              : 'sequel',
                   'PhraseFolding'              : 'phrase',
                   'phrasefolding'              : 'phrase',
                   'Tournament'                 : 'tournament',
                   'TeamFolding'                : 'team',
                   'teamfolding'                : 'team',
                   'FoldedOnChannelAffiliation' : 'channelaffiliation',
                   'foldedonchannelaffiliation' : 'channelaffiliation',
                   'sportsfilter'               : 'filter',
                 }

       new_key = KEY_MAP.get(key, key)
       if new_key in stats_dict:
           key = new_key
       return key

    def get_sports_header(self):
        HEADER = ['games', 'completed', 'winners', 'inserted', 'updated', 'skipped', 'responses', 'runs']
        SPORTS_STATS = ['games', 'completed', 'winners']
        CRAWL_STATS = ['inserted', 'updated', 'skipped', 'responses', 'runs']
        self.group_header = ['Sports Stats', 'Crawl Stats']
        return [ HEADER, SPORTS_STATS, CRAWL_STATS]

    def get_spider_stats(self, row_list, obj):
        if not row_list:
            self.stats_header = self.get_sports_header()
            return

        HEADER, SPORTS_STATS, CRAWL_STATS = self.stats_header

        STATS_FILE_LIST = [ self.local_dir_name, 'games_today.py' ]
        stats_obj_list = self.get_stats_obj_list(STATS_FILE_LIST, obj)
        stats_obj = stats_obj_list[0]
        game_stats_obj = stats_obj_list[1]

        game_stats_dict = self.get_stats_dict([ 'stats' ], game_stats_obj)
        self.get_dict_stats(row_list, SPORTS_STATS, game_stats_dict)

        stats_dict = self.get_stats_dict([ self.local_dir_name ], stats_obj)
        self.get_dict_stats(row_list, CRAWL_STATS, stats_dict)

    def get_stats_dict(self, need_stats_name_list, stats_obj, op = 'single'):
        stats_str, stats_list = stats_obj['stats']
        stats_dict = {}

        if op == 'union' and not need_stats_name_list[0]:
            stats_dict = dict(stats_list)
            return stats_dict

        if op in ['nested', 'union']:
            first_dict_list = need_stats_name_list[0]
        else:
            first_dict_list = need_stats_name_list

        need_stats_dict = {}
        for stats_name, stats_dict in stats_list:
            if stats_name in first_dict_list:
                need_stats_dict = stats_dict
        if not need_stats_dict:
            return {}
        stats_dict = need_stats_dict

        if op in ['nested', 'union']:
            found = True
            for stats_dict_list in need_stats_name_list[1:]:
                found = False
                old_stats_dict = stats_dict
                for stats_dict_name in stats_dict_list:
                    stats_dict = old_stats_dict.get(stats_dict_name, {})
                    if stats_dict:
                        found = True
                        break
                else:
                    break
            if not found:
                return {}

        return stats_dict

    def get_aggregate_stats_dict(self, obj):
        stats_dict = obj.get('aggregate_stats', {})
        return stats_dict

    def get_stats_value(self, stats_dict, key):
       key = self.get_stats_map(stats_dict, key)
       value = stats_dict.get(key, 0)
       return value

    def get_dict_stats(self, row_list, stats_list, stats_dict):
       if isinstance(stats_dict, dict):
           for key in stats_list:
               value = self.get_stats_value(stats_dict, key)
               row_list.append(value)
       else:
           row_list.extend([ 0 ] * len(stats_list))

    def get_stats_obj(self, dir_names, obj):
       empty = { 'stats' : ('', []) }
       if not obj:
           return empty

       dir_list = dir_names.split('#')
       for new_script in dir_list:
           if new_script in obj:
               return obj[new_script]
       else:
           self.logger.error('%s %s' % (new_script, dir_list))
           return empty

    def get_stats_obj_list(self, dir_names_list, obj):
        return [ self.get_stats_obj(dir_names, obj) for dir_names in dir_names_list ]

    def get_stats_from_file(self, row_list, obj, header_func, stats_file_and_name_list):
        if not row_list:
            self.stats_header = header_func()
            return

        for i, NEED_STATS in enumerate(self.stats_header[1:]):
            pos = i * 2
            stats_file_list, stats_name_info = stats_file_and_name_list[pos : pos + 2]

            if isinstance(stats_name_info, tuple):
                op = stats_name_info[0]
                if op == 'union':
                    dict_name_list, stats_name_list = stats_name_info[1:-1], stats_name_info[-1]
                else:
                    dict_name_list = stats_name_info[1:]
                    stats_name_list = []
            else:
                op, dict_name_list = 'single', []
                stats_name_list = stats_name_info

            stats_obj_list = self.get_stats_obj_list(stats_file_list, obj)
            stats_obj = stats_obj_list[0]

            if isinstance(stats_name_info, tuple):
                stats_dict = self.get_stats_dict(dict_name_list, stats_obj, op)
            else:
                stats_dict = self.get_stats_dict(stats_name_list, stats_obj)

            if op == 'union':
                child_stat_name = stats_name_list[0]
                union_dict = {}
                if isinstance(stats_dict, dict):
                    for stats_name in NEED_STATS:
                        new_stats_name = self.get_stats_map(stats_dict, stats_name)
                        stats_obj = stats_dict.get(new_stats_name, None)
                        if not stats_obj:
                            continue
                        if child_stat_name:
                            new_union_key = self.get_stats_map(stats_dict, child_stat_name)
                            new_union_dict = stats_dict[new_stats_name]
                        else:
                            new_union_key = new_stats_name
                            new_union_dict = stats_dict
                        union_dict[stats_name] = self.get_stats_value(new_union_dict, new_union_key)
                stats_dict = union_dict
            else:
                pass

            self.get_dict_stats(row_list, NEED_STATS, stats_dict)

    def get_common_header(self, heading, stats_func):
        COMMON_HEADER = [ 'Date', 'Status', 'Failed Script', 'Final Report', 'Incr Report', 'Overall Time', 'Mem-Peak', 'Mem-Diff' ]

        column_list = [ JINJA_COLUMN_FORMAT_STR % (len(COMMON_HEADER), 'Program Stats') ]
        if stats_func:
            stats_func([], None)
            header_info = self.stats_header[0]
            for i, stats_info in enumerate(self.stats_header[1:]):
                column_list.append(JINJA_COLUMN_FORMAT_STR % (len(stats_info), self.group_header[i]))
        else:
            header_info = []

        table_format = JINJA_ROW_FORMAT_STR % (heading, '\n'.join(column_list))

        header_row = COMMON_HEADER + header_info

        return header_row, table_format

    def get_percent_color(self, percent):
        if percent <= 50:
            color = 'red'
        elif percent <= 70:
            color = 'orange'
        else:
            color = 'black'
        return color

    def get_time_function_name(self, initial_val):
        split_val = initial_val.split(':')
        int_val = map(int, split_val)
        timein_sec = int_val[0]*3600+int_val[1]*60+int_val[2]
        return timein_sec

    def get_time_color(self, peak_seconds):
        if peak_seconds >= 3 * 3600:
            color = 'red'
        elif peak_seconds >= 2 * 3600:
            color = 'orange'
        else:
            color = 'black'
        return color

    def get_time_stats(self, obj):
        peak_time = ''
        try:
            time_value = []
            for script_name in obj:
                initial_val = obj[script_name]['time'][0][2].split(".")[0]
                timein_sec = self.get_time_function_name(initial_val)
                time_value.append((timein_sec, initial_val))
            if time_value:
                time_value.sort(reverse=True)
                peak_seconds, peak_time = time_value[0]
                peak_time = '<font color="%s">%s</font>' % (self.get_time_color(peak_seconds), peak_time)
        except (ValueError, IndexError):
            pass

        return peak_time

    def get_memory_color(self, memory):
        if memory >= TEN_GB:
            color = 'red'
        elif memory >= FIVE_GB:
            color = 'orange'
        else:
            color = 'black'
        return color

    def get_memory_stats(self, obj):
        peak_memory = ''
        peak_diff = ''
        memory_peak_values = []
        memory_diff_values = []
        try:
            for script_name in obj:
                if not obj[script_name]['memory']:
                    continue
                memory_peak = round(obj[script_name]['memory']['Main'][1][4]/1000.00/1000.00, 2)
                memory_diff = round((obj[script_name]['memory']['Main'][1][4] - obj[script_name]['memory']['Main'][0][4])/1000.0/1000.0, 2)
                memory_peak_values.append(memory_peak)
                memory_diff_values.append(memory_diff)
                memory_peak_values.sort(reverse=True)
                memory_diff_values.sort(reverse=True)

            memory = memory_peak_values[0]
            peak_memory = '<font color="%s">%10.2fG</font>' % (self.get_memory_color(memory), memory)
            memory = memory_diff_values[0]
            peak_diff = '<font color="%s">%10.2fG</font>' % (self.get_memory_color(memory), memory)
        except (IndexError, TypeError):
            pass

        return peak_memory, peak_diff

    def get_concepts_datagen_header(self):
        HEADER  = [ 'Total', 'NEW', 'OLD', 'SAME', 'Title', 'Type', 'More', 'Sequel', 'Movie', 'TV Series', 'Season', 'Episode', 'Role', 'Person', 'Sport', 'Tournament', 'Team', 'Sports Group', 'Stadium', 'Channel', 'Affiliation', 'Genre', 'Award', 'Language', 'Decade', 'Phrase', 'Song', 'Total', 'Matches' ]

        DIFF_COMMON_STATS = [ 'TOTAL', 'NEW', 'OLD', 'SAME' ]

        DIFF_FIELD_STATS  = [ 'Ti', 'Vt', 'More' ]

        CONCEPTS_TYPE_STATS = [ 'sequel', 'movie', 'tvseries', 'season', 'episode', 'role', 'person', 'sport', 'tournament', 'team', 'sportsgroup', 'stadium', 'channel', 'channelaffiliation', 'genre', 'award', 'language', 'decade', 'phrase', 'song' ]

        OVERRIDE_STATS = ['Total Overrides', 'Override Matches']

        self.group_header = [ 'Diff Stats', 'Diff Field Stats', 'Type Stats', 'Override Stats']

        return [HEADER, DIFF_COMMON_STATS, DIFF_FIELD_STATS, CONCEPTS_TYPE_STATS, OVERRIDE_STATS]

    def get_content_datagen_header(self):
        HEADER  = [ 'Total', 'Movie', 'Sequel', 'TV Series', 'Episode', 'Season', 'Sport', 'Tournament', 'Team', 'Sports Group', 'Stadium', 'Channel', 'Affiliation', 'Role', 'Person', 'Genre', 'Award', 'Language', 'Decade', 'Phrase', 'Concept Phrase', 'Wiki Concept', 'Song', 'Invalid', 'Total', 'Matches' ]

        CONTENT_TYPE_STATS = [ 'total', 'movie', 'sequel', 'tvseries', 'episode', 'season', 'sport', 'tournament', 'team', 'sportsgroup', 'stadium', 'channel', 'channelaffiliation', 'role', 'person', 'genre', 'award', 'language', 'decade', 'phrase', 'concept', 'wikiconcept', 'song', 'invalid' ]

        OVERRIDE_STATS = ['Total Overrides', 'Override Matches']

        self.group_header = [ 'Type Stats', 'Override Stats']

        return [HEADER, CONTENT_TYPE_STATS, OVERRIDE_STATS]

    def get_episodedb_datagen_header(self):
        HEADER  = [ 'Total', 'TV Series']

        TYPE_STATS = [ 'total', 'tvseries']

        return [HEADER, TYPE_STATS]

    def get_episodedb_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_episodedb_datagen_header, [['episodedb_datagen.py'], ['EPISODEDB STATS']])

    def get_music_concepts_datagen_header(self):
        HEADER  = [ 'Total', 'Person', 'Song', 'Album', 'Genre', 'Total', 'Matches' ]

        DIFF_COMMON_STATS = [ 'TOTAL' ]

        CONCEPTS_TYPE_STATS = [ 'person', 'song', 'album', 'genre' ]

        OVERRIDE_STATS = ['Total Overrides', 'Override Matches']

        self.group_header = [ 'Diff Stats', 'Type Stats', 'Override Stats']

        return [HEADER, DIFF_COMMON_STATS, CONCEPTS_TYPE_STATS, OVERRIDE_STATS]

    def get_wiki_datagen_header(self):
        HEADER  = [ 'Total', 'Movie', 'Sequel', 'TV Series', 'Episode', 'Season', 'Sport', 'Tournament', 'Game', 'Team', 'Sports Group', 'Stadium', 'Channel', 'Affiliation', 'Role', 'Person', 'Genre', 'Award', 'Language', 'Decade', 'Phrase', 'Concept Phrase', 'Album', 'Song', 'Invalid', 'Total', 'Matches' ]

        WIKI_TYPE_STATS = [ 'total', 'movie', 'sequel', 'tv show', 'tv episode', 'season', 'sport', 'tournament','game', 'team', 'sportsgroup', 'stadium', 'tv channel', 'channelaffiliation', 'role', 'person', 'genre', 'award', 'language', 'decade', 'phrase', 'concept', 'album', 'song', 'invalid' ]

        OVERRIDE_STATS = ['field_stats']

        self.group_header = [ 'Type Stats', 'Field Stats']

        return [HEADER, WIKI_TYPE_STATS, OVERRIDE_STATS]
  
    def get_concepts_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_concepts_datagen_header, [['concepts_multiprocess.py'], ['NEW_CONCEPTS diff stats'], ['concepts_multiprocess.py'], ('nested', ['NEW_CONCEPTS diff stats'], ['DIFF']), ['concepts_multiprocess.py'], ['CONCEPTS_STATS'], ['concepts_multiprocess.py'], ['OVERRIDE_STATS']])

    def get_content_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_content_datagen_header, [['content_datagen.py'], ['CONTENT_STATS'], ['content_datagen.py'], ['OVERRIDE_STATS'], ['archival_update.py'], ['field_stats']])

    def get_wiki_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_wiki_datagen_header, [['archival_update.py'], ['type_stats'], ['archival_update.py'], ['type_stats']])

    def get_content_all_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_content_datagen_header, [['content_collector.py'], ['CONTENT_STATS'], ['content_collector.py'], ['OVERRIDE_STATS']])

    def get_music_concepts_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_concepts_datagen_header, [['concepts_multiprocess.py'], ['NEW_CONCEPTS diff stats'], ['concepts_multiprocess.py'], ['CONCEPTS_STATS'], ['concepts_multiprocess.py'], ['OVERRIDE_STATS']])

    def get_multilang_concepts_datagen_stats(self, row_list, obj):
        #self.get_stats_from_file(row_list, obj, self.get_concepts_datagen_header, [['multilang_concepts_datagen.py'], ['NEW_CONCEPTS diff stats'], ['multilang_concepts_datagen.py'], ('nested', ['NEW_CONCEPTS diff stats'], ['DIFF']), ['multilang_concepts_datagen.py'], ['CONCEPTS_STATS'], ['multilang_concepts_datagen.py'], ['OVERRIDE_STATS']])
        self.get_stats_from_file(row_list, obj, self.get_concepts_datagen_header, [['rules_diff.py'], ['NEW_CONCEPTS diff stats'], ['rules_diff.py'], ('nested', ['NEW_CONCEPTS diff stats'], ['DIFF']), ['multilang_concepts_datagen.py'], ['CONCEPTS_STATS'], ['multilang_concepts_datagen.py'], ['OVERRIDE_STATS']])

    def get_old_concepts_header(self):
        HEADER  = [ 'Total', 'NEW', 'OLD', 'SAME', 'Title', 'Type', 'More', 'Episode', 'Tvvideo', 'Movie', 'Tournament', 'TeamFolding', 'MusicArtist', 'ConceptFolding', 'Tvseries', 'PhraseFolding', 'Person' ]

        DIFF_COMMON_STATS = [ 'TOTAL', 'NEW', 'OLD', 'SAME' ]

        DIFF_FIELD_STATS  = [ 'Ti', 'Ty', 'More' ]

        CONCEPTS_TYPE_STATS = [ 'episode', 'tvvideo', 'movie', 'Tournament', 'TeamFolding', 'musicArtist', 'ConceptFolding', 'tvseries', 'PhraseFolding', 'PersonalityFolding' ]

        self.group_header = [ 'Diff Stats', 'Diff Field Stats', 'Type Stats' ]
        return [ HEADER, DIFF_COMMON_STATS, DIFF_FIELD_STATS, CONCEPTS_TYPE_STATS ]

    def get_old_concepts_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_old_concepts_header, [['rules_diff.py'], ['OPERATOR_CONCEPTS diff stats'], ['rules_diff.py'], ('nested', ['OPERATOR_CONCEPTS diff stats'], ['DIFF']), ['rules_diff.py'], ['OPERATOR_CONCEPTS Count stats']])

    def get_guid_merge_header(self):
        HEADER = [ 'VTV', 'ALL', 'SPORTS', 'IMDB WIKI', 'LASTFM WIKI', 'LASTFM LASTFM']

        GID_STATS = [ 'vtv', 'all', 'sports_wiki_merge', 'imdb_wiki_merge', 'lastfm_wiki_merge', 'lastfm_lastfm_merge']

        self.group_header = [ 'GID Stats' ]

        return [ HEADER, GID_STATS ]

    def get_guid_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_guid_merge_header, [['guid_merge.py'], ('union', ['guid_merge statistics'], ['total_merges'])])

    def get_imdb_fetch_header(self):
        HEADER = [ 'Total', 'New', 'Old' ]

        FETCH_STATS = [ 'Total', 'New', 'Old' ]

        self.group_header = [ 'Fetch Stats' ]

        return [ HEADER, FETCH_STATS ]

    def get_rovi_merge_header(self):
        HEADER = ['Total', 'Merged', 'Unmerged', 'WIKI_GIDS', 'FREEBASE_GIDS'] + \
                    ['Total Cases', 'Matched Cases', 'Passed', 'Failed', \
                        'False Positive', 'False negative', 'True positive', 'True negative']

        MERGE_STATS = ['total', 'merged', 'unmerged', 'wiki_gids', 'freebase_gids']
        MERGE_TEST_FIELDS = ['total', 'total_matched', 'passed', 'failed',\
                             'false_positive', 'false_negative', 'true_positive', 'true_negative']

        self.group_header = ['Merge Stats', 'Merge Test Stats']

        return [HEADER, MERGE_STATS, MERGE_TEST_FIELDS]

    def get_merge_header_new(self):
        HEADER = ['Total', 'Merged', 'WIKI', 'FREEBASE', 'No Candidate', 'Low Score', 'Title Mismatch'] + \
                    ['Total Cases', 'Matched Cases', 'Passed', 'Failed', \
                        'False Positive', 'False negative', 'True positive', 'True negative']

        MERGE_STATS = ['TOTAL', 'merge_count', 'wiki_gid_count', 'frb_gid_count', 'NO_CANDIDATE', 'low_score_count', 'title_mismatch_count']
        MERGE_TEST_FIELDS = ['total', 'total_matched', 'passed', 'failed',\
                             'false_positive', 'false_negative', 'true_positive', 'true_negative']

        self.group_header = ['Merge Stats', 'Merge Test Stats']

        return [HEADER, MERGE_STATS, MERGE_TEST_FIELDS]

    def get_rovi_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_merge_header, [['customize_stats.py'], ['Merge Stats'], ['customize_stats.py'], ['Merge Test Stats']])

    def get_merge_stats_new(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_merge_header_new, [['merge_datagen.py'], ['STATS'], ['merge_datagen.py'], ['STATS']])

    def get_episode_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_merge_header_new, [['merge_episode.py'], ['STATS'], ['merge_episode.py'], ['STATS']])

    def get_merge_parser_stats_list(self):
        group_list, stats_list = [], []
        for u in ( 'Aggregate', 'Normalize' ):
            for s in ( 'Wiki', 'Freebase', 'Rovi', 'WikiDB', 'RoviOTO' ):
                stats_list.append(['merge_parser.py'])
                n = ('nested', ['%s Stats' % u], [ s.lower() ])
                stats_list.append(n)
                group_list.append('%s %s Stats' % (u, s))
        return group_list, stats_list

    def get_merge_parser_header(self):
        group_list, stats_list = self.get_merge_parser_stats_list()
        group_count = len(group_list) / 2
        HEADER = [ 'movie', 'tvseries', 'episode', 'person' ] * group_count + [ 'movie', 'tvseries', 'episode', 'movie_person', 'tvseries_person', 'episode_person' ] * group_count
        MERGE_STATS = [ [ 'movie', 'tvseries', 'episode', 'person' ] ] * group_count + [  [ 'movie', 'tvseries', 'episode', 'movie_person', 'tvseries_person', 'episode_person' ] ] * group_count
        self.group_header = group_list

        return [ HEADER ] + MERGE_STATS

    def get_merge_parser_stats(self, row_list, obj):
        group_list, stats_list = self.get_merge_parser_stats_list()
        self.get_stats_from_file(row_list, obj, self.get_merge_parser_header, stats_list)

    def get_merge_phrase_header(self):
        HEADER = [ 'Seed', 'Source', 'Candidates', 'Selected', 'Merged', 'Unmerged' ]
        MERGE_STATS = [ 'seed', 'source', 'candidates', 'selected', 'merged', 'unmerged' ]
        self.group_header = [ 'Merge Stats' ]

        return [HEADER, MERGE_STATS]

    def get_merge_phrase_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_merge_phrase_header, [['merge_phrase.py'], ['Merge Stats']])

    def get_merge_check_stats_list(self):
        group_list, stats_list = [], []
        for s in ( 'Wiki Freebase', 'Wiki Rovi', 'Freebase Rovi' ):
            for t in ( 'Movie', 'Tvseries', 'Episode', 'Person' ):
                stats_list.append(['merge_check.py'])
                n = 'Merge %s %s Stats' % (s, t)
                stats_list.append([n])
                group_list.append(n)
        return group_list, stats_list

    def get_merge_check_header(self):
        group_list, stats_list = self.get_merge_check_stats_list()
        HEADER = [ 'Candidate', 'Final', 'Same', 'New', 'Old' ] * len(group_list)
        MERGE_STATS = [ [ 'Candidate', 'Final', 'Same', 'New', 'Old' ] ] * len(group_list)
        self.group_header = group_list

        return [ HEADER ] + MERGE_STATS

    def get_merge_check_stats(self, row_list, obj):
        group_list, stats_list = self.get_merge_check_stats_list()
        self.get_stats_from_file(row_list, obj, self.get_merge_check_header, stats_list)

    def get_merge_check_wikidb_stats_list(self):
        group_list, stats_list = [], []
        for s in ( 'Wikidb Wiki', 'Wikidb Freebase', 'Wikidb Rovi' ):
            for t in ( 'Episode', ):
                stats_list.append(['merge_check.py'])
                n = 'Merge %s %s Stats' % (s, t)
                stats_list.append([n])
                group_list.append(n)
        return group_list, stats_list

    def get_merge_check_wikidb_header(self):
        group_list, stats_list = self.get_merge_check_wikidb_stats_list()
        HEADER = [ 'Candidate', 'Final' ] * len(group_list)
        MERGE_STATS = [ [ 'Candidate', 'Final' ] ] * len(group_list)
        self.group_header = group_list

        return [ HEADER ] + MERGE_STATS

    def get_merge_check_wikidb_stats(self, row_list, obj):
        group_list, stats_list = self.get_merge_check_wikidb_stats_list()
        self.get_stats_from_file(row_list, obj, self.get_merge_check_wikidb_header, stats_list)

    def get_merge_check_rovioto_stats_list(self):
        group_list, stats_list = [], []
        for s in ( 'Wiki Rovioto', 'Freebase Rovioto' ):
            for t in ( 'Movie', 'Tvseries', 'Episode' ):
                stats_list.append(['merge_check.py'])
                n = 'Merge %s %s Stats' % (s, t)
                stats_list.append([n])
                group_list.append(n)
        return group_list, stats_list

    def get_merge_check_rovioto_header(self):
        group_list, stats_list = self.get_merge_check_rovioto_stats_list()
        HEADER = [ 'Candidate', 'Final' ] * len(group_list)
        MERGE_STATS = [ [ 'Candidate', 'Final' ] ] * len(group_list)
        self.group_header = group_list

        return [ HEADER ] + MERGE_STATS

    def get_merge_check_rovioto_stats(self, row_list, obj):
        group_list, stats_list = self.get_merge_check_rovioto_stats_list()
        self.get_stats_from_file(row_list, obj, self.get_merge_check_rovioto_header, stats_list)

    def get_merge_sports_stats_list(self):
        group_list, stats_list = [], []
        for n in ( 'Sport Stats', 'Team Stats', 'Tournament Stats', 'Group Stats', 'Stadium Stats', 'Player Stats' ):
            stats_list.append(['sports_merge.py'])
            stats_list.append([n])
            group_list.append(n)
        return group_list, stats_list

    def get_merge_sports_header(self):
        group_list, stats_list = self.get_merge_sports_stats_list()
        HEADER = [ 'Total', 'Final' ] * len(group_list)
        MERGE_STATS = [ [ 'Total', 'Final' ] ] * len(group_list)
        self.group_header = group_list

        return [ HEADER ] + MERGE_STATS

    def get_merge_sports_stats(self, row_list, obj):
        group_list, stats_list = self.get_merge_sports_stats_list()
        self.get_stats_from_file(row_list, obj, self.get_merge_sports_header, stats_list)

    def get_channel_db_header(self):
        HEADER = ['Added Channels', 'Deleted Channels', 'Title Changed']
        CHANNEL_DB_STATS_FIELDS = ['Added Channels', 'Deleted Channels', 'Title Changed']
        self.group_header = ['Channel DB Stats']

        return [HEADER, CHANNEL_DB_STATS_FIELDS]

    def get_channel_db_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_channel_db_header, [['channel_db_updater.py'], ('nested', ['channel db stats'], ['ChannelDb stats'])])

    def get_rovi_channel_datagen_header(self):
        HEADER = ['Total', 'Total', 'Premium', 'Network', 'Pay TV EU', 'Broadcast','PPV', 'Basic', 'Local']


        CHAN_FOLD_STATS_FIELDS = ['Total']
        CHANNEL_STATS_FIELDS = ['Total','Premium', 'Network', 'Pay TV EU', 'Broadcast','PPV', 'Basic', 'Local']

        self.group_header = ['ChanFold Stats', 'Channels Stats']

        return [HEADER, CHAN_FOLD_STATS_FIELDS, CHANNEL_STATS_FIELDS]

    def get_rovi_channel_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_channel_datagen_header, [['rovi_channel_datagen.py'], ('nested', ['rovi channel stats'], ['ChanFold Stats']), ['rovi_channel_datagen.py'], ('nested', ['rovi channel stats'], ['Channels Stats'])])

    def get_rovi_episode_merge_header(self):
        HEADER = ['Episodes Considered', 'Episodes Discarded', 'Merges', 'Template Merge']

        MERGE_STATS = ['episodes_considered', 'episodes_discarded', 'merges', 'rovi_episode_template_merge']

        self.group_header = ['Merge Stats']

        return [HEADER, MERGE_STATS]

    def get_rovi_episode_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_episode_merge_header, [['rovi_episode_merge.py'], ['Merge Stats']])

    def get_rovi_popularity_export_stats_header(self):
        self.group_header = [ 'KG_rovi_id_2.0',  'KG_rovi_id_1.1', 'PUBLISH_rovi_id_2.0',  'PUBLISH_rovi_id_1.1' ]
        field_list = ['group id', 'org id', 'credit id']
        HEADER = field_list * len(self.group_header)
        EACH_TYPE_STATS = [['group id', 'org id', 'credit id']]*len(self.group_header)
        return [ HEADER ] + EACH_TYPE_STATS

    def get_rovi_popularity_export_stats(self, row_list, obj):
        STATS_LIST = []
        for group in self.group_header:
            STATS_LIST.append(['RoviPopularityExport.py'])
            STATS_LIST.append(('nested', ['Pop Stats'], [group]))
        self.get_stats_from_file(row_list, obj, self.get_rovi_popularity_export_stats_header, STATS_LIST)

    def get_program_keywords_status_header(self):
        field_list = ['total', 'rovi_kwds', 'rovi_desc', 'wiki_kwds']
        gk_field_list = ['New GK Count']
        HEADER = gk_field_list + field_list * len(self.group_header)
        EACH_LANG_STATS = [gk_field_list] + [['total', 'rovi_kwds', 'rovi_desc', 'wiki_kwds']]*len(self.group_header[1:])
        return [ HEADER ] + EACH_LANG_STATS

    def get_program_keywords_stats(self, row_list, obj):
        self.group_header = [ 'Gk', 'eng', 'dan', 'dut', 'fin', 'fra', 'ger', 'ita', 'nor', 'por', 'spa', 'swe', 'tur', 'rus', 'pol']
        STATS_LIST = [['goodKeywords.py'], ['GK Stats']]
        for lang in self.group_header[1:]:
            STATS_LIST.append(['AllKeywordsStats.py'])
            STATS_LIST.append(('nested', ['All Source Stats'], [lang]))
        self.get_stats_from_file(row_list, obj, self.get_program_keywords_status_header, STATS_LIST)

    def get_kg_services_datagen_stats_header(self):
        self.group_header = ['KG Services Datagen Stats']
        HEADER = [' ProgramGenres ', ' ProgramSmartTag ', ' ProgramSequel ', ' SmartTag ', ' Aka ', ' Person ', ' Keywords ', ' SmartConnections ', ' Player ', ' Rovi_1_1 ', ' Rovi_2_0 ', ' ProgramCrew ', ' Popularity ', ' Team ']
        KG_MEATADATA_STATS = ['ProgramGenres.txt', 'ProgramSmartTag.txt', 'ProgramSequel.txt', 'SmartTag.txt', 'Aka.txt', 'Person.txt', 'Keywords.txt', 'SmartConnections.txt', 'Player.txt', 'IDTranslation_1_1.txt', 'IDTranslation_2_0.txt', 'ProgramCrew.txt', 'Popularity.txt', 'Team.txt']
        return [ HEADER, KG_MEATADATA_STATS ]

    def get_kg_services_datagen_header(self):
        self.group_header = [ 'KG Services Datagen Stats']
        HEADER = [ 'Movie', 'Sequel', 'TV Series', 'Episode', 'Tournament', 'Team', 'Sports Group', 'Stadium', 'Channel', 'Channel Affiliation', 'Role', 'Person', 'Genre', 'Award', 'Award Nomination', 'Language', 'Decade', 'Filter', 'Phrase', 'Rating', 'Concept', 'Related', 'Relevance Ratio', 'Connection' ]
        KG_SERVICES_DATAGEN_STATS = [ 'movie.json','sequel.json','tvseries.json','episode.json','tournament.json','team.json','sportsgroup.json','stadium.json','channel.json','channelaffiliation.json','role.json','person.json','genre.json','award.json','award_nomination.json','language.json','decade.json','filter.json','phrase.json','rating.json','concept.json','related.json','relevance_ratio.json','connection.json' ]
        return [ HEADER, KG_SERVICES_DATAGEN_STATS ]

    def get_kg_services_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_kg_services_datagen_header, [['kg_services_datagen.py'], ('union', ['KG_SERVICES_DATAGEN_STATS'], ['Total Records'])])

    def get_kg_metadata_stats_header(self):
        self.group_header = ['KG Metadata Stats']
        HEADER = [' ProgramGenres ', ' ProgramSmartTag ', ' ProgramSequel ', ' SmartTag ', ' Aka ', ' Person ', ' Keywords ', ' SmartConnections ', ' Player ', ' Rovi_1_1 ', ' Rovi_2_0 ', ' ProgramCrew ', ' Popularity ', ' Team ']
        KG_MEATADATA_STATS = ['ProgramGenres.txt', 'ProgramSmartTag.txt', 'ProgramSequel.txt', 'SmartTag.txt', 'Aka.txt', 'Person.txt', 'Keywords.txt', 'SmartConnections.txt', 'Player.txt', 'IDTranslation_1_1.txt', 'IDTranslation_2_0.txt', 'ProgramCrew.txt', 'Popularity.txt', 'Team.txt']
        return [ HEADER, KG_MEATADATA_STATS ]

    def get_kg_metadata_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_kg_metadata_stats_header, [['kg_metadata_datagen.py'], ['file_line_stat']])

    def get_chinese_merge_stats_header(self):
        self.group_header = ['douban', 'qq', 'letv', 'youkuall', 'pptv', 'funtv', 'pps', 'iqiyi', 'sohu', 'ninteenzerofive', 'kankan']
        field_list = ['merge_movie', 'total_movie', 'merge_tvseries', 'total_tvseries', 'merge_episode', 'total_episode']
        HEADER = field_list * len(self.group_header)
        EACH_SOURCE_STATS = [['merge_movie', 'total_movie', 'merge_tvseries', 'total_tvseries', 'merge_episode', 'total_episode']]*len(self.group_header)
        return [ HEADER ] + EACH_SOURCE_STATS

    def get_chinese_merge_stats(self, row_list, obj):
        STATS_LIST = []
        for source in self.group_header:
            STATS_LIST.append(['chinese_merge.py'])
            STATS_LIST.append(('nested', ['%s merge stats' % source.lower()], [source]))
        self.get_stats_from_file(row_list, obj, self.get_chinese_merge_stats_header, STATS_LIST)

    def get_chinese_seed_stats_header(self):
        self.group_header = ['douban', 'qq', 'letv', 'youkuall', 'pptv', 'funtv', 'pps', 'iqiyi', 'sohu', 'ninteenzerofive', 'kankan']
        field_list = ['movie', 'tvseries', 'episode']
        HEADER = field_list * len(self.group_header)
        EACH_SOURCE_STATS = [['movie', 'tvseries', 'episode']]*len(self.group_header)
        return [ HEADER ] + EACH_SOURCE_STATS

    def get_chinese_seed_stats(self, row_list, obj):
        STATS_LIST = []
        for source in self.group_header:
            STATS_LIST.append(['chinese_seed_generate.py'])
            STATS_LIST.append(('nested', ['%s seed stats' % source.lower()], [source]))
        self.get_stats_from_file(row_list, obj, self.get_chinese_seed_stats_header, STATS_LIST)

    def get_mvp_datagen_stats_header(self):
        self.group_header = ['Director', 'Genre', 'Keyword', 'Language', 'MovieRating', 'OriginalTitle', 'OriginCountry', 'Poster', 'Producer', 'ProductionCompany', 'ReleaseDate', 'ReleaseYear', 'Roles', 'Runtime', 'Synopsis', 'TopCast']
        tmp_field_list = ['heatmap_', 'kg_overlap_', '', 'filled_from_scraping_']
        field_list = ['heatmap', 'kg_overlap', 'filled', 'filled_from_scraping']
        MVP_DATAGEN_STATS = [['%s%s' % (item, gitem) for item in tmp_field_list] for gitem in self.group_header]
        HEADER = [field_list * len(self.group_header)]
        HEADER.extend(MVP_DATAGEN_STATS)
        return HEADER

    def get_mvp_movie_datagen_stats(self, row_list, obj):
        STATS_LIST = []
        for item in self.group_header:
            STATS_LIST.append(['mvp_datagen.py'])
            STATS_LIST.append(('nested', ['mvp datagen stats'], ['movie']))
        self.get_stats_from_file(row_list, obj, self.get_mvp_datagen_stats_header, STATS_LIST)
    '''
    def get_imdb_coverage_stats(self, row_list, obj):
        STATS_LIST = []
        self.group_header = ['Rating', 'Episode', 'Movie', 'Tvshow', 'Richmedia', 'Award', 'Crew', 'Other', 'Video', 'Boxoffice', 'Release', 'Othermedia']
        for item in self.group_header:
            STATS_LIST.append(['calculate_stats.py'])
            STATS_LIST.append(('nested', ['CALCULATE_IMDB_STATS'], [item]))
        self.get_stats_from_file(row_list, obj, self.get_imdb_coverage_stats_header, STATS_LIST)

    def get_imdb_coverage_stats_header(self):
        self.group_header = ['Rating', 'Episode', 'Movie', 'Tvshow', 'Richmedia', 'Award', 'Crew', 'Other', 'Video', 'Boxoffice', 'Release', 'Othermedia']
        fields = ['crawled_total', 'total', 'crawled_today', 'updated_today']
        HEADER = fields*len(self.group_header)
        EACH_SOURCE_STATS = [fields]*len(self.group_header)
        return [ HEADER ] + EACH_SOURCE_STATS
    '''
    def get_mvp_heatmap_stats_header(self):
        self.group_header = ['movie', 'tvseries', 'episode']
        field_movie = ['Runtime', 'ProductionCompany', 'Keyword', 'Synopsis', 'Language', 'Roles', 'Director', 'ReleaseDate', 'OriginalTitle', 'TopCast', 'Producer',\
                       'ReleaseYear', 'MovieRating', 'OriginCountry', 'ProductionCompany']
        field_tvseries = ['OriginCountry', 'Keyword', 'OriginalLanguage', 'OriginalTitle', 'ReleaseDate', 'Synopsis', 'Producer', 'Cast', 'Director', 'Runtime']
        field_episode  =  ['Synopsis', 'Runtime', 'OriginalTitle', 'Director', 'Cast', 'Keyword', 'Language', 'Producer', 'OAD']
        HEADER = field_movie + field_tvseries + field_episode
        EACH_SOURCE_STATS = [field_movie, field_tvseries, field_episode]
        return [ HEADER ] + EACH_SOURCE_STATS

    def get_mvp_heatmap_stats(self, row_list, obj):
        STATS_LIST = []
        self.group_header = ['movie', 'tvseries', 'episode']
        for item in self.group_header:
            STATS_LIST.append(['mvp_heatmap_datagen.py'])
            STATS_LIST.append(('nested', ['MVP_HEATMAP_DATAGEN_STATS'], [item]))
        self.get_stats_from_file(row_list, obj, self.get_mvp_heatmap_stats_header, STATS_LIST)

    def get_wiki_pv_pop_datagen_stats_heading(self):
        self.group_header = [ 'en', 'da', 'de', 'fi', 'fr', 'it', 'nl', 'no', 'es', 'pt', 'sv']
        field_list = ['movie 100', 'movie 200', 'movie 300', 'movie 400', 'movie 500', 'person 100', 'person 200', 'person 300', 'person 400', 'person 500', 'person 600', 'tvseries 100', 'tvseries 200', 'tvseries 300', 'tvseries 400', 'tvseries 500', 'tvseries 600', 'tvseries 700', 'tvseries 800']
        HEADER = field_list * len(self.group_header)
        EACH_LANG_STATS = [field_list]*len(self.group_header)
        return [ HEADER ] + EACH_LANG_STATS

    def get_wiki_pv_pop_datagen_stats(self, row_list, obj):
        STATS_LIST = []
        for lang in self.group_header:
            STATS_LIST.append(['GenPop.py'])
            STATS_LIST.append(('nested', ['PV Pop Stats'], [lang]))
        self.get_stats_from_file(row_list, obj, self.get_wiki_pv_pop_datagen_stats_heading, STATS_LIST)

    def get_wiki_pv_fetch_datagen_stats_heading(self):
        self.group_header = [ 'en', 'da', 'de', 'fi', 'fr', 'it', 'nl', 'no', 'es', 'pt', 'sv']
        field_list = [CONTENT_TYPE_PERSON, CONTENT_TYPE_MOVIE, CONTENT_TYPE_TVSERIES, CONTENT_TYPE_TEAM, CONTENT_TYPE_TOURNAMENT]
        HEADER = field_list * len(self.group_header)
        EACH_LANG_STATS = [field_list]*len(self.group_header)
        return [ HEADER ] + EACH_LANG_STATS

    def get_wiki_pv_fetch_datagen_stats(self, row_list, obj):
        STATS_LIST = []
        for lang in self.group_header:
            STATS_LIST.append(['WikiPVFetch.py'])
            STATS_LIST.append(('nested', ['PV Stats'], [lang]))
        self.get_stats_from_file(row_list, obj, self.get_wiki_pv_fetch_datagen_stats_heading, STATS_LIST)

    def get_role_merge_header(self):
        HEADER = [ 'Total Merges', 'Merged Roles', 'Unmerged Roles', 'One 2 Many Roles', 'Duplicate Roles', 'Manual Roles' ] + MERGE_STATS_HEADER

        MERGE_STATS = [ 'total_merges', 'merged_roles', 'unmerged_roles', 'one_to_many_merges', 'duplicate_roles', 'manual_merges' ]

        self.group_header = [ 'Merge Stats', 'Sanity Stats' ]

        return [ HEADER, MERGE_STATS, MERGE_STATS_FIELDS ]

    def get_role_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_role_merge_header, [['mergeImdbRoles.py'], ['Role Merge Stats'], ['merge_sanity.py'], ['ROLE_MERGE Sanity Statistics']])

    def get_sports_datagen_header(self):
        HEADER = [ 'PLAYERS FOLD', 'PLAYERS MC', 'SPORTS FOLD', 'SPORTS MC', 'TEAMS FOLD', 'TEAMS MC', 'TOURNAMENTS MC', 'SPORTS POPULARITY', 'GROUPS MC', 'STADIUMS MC']

        SPORTS_STATS = [ 'DATA_PLAYERS_FOLD_FILE Stats', 'DATA_PLAYERS_MC_FILE Stats', 'DATA_SPORTS_FOLD_FILE Stats', 'DATA_SPORTS_MC_FILE Stats', 'DATA_TEAMS_FOLD_FILE Stats', 'DATA_TEAMS_MC_FILE Stats', 'DATA_TOURNAMENTS_MC_FILE Stats', 'DATA_SPORTS_POPULARITY_FILE Stats','DATA_GROUPS_MC_FILE Stats','DATA_STADIUMS_MC_FILE Stats' ]

        self.group_header = [ 'Sports Stats' ]

        return [ HEADER ]  + [ SPORTS_STATS ]

    def get_sports_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_sports_datagen_header, [['data_diff.py'], ('union', [], ['Total'])])

    def get_sports_events_header(self):
        HEADER = ['Games', 'Stations', 'Error Games']
        EVENTS_STATS =  ['Games', 'Stations', 'Error Games']
        self.group_header = ['Sports Events Stats']
        return [HEADER, EVENTS_STATS]

    def get_sports_events_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_sports_events_header, [['sports_events_publisher.py'], ['Sports Events Stats']])

    def get_sports_ingestion_header(self):
        HEADER = ['EPG games', 'EPG live games', 'Sports DB games', 'Total',
                  'Merged games', 'Umerged games', 'Unmerged found in past merge', 'Unmerged drop candidates', 'Unmerged retain candidates',
                  'Meta From EPG', 'Meta From Sports DB', 'Schedule from EPG', 'Patched from SportsDB']
        RAW_COUNT_STATS =  ['EPG Programs', 'EPG Data Games', 'Sports Data Games', 'Total Games']
        MERGE_STATS =  ['Merged games', 'Unmerged games', 'Unmerged found in past merge', 'Unmerged dropped', 'Unmerged retained']
        UNIONIZATION_STATS =  ['Meta content from EPG', 'Meta content from Sports', 'Schedules from EPG', 'EPG Meta content patched with Sports']
        self.group_header = ['Sports Ingestion Stats', 'Merge Stats', 'Meta content unionization stats']
        return [HEADER, RAW_COUNT_STATS, MERGE_STATS, UNIONIZATION_STATS]

    def get_sports_ingestion_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_sports_ingestion_header, [['SportsDataIngestion.py'], ['Sports Ingestion Stats']] * 3)

    def get_espn_datagen_header(self):
        HEADER = [ 'PLAYERS FOLD', 'PLAYERS MC', 'TEAMS FOLD', 'TEAMS MC', 'TOURNAMENTS MC']

        SPORTS_STATS = [ 'ESPN_PLAYERS_FOLD_FILE', 'ESPN_PLAYERS_MC_FILE', 'ESPN_TEAMS_FOLD_FILE', 'ESPN_TEAMS_MC_FILE', 'ESPN_TOURNAMENTS_MC_FILE' ]

        self.group_header = [ 'ESPN Stats' ]

        return [ HEADER ]  + [ SPORTS_STATS ]

    def get_espn_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_espn_datagen_header, [['sports_file_generator.py'], ('union', [], ['total records'])])

    def get_sports_merge_header(self):
        HEADER = [ 'Total', 'Team', 'Person' ] + MERGE_STATS_HEADER

        MERGE_STATS = [ 'merge_count', 'team', 'person' ]

        self.group_header = [ 'Merge Stats', 'Sanity Stats' ]

        return [ HEADER, MERGE_STATS, MERGE_STATS_FIELDS ]

    def get_sports_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_sports_merge_header, [['sports_wiki_merge.py'], ['Sports Merge Statistics'], ['merge_sanity.py'], ['SPORTS_MERGE Sanity Statistics', 'Sanity Statistics']])

    def get_espn_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_sports_merge_header, [['sports_wiki_merge.py'], ['Sports Merge Statistics'], ['merge_sanity.py'], ['ESPN_MERGE Sanity Statistics', 'Sanity Statistics']])

    def get_sports_rovi_merge_header(self):
        HEADER = [ 'Total', 'Merged' ]

        MERGE_STATS = [ 'Total', 'Merged' ]

        self.group_header = [ 'Merge Stats' ]

        return [ HEADER, MERGE_STATS ]

    def get_sports_rovi_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_sports_rovi_merge_header, [['sports_rovi_merge.py'], ['Sports Merge']])

    def get_rovi_sports_merge_header(self):
        HEADER = [ 'Total Count', 'Total Merge', 'Automated Merge', 'Manual Merge', 'Override Merge']

        MERGE_STATS = list(HEADER)

        self.group_header = [ 'Sports Rovi Merge Stats' ]

        return [ HEADER, MERGE_STATS ]

    def get_rovi_sports_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_sports_merge_header, [['sports_merge.py'], ['sports_rovi_merge_count_stats']])

    def get_channel_rovi_merge_header(self):
        HEADER = [ 'Total', 'Merged' ]

        MERGE_STATS = [ 'Total', 'Merged' ]

        self.group_header = [ 'Merge Stats' ]

        return [ HEADER, MERGE_STATS ]

    def get_channel_rovi_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_channel_rovi_merge_header, [['channel_rovi_merge.py'], ['Channel Merge']])

    def get_channel_merge_header(self):
        HEADER = [ 'Total', 'Merges', 'Overrides', 'Candidates', 'Uniques', 'Duplicates' ]

        MERGE_STATS = HEADER

        self.group_header = [ 'Merge Stats' ]

        return [ HEADER, MERGE_STATS ]

    def get_channel_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_channel_merge_header, [['channel_merge.py'], ['Channel Merge']])

    def get_misc_kg_datagen_header(self):
        HEADER =  [ 'Languages', 'Region Count', 'Ratings', 'Decades', 'Filters',
                    'Genres', 'Grammars', 'Charmaps', 'Awards',
                    'Ipc'
                  ]

        MISC_STATS = HEADER

        self.group_header = [ 'Misc Stats' ]

        return [ HEADER, MISC_STATS ]

    def get_misc_kg_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_misc_kg_datagen_header, [['misc_kg_datagen.py'], ['Misc Datagen']])

    def get_multi_lang_datagen_header(self):
        HEADER = [ 'Publishes', 'Pushes', 'Archives', 'Lexicals' ]

        MISC_STATS = HEADER

        self.group_header = [ 'Generate Stats' ]

        return [ HEADER, MISC_STATS ]

    def get_multi_lang_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multi_lang_datagen_header, [['multi_lang.py'], ['Multi Lang Datagen']])

    def get_multi_lang_seed_datagen_header(self):
        HEADER = [ 'Movie File', 'TV Series File', 'Episode File',
                   'Tournament File', 'Team File',  'Person File', 'Stadium File',
                   'genre file', 'rating', 'filter', 'sportsgroup',
                   'phrase', 'decade', 'language', 'award',
                   'sequel', 'channel', 'channel-affiliation']
        MULTI_LANG_STATS_LIST = [ 'movie.data.gen.merge','tvseries.data.gen.merge', 'episode.data.gen.merge',
                                  'tournament.data.gen.merge', 'team.data.gen.merge', 'person.data.gen.merge', 'stadium.data.gen.merge',
                                  'genre.data.gen.merge', 'rating.data.gen.merge', 'filter.data.gen.merge', 'sportsgroup.data.gen.merge',
                                  'phrase.data.gen.merge', 'decade.data.gen.merge', 'language.data.gen.merge', 'award.data.gen.merge',
                                  'sequel.data.gen.merge','channel.data.gen.merge','channelaffiliation.data.gen.merge']

        self.group_header = [ 'Generate Stats' ]

        return [ HEADER, MULTI_LANG_STATS_LIST ]

    def get_multi_lang_seed_datagen_stats(self, row_list, obj):
        print row_list
        self.get_stats_from_file(row_list, obj, self.get_multi_lang_seed_datagen_header, [['gen_lang_seed.py'], ['MULTI LANG STATS']])


    def get_popularity_header(self):
        field_list = ['Total'] + [i for i in range(100, 1400, 100)]
        HEADER = field_list * len(self.group_header)
        FIELD_HEADING = [field_list]* len(self.group_header)
        return [ HEADER ] + FIELD_HEADING

    def get_popularity_stats(self, row_list, obj):
        self.pop_countries = ['USA', 'CAN', 'ARG', 'BRA', 'CHL', 'BOL', 'BOL', 'PRY', 'COL', 'ECU', 'CHN', 'FRA', 'ESP', 'GBR', 'PRT', 'DEU', 'ITA', 'DNK', 'SWE', 'FIN', 'CHE', 'NLD', 'NOR']
        self.pop_langs = ['eng', 'dan', 'dut', 'fin', 'fra', 'ger', 'ita', 'nor', 'por', 'spa', 'swe', 'zho' ]
        self.pop_regions = ['EU', 'LA', 'BRA', 'CAN', 'CHN', 'USA']
        self.group_header = ['base'] + self.pop_countries + self.pop_langs + self.pop_regions
        group_header_keys = ['base'] + map(lambda x: "%s_country" % x.lower(), self.pop_countries) + map(lambda x: "%s_lang" % x.lower(), self.pop_langs) + map(lambda x: "%s_region" % x.lower(), self.pop_regions)
        STATS_LIST = []
        for group in group_header_keys:
            STATS_LIST.append(['CountryPop.py'])
            STATS_LIST.append(('nested', ['Country Pop Stats'], [group]))
        self.get_stats_from_file(row_list, obj, self.get_popularity_header, STATS_LIST)

    def get_keyword_header(self):
        field_list = [ 'TOTAL_PHRASE_RECORDS', 'WIKI_RECORDS' ]
        HEADER = field_list * len(self.group_header)
        EACH_LANG_STATS = [[ 'TOTAL_PHRASE_RECORDS', 'WIKI_RECORDS']]*len(self.group_header)
        return [ HEADER ] + EACH_LANG_STATS

    def get_keyword_stats(self, row_list, obj):
        self.group_header = [ 'eng', 'dan', 'dut', 'fin', 'fra', 'ger', 'ita', 'nor', 'por', 'spa', 'swe', 'tur', 'rus', 'pol']
        STATS_LIST = []
        for lang in self.group_header:
            STATS_LIST.append(['phraseICIRGen.py'])
            STATS_LIST.append(('nested', ['Keywords Corpus Stats'], [lang]))
        self.get_stats_from_file(row_list, obj, self.get_keyword_header, STATS_LIST)

    def get_wiki_program_kwds_stats_header(self):
        self.group_header = []
        return [ [] ]

    def get_wiki_program_kwds_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_wiki_program_kwds_stats_header, [['WikiProgramKeywords.py'], []])

    def get_kramer_lexical_header(self):
        HEADER = [ 'Total Records Processed', 'Total Phrases Processed']

        LEXICAL_STATS = [ 'meta_file', 'phrase_list' ]

        self.group_header = [ 'Kramer Lexical Stats' ]

        return [ HEADER, LEXICAL_STATS ]

    def get_kramer_lexical_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_kramer_lexical_header, [['kramer_lexical_datagen.py'], ['lexical']])

    def get_music_temporal_pop_header(self):
        HEADER = [ 'Top Tracks', 'Top Artists', 'New artist gid', 'Hyped Tracks', 'Hyped Artists', 'Wiki Temporal' , 'AllMusic Artists', 'AllMusic Albums' ]

        STATS = [ 'top tracks', 'top artists', 'new artist gid', 'hyped tracks', 'hyped artists', 'wiki temporal', 'allmusic artists', 'allmusic albums' ]

        self.group_header = [ 'Music Temporal Pop Stats' ]

        return [ HEADER, STATS ]

    def get_music_temporal_pop_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_temporal_pop_header, [['MusicTemporalPopularity.py'], ['temporal_pop_stats']])

    def get_music_lexical_header(self):
        HEADER = [ 'Lines']

        STATS = [ 'lines']

        self.group_header = [ 'Music Lexical Stats' ]

        return [ HEADER, STATS ]

    def get_music_lexical_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_lexical_header, [['lexical_datagen.py'], ['Lexical Datagen Stats']])

    def get_lastfm_datagen_header(self):
        HEADER = ['Genres', 'Decades', 'Artists', 'Albums', 'Songs', 'Duplicate songs', 'Unique songs',  'Artists',
                   'Band Members', 'Genre',  'Decade', 'Description', 'Image',
                   'Wiki Links', 'Old gid', 'Recommendations',
                   'Albums', 'Artist', 'Genre', 'Decade', 'Image',
                   'Songs', 'Artist', 'Album', 'Genre', 'Decade', 'Missing Album',
                   'Test Cases', 'Missing Gids', 'Affected parent Gids', 'Failed',
                   'Total', 'Overridden', 'Matches'
                ]

        COMMON_STATS = ['genres', 'decades']

        UPDATE_POP_STATS = ['artists', 'albums', 'songs']

        INTERNAL_MERGE_STATS = ['merged songs', 'unique gids']

        ARTIST_STATS = ['artists', 'band members', 'artist genre', 'artist decade', \
                        'artist desc', 'artist image', 'wiki links', 'artist old', 'recommendations']

        ALBUM_STATS = ['albums', 'album artists', 'album genre', 'album decade', 'album image']

        SONG_STATS = ['songs', 'song artist', 'song album', 'song genre', 'song decade', 'missing song album']

        SANITY_STATS = [ 'test_cases', 'missing_gids', 'affected_parent_gids', 'failed' ]
        OVERRIDE_STATS = [ 'Total Overrides', 'Overridden Count', 'Override Matches']

        self.group_header = [ 'Common', 'Update Pop', 'Internal Merge', 'Artist', 'Album', 'Songs', 'Sanity Stats', 'Override Stats' ]

        return [ HEADER, COMMON_STATS, UPDATE_POP_STATS, INTERNAL_MERGE_STATS, ARTIST_STATS, ALBUM_STATS, SONG_STATS, SANITY_STATS, OVERRIDE_STATS ]

    def get_lastfm_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_lastfm_datagen_header, [['lastFMDatagen.py'], ['lastfm_stats'], \
            ['updateLastfmPop.py'], ['lastfm_pop_stats'], \
            ['internal_merge.py'], ['internal_merge_stats'],\
            ['lastFMDatagen.py'], ['lastfm_stats'], ['lastFMDatagen.py'], ['lastfm_stats'], \
            ['lastFMDatagen.py'], ['lastfm_stats'], ['music_datagen_sanity.py'], ['LASTFM Datagen Sanity'], \
            ['lastFMDatagen.py'] , ['override_stats'] ])

    def get_lastfm_pop_datagen_header(self):
        HEADER = [ 'Songs', 'Artists', 'Albums' ]

        LASTFM_STATS = [ 'songs', 'artists', 'albums' ]

        self.group_header = [ 'Lastfm Pop Stats' ]

        return [ HEADER, LASTFM_STATS ]

    def get_lastfm_pop_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_lastfm_pop_datagen_header, [['updateLastfmPop.py'], ['lastfm_pop_stats']])

    def get_lastfm_merge_header(self):
        HEADER = [ 'Total', 'Track', 'Music Artist', 'Album' ] + MERGE_STATS_HEADER

        MERGE_STATS = [ 'merge_count', 'song', 'PersonalityFolding', 'album' ]

        self.group_header = [ 'Merge Stats', 'Sanity Stats' ]

        return [ HEADER, MERGE_STATS, MERGE_STATS_FIELDS ]

    def get_lastfm_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_lastfm_merge_header, [['merge_data.py'], ['Lastfm Merge Statistics'], ['merge_sanity.py'], ['LASTFM_MERGE Sanity Statistics', 'Sanity Statistics']])

    def get_rovi_music_merge_header(self):
        HEADER = [ 'Albums merged', 'Albums unmerged', 'Artists merged', 'Artists unmerged', 'Tracks Folded', 'Empty sks', 'Unique sks', 'Non unique sks', 'Total add overrides', 'Add overrides solved', 'Add overrides unsolved', 'Total remove overrides', 'Remove overrides solved', 'Remove overrides unsolved', 'Merge duplicates' ]

        MERGE_STATS = [ 'albums merged', 'albums unmerged', 'artists merged', 'artists unmerged', 'tracks folded', 'empty lfm song sks', 'unique song sks', 'non unique song sks', 'total add overrides', 'add overrides solved', 'add overrides unsolved', 'total remove overrides', 'remove overrides solved', 'remove overrides unsolved', 'merge duplicates' ]

        self.group_header = [ 'New Rovi Music Merge Stats']

        return [ HEADER, MERGE_STATS ]

    def get_rovi_music_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_music_merge_header, [['generate_music_merge.py'], ['Rovi Music Merge datagen Stats']])

    def get_cur_candidate_music_merge_header(self):
        HEADER = [ 'Albums merged', 'Albums unmerged', 'Artists merged', 'Artists unmerged', 'Tracks Folded', 'Empty sks', 'Unique sks', 'Non unique sks', 'Total add overrides', 'Add overrides solved', 'Add overrides unsolved', 'Total remove overrides', 'Remove overrides solved', 'Remove overrides unsolved', 'Merge duplicates' ]

        MERGE_STATS = [ 'albums merged', 'albums unmerged', 'artists merged', 'artists unmerged', 'tracks folded', 'empty lfm song sks', 'unique song sks', 'non unique song sks', 'total add overrides', 'add overrides solved', 'add overrides unsolved', 'total remove overrides', 'remove overrides solved', 'remove overrides unsolved', 'merge duplicates' ]

        self.group_header = [ 'Cur Candidate Music Merge Stats']

        return [ HEADER, MERGE_STATS ]

    def get_cur_candidate_music_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur_candidate_music_merge_header, [['generate_music_merge.py'], ['Cur Candidate Music Merge datagen Stats']])

    def get_rovi_music_candidate_header(self):
        HEADER = [ 'Artist Candidates', 'Album Candidates', 'Song Candidates']

        CANDIDATE_STATS = [ 'artists', 'albums', 'songs' ]

        self.group_header = [ 'Rovi Music Candidate Stats']

        return [ HEADER, CANDIDATE_STATS ]

    def get_rovi_music_candidate_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_music_candidate_header, [['generate_candidates.py'], ['Rovi Music Candidate datagen Stats']])

    def get_cur_music_candidate_header(self):
        HEADER = [ 'Artist Candidates', 'Album Candidates', 'Song Candidates']

        CANDIDATE_STATS = [ 'artists', 'albums', 'songs' ]

        self.group_header = [ 'Cur Music Candidate Stats']

        return [ HEADER, CANDIDATE_STATS ]

    def get_cur_music_candidate_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur_music_candidate_header, [['generate_candidates.py'], ['Cur Music Candidate datagen Stats']])

    def get_cur_music_merge_header(self):
        HEADER = [ 'Artist Merges', 'Album Merges', 'Track Merges']

        MERGE_STATS = [ 'artists merged', 'albums merged', 'tracks merged' ]

        self.group_header = [ 'Cur Music Merge Stats']

        return [ HEADER, MERGE_STATS ]

    def get_cur_music_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur_music_merge_header, [['get_amg_2_rovi_music_ids.py'], ['Cur Music Merge datagen Stats']])

    def get_id_music_merge_header(self):
        HEADER = [ 'Artist Merges', 'Album Merges', 'Track Merges']

        MERGE_STATS = [ 'artists merged', 'albums merged', 'tracks merged' ]

        self.group_header = [ 'Id Music Merge Stats']

        return [ HEADER, MERGE_STATS ]

    def get_id_music_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_id_music_merge_header, [['id_music_merge_datagen_2.py'], ['Id Music Merge datagen Stats']])

    def get_music_server_header(self):
        HEADER = [ 'MC Records', 'Artist', 'Album', 'Song', 'Genres', 'Decades', 'Song Rys Available', 'Song Rys Assigned', 'Song Writer', 'Song Composer', 'Source Folds',
                   'Total', 'Overridden', 'Matches'
                 ]
        MERGE_STATS = [ 'MC_records', 'PersonalityFolding', 'album', 'song', 'genre', 'decade', 'song_rys_available', 'song_rys_assigned', 'composer', 'writer', 'num_source_folds', ]
        OVERRIDE_STATS = [ 'Total Overrides', 'Overridden Count', 'Override Matches']
        self.group_header = [ 'Server Stats', 'Override Stats' ]

        return [ HEADER, MERGE_STATS, OVERRIDE_STATS ]

    def get_music_server_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_server_header, [['publishMusicTGZ.py'], ['music_publish_stats'], ['publishMusicTGZ.py'], ['override_stats' ]])

    def get_music_publish_header(self):
        HEADER = [ 'Songs MC', 'Artists MC' ]
        MERGE_STATS = [ 'songs mc', 'artists mc' ]

        self.group_header = [ 'Music Publish Stats' ]

        return [ HEADER, MERGE_STATS ]

    def get_music_publish_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_publish_header, [['publishMusicTGZ.py'], ['Music Publish datagen Stats']])

    def get_music_lexical_server_header(self):
        HEADER = [ 'Count']

        LEXICAL_STATS = [ 'lines' ]

        self.group_header = [ 'Lexical Stats' ]

        return [ HEADER, LEXICAL_STATS ]

    def get_music_lexical_server_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_lexical_server_header, [['publishMusicLexicalTGZ.py'], ['music_lexical_publish_stats']])

    def get_lexical_datagen_header(self):
        HEADER = [ 'Count']

        LEXICAL_STATS = [ 'lines' ]

        self.group_header = [ 'Lexical Stats' ]

        return [ HEADER, LEXICAL_STATS ]

    def get_lexical_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_lexical_datagen_header, [['lexical_datagen.py'], ['Lexical Datagen Stats']])

    def get_rules_header(self):
        HEADER = [ 'Channel', 'Total', 'Movie', 'TV Series', 'Episodes', 'Person', 'Total', 'Sequel', 'Movie', 'TV Series', 'Episodes', 'Person', 'Total', 'Songs', 'Music Artist', 'Total', 'Tournament', 'Team', 'Sports Person', 'Stadium', 'Sports Group', 'Total', 'Team', 'Tournament', 'Channel', 'Person', 'Movie', 'TV Series', 'Episodes', 'Songs', 'Season', 'Disambig' ]

        CHANNEL_STATS = [ 'channel' ]
        ROVI_STATS = [ 'Total', 'movie', 'tvseries', 'episode', 'PersonalityFolding' ]
        FRB_STATS = ['Total', 'SequelFolding', 'movie', 'tvseries', 'episode', 'PersonalityFolding' ]
        MUSIC_STATS = [ 'Total', 'song', 'PersonalityFolding' ]
        SPORTS_STATS = [ 'Total', 'Tournament', 'team', 'person', 'stadium', 'sportsgroup' ]
        WIKI_STATS = [ 'Total', 'TeamFolding', 'Tournament', 'channel', 'PersonalityFolding', 'movie', 'tvseries', 'episode', 'song', 'season', 'disambig' ]
        self.group_header = [ 'CHANNEL', 'ROVI', 'FRB', 'MUSIC', 'SPORTS', 'WIKI' ]

        return [ HEADER, CHANNEL_STATS, ROVI_STATS, FRB_STATS, MUSIC_STATS, SPORTS_STATS, WIKI_STATS ]

    def get_multilang_rules_header(self):
        HEADER = [ 'Total', 'Movie', 'TV Series', 'Episodes', 'Person', 'Total', 'Team', 'Tournament', 'Channel', 'Person', 'Movie', 'TV Series', 'Episodes', 'Songs', 'Season', 'Disambig' ]

        ROVI_STATS = [ 'Total', 'movie', 'tvseries', 'episode', 'PersonalityFolding' ]
        WIKI_STATS = [ 'Total', 'TeamFolding', 'Tournament', 'channel', 'PersonalityFolding', 'movie', 'tvseries', 'episode', 'song', 'season', 'disambig' ]
        self.group_header = [ 'ROVI', 'WIKI' ]

        return [ HEADER, ROVI_STATS, WIKI_STATS ]

    def get_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rules_header, [ ['rules_diff.py'], ['ROVI_CHANNEL Count stats'], ['rules_diff.py'], ['ROVI Count stats'], ['rules_diff.py'], ['FREEBASE Count stats'], ['rules_diff.py'], ['MUSIC Count stats'], ['rules_diff.py'], ['SPORTS Count stats'], ['rules_diff.py'], ['WIKI Count stats'] ] )

    def get_music_rules_header(self):
        HEADER = [ 'Person', 'Song', 'Album']

        MUSIC_STATS = [ 'person', 'song', 'album' ]
        self.group_header = [ 'MUSIC STATS', ]

        return [ HEADER, MUSIC_STATS]

    def get_music_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_rules_header, [ ['rules_diff.py'], ['MUSIC_SEED Count stats'], ] )

    def get_cur_music_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_rules_header, [ ['rules_diff.py'], ['CUR_SEED Count stats'], ] )

    def get_push_kramer_header(self):
        HEADER = [ 'count',]

        COUNT_STATS = [ 'count', ]
        self.group_header = [ 'PUSH STATS', ]

        return [ HEADER, COUNT_STATS]

    def get_push_kramer_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_push_kramer_header, [ ['push_kramer_data.py'], ['Count Stats'], ] )

    def get_er_phrase_header(self):
        HEADER = [ 'Tag-Zone 1','Tag-Zone 2','Tag-Zone 3','Tag-Zone 4','Tag-Zone 5','phrases','normalized phrase']

        COUNT_STATS = [ 'Tag-Zone 1','Tag-Zone 2','Tag-Zone 3','Tag-Zone 4','Tag-Zone 5','phrases','normalized phrases']
        self.group_header = [ 'ER PHRASE STATS', ]

        return [ HEADER, COUNT_STATS]

    def get_er_phrase_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_er_phrase_header, [ ['er_phrase_datagen.py'], ['er phrase datagen'], ] )

    def get_fra_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multilang_rules_header, [ ['rules_diff.py'], ['ROVI_FRA Count stats'], ['rules_diff.py'], ['WIKI_FRA Count stats'] ] )

    def get_nor_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multilang_rules_header, [ ['rules_diff.py'], ['ROVI_NOR Count stats'], ['rules_diff.py'], ['WIKI_NOR Count stats'] ] )

    def get_ger_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multilang_rules_header, [ ['rules_diff.py'], ['ROVI_GER Count stats'], ['rules_diff.py'], ['WIKI_GER Count stats'] ] )

    def get_spa_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multilang_rules_header, [ ['rules_diff.py'], ['ROVI_FIN Count stats'], ['rules_diff.py'], ['WIKI_SPA Count stats'] ] )

    def get_swe_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multilang_rules_header, [ ['rules_diff.py'], ['ROVI_SWE Count stats'], ['rules_diff.py'], ['WIKI_SWE Count stats'] ] )

    def get_ita_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multilang_rules_header, [ ['rules_diff.py'], ['ROVI_ITA Count stats'], ['rules_diff.py'], ['WIKI_ITA Count stats'] ] )

    def get_por_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multilang_rules_header, [ ['rules_diff.py'], ['ROVI_POR Count stats'], ['rules_diff.py'], ['WIKI_POR Count stats'] ] )

    def get_dut_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multilang_rules_header, [ ['rules_diff.py'], ['ROVI_DUT Count stats'], ['rules_diff.py'], ['WIKI_DUT Count stats'] ] )

    def get_dan_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multilang_rules_header, [ ['rules_diff.py'], ['ROVI_DAN Count stats'], ['rules_diff.py'], ['WIKI_DAN Count stats'] ] )

    def get_fin_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multilang_rules_header, [ ['rules_diff.py'], ['ROVI_FIN Count stats'], ['rules_diff.py'], ['WIKI_FIN Count stats'] ] )

    def get_tur_rules_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_multilang_rules_header, [ ['rules_diff.py'], ['ROVI_TUR Count stats'], ['rules_diff.py'], ['WIKI_TUR Count stats'] ] )

    def get_seed_header(self):
        HEADER = [ 'Movie', 'Sequel', 'TV Series', 'Episode', 'Tournament', 'Team', 'Sports Group', 'Stadium', 'Channel', 'Affiliation', 'Role', 'Person', 'Genre', 'Award', 'Language', 'Decade', 'Filter', 'Phrase', 'Rating', 'Sport', 'Production House', 'Original Air Channel', 'Aka', 'Ik', 'Total', 'Matches' ]

        SEED_STATS = [ 'movie', 'sequel', 'tvseries', 'episode', 'tournament', 'team', 'sportsgroup', 'stadium', 'channel', 'channelaffiliation', 'role', 'person', 'genre', 'award', 'language', 'decade', 'filter', 'phrase', 'rating', 'sport', 'productionhouse', 'originalchannel' ]

        COUNT_STATS = ['Ak count', 'Ik count']

        OVERRIDE_STATS = [ 'Overridden Count', 'Override Matches' ]

        self.group_header = [ 'Seed Stats', 'Count Stats', 'Override Stats' ]

        return [ HEADER, SEED_STATS, COUNT_STATS, OVERRIDE_STATS ]

    def get_seed_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_seed_header, [['seed_stats.py'], ('union', ['Seed Stats'], ['Content Type'], ['Total']), ['seed_stats.py'], ('union', ['Seed Stats'], ['']), ['generate_full_seed.py'], ('union', [], [''])])

    def get_seed_updater_header(self):
        self.group_header = ['SEED UPDATER Stats']
        HEADER = [' Movie ', ' TV Series ', ' Episode ', ' Person ', ' Genre ']
        SEED_UPDATER_STATS = ['movie', 'tvseries', 'episode', 'person', 'genre']
        return [ HEADER, SEED_UPDATER_STATS ]

    def get_seed_updater_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_seed_updater_header, [['seed_updater.py'], ['Seed Updater Stats']])

    def get_seed_patcher_header(self):
        self.group_header = ['SEED PATCHER Stats']
        HEADER = [' Newly Written ', ' Episode Parent not Tvseries ', ' Already in KG ', ' KG-Rovi Type Mismatch ']
        SEED_PATCHER_STATS = ['newly_written', 'epi_parent_not_tvseries_cnt', 'already_in_kg_cnt',  'vt_mismatch']
        return [ HEADER, SEED_PATCHER_STATS ]

    def get_seed_patcher_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_seed_patcher_header, [['seed_updater.py'], ['SeedPatcherStats']])

    def get_seed_archive_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_seed_header, [['seed_stats.py'], ('union', ['Archive Stats'], ['Content Type'], ['Total']), ['seed_stats.py'], ('union', ['Archive Stats'], ['']), ['data_overrides.py'], ('union', [], [''])])

    def get_check_crawler_header(self):
        HEADER = ['183 edb']

        CHECK_CRAWLERS_STATS = ['10.4.18.183.EDB']

        self.group_header = [ 'Failures' ]

        return [ HEADER, CHECK_CRAWLERS_STATS ]

    def get_check_crawler_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_check_crawler_header, [['check_crawlers.py'], ['Failure Stats']])

    def get_rating_datagen_header(self):
        HEADER = [ 'RATINGS', 'MOVIE', 'TVSERIES', 'TVVIDEO', 'EPISODES' ]

        RATING_DATAGEN_STATS = [ 'rating', 'movie', 'tvseries', 'tvvideo', 'episode' ]

        self.group_header = [ 'RATING DATAGEN STATS' ]

        return [ HEADER, RATING_DATAGEN_STATS]

    def get_rating_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rating_datagen_header, [['seed_rating_datagen.py'], ('union', ['Rating Datagen Stats'], ['Ra'])])

    def get_crew_datagen_header(self):
        HEADER = [ 'TOTAL', 'MOVIE', 'TVSERIES', 'TVVIDEO', 'EPISODES' ]

        CREW_DATAGEN_STATS = [ 'total', 'movie', 'tvseries', 'tvvideo', 'episode' ]

        self.group_header = [ 'CREW DATAGEN STATS' ]

        return [ HEADER, CREW_DATAGEN_STATS]

    def get_crew_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_crew_datagen_header, [['seed_crew_datagen.py'], ['Crew datagen Stats']])

    def get_genre_datagen_header(self):
        self.group_header = [ 'Movie', 'Tvseries', 'Episode', 'Person' ]

        field_list = [ 'Ge', 'Gg' ]
        HEADER = field_list * len(self.group_header)

        MOVIE_GENRE_STATS = TVSERIES_GENRE_STATS = EPISODE_GENRE_STATS = PERSON_GENRE_STATS = field_list

        return [ HEADER, MOVIE_GENRE_STATS, TVSERIES_GENRE_STATS, EPISODE_GENRE_STATS, PERSON_GENRE_STATS]

    def get_genre_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_genre_datagen_header, [['seed_genres_datagen.py'], ('nested', ['Genre Association Count Stats'], [CONTENT_TYPE_MOVIE]), ['seed_genres_datagen.py'], ('nested', ['Genre Association Count Stats'], [CONTENT_TYPE_TVSERIES]), ['seed_genres_datagen.py'], ('nested', ['Genre Association Count Stats'], [CONTENT_TYPE_EPISODE]), ['seed_genres_datagen.py'], ('nested', ['Genre Association Count Stats'], [CONTENT_TYPE_PERSON])])

    def get_ig_datagen_header(self):
        HEADER = [ 'TOTAL', 'MOVIE', 'TVSERIES', 'EPISODES', 'PERSON', 'TEAM', 'TOURNAMENT' ]

        IG_DATAGEN_STATS = [ 'total', 'movie', 'tvseries', 'episode', 'person', 'team', 'tournament']

        self.group_header = [ 'IG DATAGEN STATS' ]

        return [ HEADER, IG_DATAGEN_STATS]

    def get_ig_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_ig_datagen_header, [['ig_datagen.py'], ['IG datagen Stats']])

    def get_seed_diff_header(self):
        HEADER = ['Gids', 'Movie', 'Tvseries', 'Tvvideo', 'Episode', 'Fold',
                  'Role',  'Sequel', 'Popularity', 'Channel', 'Chanfold']

        SEED_DIFF_STATS = ['veveo_gid.list',
                           'movie.data.gen.merge',
                           'tvseries.data.gen.merge',
                           'tvvideo.data.gen.merge',
                           'episode.data.gen.merge',
                           'fold.data.gen.merge',
                           'role.data.gen.merge',
                           'sequel.data.gen.merge',
                           'DATA_POP.BASE',
                           'channel.data.gen.merge',
                           'chanfold.data.gen.merge']

        self.group_header = ['SEED DIFF STATS']

        return [ HEADER, SEED_DIFF_STATS ]

    def get_seed_diff_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_seed_diff_header, [['seed_diff.py'], ('union', ['Diff Count'], ['Total'])])

    def get_rovi_json_datagen_stats_header(self):
        HEADER = ['Movie', 'TVEpisode', 'TVSeries', 'TVSeason', 'Person', 'Organization', 'BroadcastService', 'BroadcastEvent', 'CableOrSatelliteService', 'TelevisionChannel']
        self.group_header = ['Rovi Json Schema Stats']
        return [ HEADER, HEADER]

    def get_rovi_json_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_json_datagen_stats_header, [['generate_diff.py'], ['Shallow count stats']])

    def get_rovi_json_datagen_stats_old(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_json_datagen_stats_header, [['count_stats.py'], ['Shallow count stats']])

    def get_rovi_datagen_stats_header(self):
        HEADER = ['movie', 'tvseries', 'episode', 'tvvideo']

        ROVI_DATA_STATS = HEADER

        self.group_header = ['Rovi Data Stats']

        return [ HEADER, ROVI_DATA_STATS]

    def get_rovi_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_datagen_stats_header, [['rovi_merge_meta.py'], ['Count stats']])

    def get_sequel_datagen_stats_header(self):
        HEADER = ['wiki', 'freebase', 'wikidb', 'static']

        SEQUEL_DATA_STATS = HEADER

        self.group_header = ['Sequel Data Stats']

        return [HEADER, SEQUEL_DATA_STATS]

    def get_sequel_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_sequel_datagen_stats_header, [['sequel_datagen.py'], ['Count stats']])

    def get_rovi_crew_merge_stats_header(self):
        HEADER = ['Merges']

        ROVI_CREW_MERGE_STATS = ['Merge Stats']

        MERGE_STATS = ['Merged']

        self.group_header = ['Merge Stats']

        return [ HEADER, MERGE_STATS ]

    def get_rovi_crew_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_crew_merge_stats_header, [['crew_merge_standalone.py'], ('union', ['Rovi Crew Merge Stats'], ['']), ['crew_merge_standalone.py'], ])

    def get_seed_source_datagen_header(self):
        HEADER = [ 'INTERNETVOD', 'EDB', 'IMAGEDB' ]

        SEED_SOURCE_DATAGEN_STATS = [ '10.4.2.207.INTERNETVOD', '10.4.18.183.edb', '10.4.2.207.IMAGEDB' ]

        self.group_header = [ 'SEED SOURCE STATS' ]

        return [ HEADER, SEED_SOURCE_DATAGEN_STATS ]

    def get_seed_source_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_seed_source_datagen_header, [['seed_source_datagen.py'], ('union', ['Count Stats'], ['Total'])])

    def get_genre_movies_merge_header(self):
        HEADER = [ 'Total Merges', 'Unmerged', 'Gids without candidate' ]

        GENRE_MOVIES_MERGE_STATS = [ 'total_merges', 'unmerged', 'gids_without_candidates' ]

        self.group_header = [ 'Genre moves merge Stats']

        return [ HEADER, GENRE_MOVIES_MERGE_STATS ]

    def get_genre_movies_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_genre_movies_merge_header, [['genre_movies_merge.py'], ['GENRES MERGE STATS']])

    def get_patch_seed_header(self):
        HEADER = [ 'New Merges' ]

        PATCH_STATS = [ 'New merges' ]

        self.group_header = [ 'Patch Stats' ]

        return [ HEADER, PATCH_STATS ]

    def get_patch_seed_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_patch_seed_header, [['seed_stats.py'], ['Patch Stats']])

    def get_rovi_bound_fetch_header(self):
        HEADER = [ 'USA Bound', 'AUS Bound', 'Brazil Bound', 'Russia Bound', 'India Bound', 'China Bound', 'SEA Bound', 'Turkey Bound', 'Canada Bound', 'EU Bound', 'LA Bound' ]

        FILE_COUNT_STATS = [ 'USA_bound', 'AUS_bound', 'BRA_bound', 'RUS_bound', 'IND_bound', 'CHN_bound', 'SEA_bound', 'TUR_bound', 'CAN_bound', 'EU_bound', 'LA_bound' ]

        self.group_header = [ 'File Count' ]

        return [ HEADER, FILE_COUNT_STATS ]

    def get_rovi_bound_fetch_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_bound_fetch_header, [['rovi_fetch.py'], ['File Count Hash']])

    def get_rovi_unbound_fetch_header(self):
        HEADER = [ 'All Unbound', 'AUS Unbound', 'Brazil Unbound', 'Russia Unbound', 'Japan Unbound', 'India Unbound', 'China Unbound', 'SEA Unbound', 'Turkey Unbound' ]

        FILE_COUNT_STATS = [ 'ALL_unbound', 'AUS_unbound', 'BRA_unbound', 'RUS_unbound', 'JPN_unbound', 'IND_unbound', 'CHN_unbound', 'SEA_unbound', 'TUR_unbound' ]

        self.group_header = [ 'File Count' ]

        return [ HEADER, FILE_COUNT_STATS ]

    def get_rovi_unbound_fetch_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_unbound_fetch_header, [['rovi_fetch.py'], ['File Count Hash']])

    def get_tribune_fetch_header(self):
        HEADER = [ 'Files', ' VOD files' ]
        FILE_COUNT_STATS = [ 'file_count', 'vod_file_count']
        self.group_header = [ 'File Count' ]
        return [ HEADER, FILE_COUNT_STATS ]

    def get_tribune_fetch_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_tribune_fetch_header, [['tribune_fetcher.py'], ['File Count Hash']])

    def get_star_type_header(self):
        HEADER = ['Actor', 'Director', 'Producer', 'Host', 'Player', 'Music Artist', 'Person', 'Music Band', 'Prize', 'Subgenre', 'Mood', 'Duo', 'Overrides', 'Matches', ]
        self.group_header = ['Count Stats', 'Override Stats' ]
        count_stats = [CONTENT_XT_ACTOR, CONTENT_XT_DIRECTOR, CONTENT_XT_PRODUCER, CONTENT_XT_HOST, CONTENT_XT_PLAYER, \
                       CONTENT_XT_MUSIC_ARTIST, CONTENT_XT_PERSON, CONTENT_XT_MUSIC_BAND, CONTENT_XT_PRIZE, CONTENT_XT_SUBGENRE, CONTENT_XT_MOOD, CONTENT_XT_DUO]
        override_stats = ['Overridden Count', 'Override Matches']
        return [ HEADER, count_stats, override_stats ]

    def get_star_type_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_star_type_header, [['star_type_csv.py'], ['COUNT STATS'], ['star_type_csv.py'], ['COUNT STATS']])

    def get_old_star_type_header(self):
        HEADER = ['Actor', 'Director', 'Producer', 'Host', 'Player', 'Music Artist', 'Person', 'Music Band', 'Prize', 'Subgenre', 'Mood', 'Duo', 'Overrides', 'Matches', 'OLD', 'NEW', 'OLD ONLY', 'NEW ONLY', 'COMMON', 'DIFF', 'Total', 'Changed', 'New', 'Removed', 'Added', 'Deleted', 'to_actor', 'to_director', 'to_musicartist', 'to_host', 'to_person']
        self.group_header = ['Count Stats', 'Override Stats', 'Diff Count Stats', 'Merge Diff Stats', 'FB converted Xt' ]
        count_stats = [CONTENT_XT_ACTOR, CONTENT_XT_DIRECTOR, CONTENT_XT_PRODUCER, CONTENT_XT_HOST, CONTENT_XT_PLAYER, \
                       CONTENT_XT_MUSIC_ARTIST, CONTENT_XT_PERSON, CONTENT_XT_MUSIC_BAND, CONTENT_XT_PRIZE, CONTENT_XT_SUBGENRE, CONTENT_XT_MOOD, CONTENT_XT_DUO]
        override_stats = ['Overridden Count', 'Override Matches']
        diff_count_stats = ['OLD', 'NEW', 'OLD ONLY', 'NEW ONLY', 'COMMON', 'DIFF']
        merge_diff_stats = ['DIFF', 'CHANGE', 'NEW', 'REMOVE', 'ADD', 'DELETE']
        fb_xt_convert = ['to_actor', 'to_director', 'to_musicartist', 'to_host', 'to_person']
        return [ HEADER, count_stats, override_stats, diff_count_stats, merge_diff_stats, fb_xt_convert]

    def get_old_star_type_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_old_star_type_header, [['star_type_datagen.py'], ['COUNT STATS'], ['star_type_datagen.py'], ['COUNT STATS'], ['star_type_datagen.py'], ['DIFF COUNT STATS'], ['star_type_datagen.py'], ['MERGE DIFF'], ['star_type_datagen.py'], ['FB_MAPPED_XT']])

    def get_internal_merge_header(self):
        HEADER = ['TOTAL', 'PARENTS', 'CHILDREN', 'FRESH', 'MISSING', 'OLD', 'NEW', 'FRESH', 'CHANGED', 'MISSING', 'OLD', 'NEW', 'FRESH', 'CHANGED', 'MISSING']
        self.group_header = ['Count Stats', 'Parents Stats', 'Children Stats']
        count_stats = ['TOTAL', 'PARENTS', 'CHILDREN', 'FRESH', 'MISSING',]
        parents_stats = ['OLD', 'NEW', 'FRESH', 'CHANGED', 'MISSING']
        children_stats = ['OLD', 'NEW', 'FRESH', 'CHANGED', 'MISSING']
        return [HEADER, count_stats, parents_stats, children_stats]

    def get_internal_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_internal_merge_header, [['internal_guid_merger.py'], ['COUNT STATS'], ['internal_guid_merger.py'], ['PARENTS STATS'], ['internal_guid_merger.py'], ['CHILDREN STATS']])

    def get_new_live_scores_header(self):
        self.group_header = ['Games Stats', 'Tournament Stats', 'Groups Stats']
        GAME_STATS = ['Total','tennis', 'basketball', 'football', 'baseball','hockey', 'golf',
                      'cricket', 'soccer', 'motorsports', 'cycling', 'horseracing']
        TOU_STATS = ['Total','scheduled','ongoing','completed']
        GROUP_STATS = ['Total']
        HEADER = GAME_STATS + TOU_STATS + GROUP_STATS
        return [ HEADER, GAME_STATS, TOU_STATS, GROUP_STATS]

    def get_new_live_scores_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_new_live_scores_header, [['redis_sports_updater.py'], ('union', ['Games count'], ['Total']), ['redis_sports_updater.py'], ['Tournaments count'], ['redis_sports_updater.py'], ['Total Groups']])

    def get_sports_data_populate_header(self):
        self.group_header = ['Count Stats']
        tou_stats = ['New Winners','Tournaments Found','Tournaments Not Found','Tournaments Without Winner']
        HEADER = tou_stats
        return [ HEADER,tou_stats]

    def get_sports_data_populate_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_sports_data_populate_header, [['sports_data_populate.py'], ['Count Stats']])

    def get_tvpublish_vod_stats_header(self):
        HEADER = [
                   'Total', 'Movie', 'Sequel', 'TV Series', 'Episode', 'Tournament', 'Team', 'VOD', 'Music Video', 'Role', 'Genre', 'Subgenre', 'Language', 'Decade', 'Person', 'Music Artist', 'Phrase', 'Concept Phrase', 'Program', 'Fields', 'total_sources'
                ]

        MC_STATS = [ 'Total', 'movie', 'SequelFolding', 'tvseries', 'episode', 'Tournament', 'TeamFolding', 'VOD', 'musicvideo', 'role', 'genre', 'subgenre', 'language', 'decade', 'PersonalityFolding', 'musicArtist', 'PhraseFolding', 'ConceptFolding' ]

        DROPPED_GID_STATS = ['Program', 'Fields']
        VOD_SOURCES_STATS = ['total_sources']

        self.group_header = ['MC Stats', 'Dropped gid stats', 'Vod sources stats']

        return [HEADER, MC_STATS, DROPPED_GID_STATS, VOD_SOURCES_STATS]

    def get_tvpublish_vod_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_tvpublish_vod_stats_header, [['data_diff.py#data_diff.pyc'], ['DATA_MC_FILE Stats'],['TVPublish.py'], ['DROPPED_GIDS'], ['TVPublish.py'], ['TOTAL_SOURCES']])

    def get_tvpublish_merge_stats_header(self):
        HEADER = ['Fandango count', 'Netflix count', 'Modified Records count', 'Total lines written']

        MERGE_STATS = ['fandango', 'Modified records count', 'Total lines written', 'netflix']

        self.group_header = ['Merge stats']

        return [HEADER, MERGE_STATS]

    def get_tvpublish_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_tvpublish_merge_stats_header, [['TVPublish.py'], ['TVPUBLISH SOURCE MERGE']])

    def get_epg_patch_header(self):
        HEADER = [ 'Total Patch', 'Season Patch', 'Series Patch', 'Unmerged Games', 'Merged Games', 'Actual Games', 'Non Games', 'Veveo Games', 'Veveo Games Merged', 'Program Gameso' ]

        SHOW_STATS = [ 'total_patch_count', 'season_patch_count', 'series_patch_count' ]

        SPORTS_STATS = [ 'Unmerged_tribune_games', 'Tribune_Games_merged', 'Tribune_actual_games', 'Tribune_non_games', 'Veveo_games_from_db', 'Veveo_games_merged', 'Tribune_program_game_candidates' ]

        self.group_header = [ 'Show Stats', 'Sports Stats' ]

        return [ HEADER, SHOW_STATS, SPORTS_STATS ]

    def get_epg_patch_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_tvpublish_merge_stats_header, [['EpgTvshowPremierePatch.py'], ['EPG TVSHOW PREMIERE PATCH STATS'], ['EpgSportsPatch.py'], ['EPG SPORTS PATCH STATS:']])

    def get_epg_patch_header_tribune2(self):
        HEADER = [ 'Total Patch', 'Season Patch', 'Series Patch', 'Total Merged Games', 'Total Unmerged Games', 'Total Games', 'Total Non Games', 'Total Non Live Games', 'Total Live Games', 'Games in Ld', 'Non Games in Ld', 'Non Live Games in Ld', 'Live Games in Ld', 'Games in dish', 'Non Games in dish', 'Non Live Games in dish', 'Live Games in dish' ]

        SHOW_STATS = [ 'total_patch_count', 'season_patch_count', 'series_patch_count' ]

        SPORTS_STATS = [ 'Total_tribune_games_merged_with_sports_data', 'Total_tribune_games_not_merged_with_sports_data', 'Total_games_in_all_headends', 'Total_non_games_in_all_headends', 'Total_non_live_games_in_all_headends', 'Total_live_games_in_all_headends', 'Total_games_in_london_derry', 'Total_non_games_in_london_derry', 'Total_non_live_games_in_london_derry', 'Total_live_games_in_london_derry', 'Total_games_in_dish_denver_and_sanf', 'Total_non_games_in_dish_denver_and_sanf', 'Total_non_live_games_in_dish_denver_and_sanf', 'Total_live_games_in_dish_denver_and_sanf'  ]

        self.group_header = [ 'Show Stats', 'Sports Stats' ]

        return [ HEADER, SHOW_STATS, SPORTS_STATS ]

    def get_epg_patch_stats_tribune2(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_tvpublish_merge_stats_header, [['EpgTvshowPremierePatch.py'], ['EPG TVSHOW PREMIERE PATCH STATS'], ['EpgSportsPatch.py'], ['EPG SPORTS PATCH STATS:']])

    def get_tvpublish_header(self):
        HEADER = [
                   'Total', 'Movie', 'Sequel', 'TV Series', 'Episode', 'Tournament', 'Team', 'VOD', 'Music Video', 'Role', 'Genre', 'Subgenre', 'Language', 'Decade', 'Person', 'Music Artist', 'Phrase', 'Concept Phrase',
                   'Total', 'Sequel Fold', 'VOD', 'Tournament Fold', 'Team Fold', 'Intersection Fold', 'Concept Fold', 'Attribute Fold', 'Episode Fold', 'Role', 'Genre', 'Subgenre', 'Language', 'Decade', 'Person Fold', 'Phrase Fold',
                   'Total', 'Movie', 'TV Video', 'Sequel', 'TV Series', 'Episode', 'Tournament', 'Team', 'Channel', 'Music Video', 'Role', 'Genre', 'Subgenre', 'Language', 'Decade', 'Concept Fold', 'Person', 'Music Artist', 'Phrase Fold',
                 ]

        MC_STATS = [ 'Total', 'movie', 'SequelFolding', 'tvseries', 'episode', 'Tournament', 'TeamFolding', 'VOD', 'musicvideo', 'role', 'genre', 'subgenre', 'language', 'decade', 'PersonalityFolding', 'musicArtist', 'PhraseFolding', 'ConceptFolding' ]

        FOLD_STATS = [ 'Total', 'SequelFolding', 'VOD', 'TournamentFolding', 'TeamFolding', 'IntersectionFold', 'ConceptFolding', 'AttributeFolding', 'EpisodeFolding', 'role', 'genre', 'subgenre', 'language', 'decade', 'PersonalityFolding', 'PhraseFolding', ]

        POP_STATS = [ 'Total', 'movie', 'tvvideo', 'SequelFolding', 'tvseries', 'episode', 'Tournament', 'TeamFolding', 'channel', 'musicvideo', 'role', 'genre', 'subgenre', 'language', 'decade', 'ConceptFolding', 'PersonalityFolding', 'musicArtist', 'PhraseFolding', ]

        self.group_header = [ 'MC Stats', 'Fold Stats', 'Pop Stats' ]

        return  [ HEADER, MC_STATS, FOLD_STATS, POP_STATS ]

    def get_tvpublish_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_tvpublish_header, [['data_diff.py#data_diff.pyc'], ['DATA_MC_FILE Stats'], ['data_diff.py#data_diff.pyc'], ['DATA_FOLD_FILE Stats'], ['data_diff.py#data_diff.pyc'], ['DATA_POPULARITY_FILE Stats']])

    def get_related_header(self):
        HEADER = [ 'Total', 'Final', 'Empty', 'Pruned', 'Skipped' ] * 2
        RELATED_STATS = [ 'Total', 'Final', 'Empty', 'Pruned', 'Skipped' ]
        self.group_header = [ 'Movie', 'TV Series' ]
        return  [ HEADER, RELATED_STATS, RELATED_STATS ]

    def get_related_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_related_header, [['similar.py'], ('nested', ['Similar'], ['movie']), ['similar.py'], ('nested', ['Similar'], ['tvseries'])])

    def get_dtv_pa_skyb_related_convert_header(self):
        self.group_header = [ 'DTV Current', 'DTV Stable', 'PA Current', 'PA Stable', 'SKYB Current', 'SKYB Stable' ]
        groups = len(self.group_header)
        HEADER = [ 'Total', 'Related' ] * groups
        RELATED_STATS = [ 'total', 'related' ]
        return  [ HEADER ] + [ RELATED_STATS ] * groups

    def get_dtv_pa_skyb_related_convert_stats(self, row_list, obj):
        related_list = []
        for source in ( 'dtv', 'pa', 'skyb' ):
            for type in ( 'current', 'stable' ):
                related_list.extend([['similar_gids_to_sk.py'], ('nested', ['Similar'], ['%s %s' % (source, type)])])
        self.get_stats_from_file(row_list, obj, self.get_dtv_pa_skyb_related_convert_header, related_list)

    def get_dtv_pa_skyb_related_loader_header(self):
        self.group_header = [ 'DTV Search', 'DTV Redis', 'PA Search', 'PA Redis', 'SKYB Search', 'SKYB Redis' ]
        groups = len(self.group_header) / 2
        HEADER = [ 'Total', 'Current', 'Stable', 'Total', 'VG Gids' ] * groups
        RELATED_STATS = [ [ 'total', 'current', 'stable' ], [ 'total', 'vg_gids' ] ]
        return  [ HEADER ] + RELATED_STATS * groups

    def get_dtv_pa_skyb_related_loader_stats(self, row_list, obj):
        related_list = []
        for source in ( 'dtv', 'pa', 'skyb' ):
            for type in ( 'search', 'redis' ):
                related_list.extend([['related_loader.py'], ('nested', ['Loader'], ['%s %s' % (source, type)])])
        self.get_stats_from_file(row_list, obj, self.get_dtv_pa_skyb_related_loader_header, related_list)

    def get_kg_related_loader_header(self):
        self.group_header = [ 'KG Search', 'KG Redis' ]
        groups = len(self.group_header) / 2
        HEADER = [ 'Total', 'Current', 'Stable', 'Total', 'VG Gids' ] * groups
        RELATED_STATS = [ [ 'total', 'current', 'stable' ], [ 'total', 'vg_gids' ] ]
        return  [ HEADER ] + RELATED_STATS * groups

    def get_kg_related_loader_stats(self, row_list, obj):
        related_list = []
        source = 'kg'
        for type in ( 'search', 'redis' ):
            related_list.extend([['related_loader.py'], ('nested', ['Loader'], ['%s %s' % (source, type)])])
        self.get_stats_from_file(row_list, obj, self.get_kg_related_loader_header, related_list)

    def get_kramer_dump_header(self):
        HEADER = [ 'Movie File', ' TV Video File', 'TV Series File', 'Episode File',
                   'Tournament File', 'Team File',  'Person File', 'Stadium File',
                   'genre file', 'rating', 'filter', 'sportsgroup',
                   'phrase', 'decade', 'language', 'award', 'role',
                   'sequel', 'channel', 'channel-affiliation',
                    'Good Keywords', 'Quote',
                   'LastFM',
                   'Wiki AKA' , 'Wiki AKA Incr', 'Excluded Chanels']

        KRAMER_STATS_LIST = [ 'movie.data.gen.merge', 'tvvideo.data.gen.merge', 'tvseries.data.gen.merge', 'episode.data.gen.merge',
                              'tournament.data.gen.merge', 'team.data.gen.merge', 'person.data.gen.merge', 'stadium.data.gen.merge',
                              'genre.data.gen.merge', 'rating.data.gen.merge', 'filter.data.gen.merge', 'sportsgroup.data.gen.merge',
                              'phrase.data.gen.merge', 'decade.data.gen.merge', 'language.data.gen.merge', 'award.data.gen.merge', 'role.data.gen.merge',
                              'sequel.data.gen.merge','channel.data.gen.merge','channelaffiliation.data.gen.merge',
                               'good_kw', 'quote-count',
                              'lastfm',
                              'arc_aka', 'incr_aka' , 'excluded_chanels']
        self.group_header = [ 'Kramer Dump Stats( No of Gids read from each file)' ]

        return [ HEADER, KRAMER_STATS_LIST ]


    def get_kramer_dump_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_kramer_dump_header, [['kramerdb_dump.py'], ['Main Stats']])

    def get_kramer_incr_dump_header(self):
        HEADER = [ 'Tournament', 'Team', 'Game', 'Player', 'Stadium',
                   'Good Keywords', 'Quote', 'instant2',
                   'instant1' , 'instant', 'EPG', 'Roles', 'concepts',
                   'Wiki AKA' , 'Wiki AKA Incr',  'Excluded Chanels']

        KRAMER_STATS_LIST = [ 'tournaments', 'team', 'game', 'player', 'stadium',
                              'good_kw', 'quote-count', 'DATA_MC_FILE4',
                              'DATA_MC_FILE3','DATA_MC_FILE2', 'DATA_MC_FILE1', 'role-count', 'concept_vdb',
                              'arc_aka', 'incr_aka' ,'excluded_chanels']
        self.group_header = [ 'Kramer Dump Stats' ]

        return [ HEADER, KRAMER_STATS_LIST ]

    def get_kramer_incr_dump_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_kramer_incr_dump_header, [['kramerdb_dump.py'], ['Main Stats']])

    def get_kramer_incr_header(self):
        HEADER = [  'Movie',  'Sequel', 'TV Series', 'Episode' , 'Tournament' ,'Team', 'Channel', 'Chan Folds',
                    'Role', 'Genre',  'Language', 'Decade', 'Person', 'Phrase', 'Concept Phrase', 'wikiconcept', 'game',
                    'Sportsfilter' , 'Stadium', 'Award', 'Sportsgroup',
                    "actor", "director", "producer" , "role played", "has role", "award", "game", "team",  "player",
                    "host", "genre", "writer", "composer", "guest",
                    "Episode", "Tournament", "Role", "Tvseries", "Movie", "Person Folding", "Music Band", "Game", "Award", "Genre", "Stadium",
                    'Tournament', 'Team', 'Game', 'Player', 'Stadium', 'Good Keywords', 'Quote', 'instant2',
                    'instant1' , 'instant', 'EPG', 'Roles', 'concepts', 'Wiki AKA' , 'Wiki AKA Incr',  'Excluded Chanels', 'count']

        GID_META_STATS = [ 'movie',  'sequelfolding', 'tvseries', 'episode', 'tournament', 'teamfolding', 'channel', 'foldedonchannelaffiliation',
                           'role' , 'genre' , 'language', 'decade', 'personalityfolding', 'phrasefolding', 'conceptfolding', 'wikiconcept',
                           'game', 'sportsfilter', 'stadium', 'award', 'sportsgroup',]


        ENTITY_STATS = [ "e_1", "e_2" , "e_3", "e_7",  "e_8", "e_9",  "e_12", "e_13", "e_18",  "e_21", "e_22", "e_23", "e_24", "e_30"]

        GRAPH_NODES = ["g_episode", "g_tournament" , "g_role" , "g_tvseries", "g_movie", "g_personalityfolding", "g_musicband", "g_game",  "g_award", "g_genre", "g_stadium",]

        KRAMER_STATS_LIST = [ 'tournaments', 'team', 'game', 'player', 'stadium',
                              'good_kw', 'quote-count', 'DATA_MC_FILE4',
                              'DATA_MC_FILE3','DATA_MC_FILE2', 'DATA_MC_FILE1', 'role-count', 'concept_vdb',
                              'arc_aka', 'incr_aka' ,'excluded_chanels']

        COUNT_STATS = [ 'count', ]

        self.group_header = [ 'Gid Meta Stats', 'Graph Edges Stats', "Graph Nodes", "Kramer Dump Stats", "Push stats"]

        return [ HEADER, GID_META_STATS, ENTITY_STATS , GRAPH_NODES, KRAMER_STATS_LIST, COUNT_STATS]


    def get_kramer_incr_stats(self, row_list, obj):
        summary_list = [['kramerdb_datagen.py'], ['Kramer Database']] * 3 + [['kramerdb_dump.py'], ['Main Stats']] + [ ['push_kramer_data.py'], ['Count Stats'] ]
        self.get_stats_from_file(row_list, obj, self.get_kramer_incr_header, summary_list)

    def get_kramer_music_dump_header(self):
        HEADER = [
                   'Albums mc','fold file' , 'song mc', 'artist mc', 'concepts',
                   'Wiki AKA' , 'Wiki AKA Incr',  ]

        KRAMER_STATS_LIST = [ 'DATA_ALBUM_MC_FILE', 'DATA_MUSIC_FOLD_FILE','DATA_SONG_MC_FILE',
                               'DATA_ARTIST_MC_FILE', 'concept_vdb',
                              'arc_aka', 'incr_aka' ,]
        self.group_header = [ 'Kramer Dump Stats' ]

        return [ HEADER, KRAMER_STATS_LIST ]

    def get_trending_topics_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_trending_topics_header, [['trending_topics.py'], ['TRENDING TOPICS']] * 6)

    def get_trending_topics_header(self):
        HEADER = ['Topics' , 'Topic-Topic Pairs', 'Topic-Kwd Pairs', 'Items from source']*6
        GOOGLE_STATS = ['google_topics', 'google_topictopic_pairs', 'google_topickwd_paris', 'google']
        YOUTUBE_STATS = ['youtube_topics', 'youtube_topictopic_pairs', 'youtube_topickwd_paris', 'youtube']
        TWITTER_STATS = ['twitter_topics', 'twitter_topictopic_pairs', 'twitter_topickwd_paris', 'twitter']
        WIKIEVENTS_STATS = ['wikievents_topics', 'wikievents_topictopic_pairs', 'wikievents_topickwd_paris', 'wikievents']
        GOOGLE_NEWS_STATS = ['google_news_topics', 'google_news_topictopic_pairs', 'google_news_topickwd_paris', 'google_news']
        ALL_STATS = ['all_topics', 'all_topictopic', 'all_topickwd', 'all']
        self.group_header = ['Twitter', 'Google', 'Youtube', 'Wiki Events', 'Google News', 'All']
        return [HEADER, TWITTER_STATS, GOOGLE_STATS, YOUTUBE_STATS, WIKIEVENTS_STATS, GOOGLE_NEWS_STATS, ALL_STATS]

    def get_daily_trends_aggregate_header(self):
        HEADER = [ 'count',]

        COUNT_STATS = [ 'count', ]
        self.group_header = [ 'DAILY TRENDS STATS', ]

        return [ HEADER, COUNT_STATS]

    def get_daily_trends_aggregate_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_daily_trends_aggregate_header, [ ['daily_trending_aggregator.py'], ['TRENDING TOPICS AGGERGATE'], ] )


    def get_kramer_music_dump_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_kramer_music_dump_header, [['kramerdb_dump.py'], ['Main Stats']])

    def get_new_kramer_datagen_header(self):
        HEADER = [  'Phrase Ti', 'Phrase Va', 'Phrase Ak', 'Phrase Ik',
                    'Movie',  'Sequel', 'TV Series', 'Episode' , 'Tournament' ,'Team', 'Channel',
                    'Role', 'Genre',  'Language', 'Decade', 'Person', 'Phrase', 'Concept Phrase', 'wikiconcept', 'game',
                    'Sportsfilter' , 'Stadium', 'Award', 'Sportsgroup',
                    "actor", "director", "producer" , "role played", "has role", "award", "game", "team", "sequel",
                    "tvseries",  "player", "host", "genre", "writer", "composer", "guest",
                    "Episode", "Tournament","Role", "Tvseries", "Movie", "Person Fold", "Music Band", "Game", "Award", "Genre", "Stadium"
                 ]

        PHRASE_STATS = ["Phrase Ti", "Phrase Va", "Phrase Ak", "Phrase Ik"]

        GID_META_STATS =[ 'movie',  'sequelfolding', 'tvseries', 'episode', 'tournament', 'teamfolding', 'channel',
                          'role' , 'genre' , 'language', 'decade', 'personalityfolding', 'phrasefolding', 'conceptfolding', 'wikiconcept',
                          'game', 'sportsfilter', 'stadium', 'award', 'sportsgroup',]

        ENTITY_STATS = [ "e_1", "e_2" , "e_3", "e_7",  "e_8", "e_9",  "e_12", "e_13", "e_15", "e_16", "e_18", "e_21", "e_22", "e_23", "e_24", "e_30"]

        GRAPH_NODES = ["g_episode", "g_tournament" , "g_role" , "g_tvseries", "g_movie", "g_personalityfolding", "g_musicband", "g_game",  "g_award", "g_genre", "g_stadium",]

        self.group_header = [ 'Phrase Stats', 'Gid Meta Stats', 'Graph Edges Stats', 'Graph Nodes' ]

        return [ HEADER, PHRASE_STATS, GID_META_STATS, ENTITY_STATS, GRAPH_NODES ]

    def get_new_kramer_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_new_kramer_datagen_header, [['kramerdb_datagen.py'], ['Kramer Database']] * 4)

    def get_new_kramer_datagen_header_incr(self):
        HEADER = [  "Movie",  "Sequel", "TV Series", "Episode" , "Tournament" ,"Team", "Channel", "Chan Folds",
                    "Role", "Genre",  "Language", "Decade", "Person", "Phrase", "Concept Phrase", "wikiconcept", "game",
                    "Sportsfilter" , "Stadium", "Award", "Sportsgroup",
                    "actor", "director", "producer" , "role played", "has role", "award", "game", "team",  "player",
                    "host", "genre", "writer", "composer", "guest",
                    "Episode", "Tournament", "Role", "Tvseries", "Movie", "Person Folding", "Music Band", "Game", "Award", "Genre", "Stadium",]

        GID_META_STATS = [ 'movie',  'sequelfolding', 'tvseries', 'episode', 'tournament', 'teamfolding', 'channel', 'foldedonchannelaffiliation', 'role' , 'genre' , 'language', 'decade', 'personalityfolding', 'phrasefolding', 'conceptfolding', 'wikiconcept', 'game', 'sportsfilter', 'stadium', 'award', 'sportsgroup',]

        ENTITY_STATS = [ "e_1", "e_2" , "e_3", "e_7",  "e_8", "e_9",  "e_12", "e_13", "e_18",  "e_21", "e_22", "e_23", "e_24", "e_30"]

        GRAPH_NODES = ["g_episode", "g_tournament" , "g_role" , "g_tvseries", "g_movie", "g_personalityfolding", "g_musicband", "g_game",  "g_award", "g_genre", "g_stadium",]

        self.group_header = [ 'Gid Meta Stats', 'Graph Edges Stats', "Graph Nodes"]

        return [ HEADER, GID_META_STATS, ENTITY_STATS , GRAPH_NODES]

    def get_new_kramer_datagen_incr_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_new_kramer_datagen_header_incr, [['kramerdb_datagen.py'], ['Kramer Database']] * 3)

    def get_new_kramer_music_datagen_header_incr(self):
        HEADER = [  'song', 'person' , 'album', "decade", "genre",
                     "genre", "writer", "composer","song","bandmember", "secondary artist", "guest",
                    "Music Band", "album", "song", "Genre", "person",]

        GID_META_STATS = ['song', 'person' , 'album', "decade", "genre", ]


        ENTITY_STATS = [ "e_22", "e_23", "e_24", "e_19", "e_20", "e_28", "e_30"]

        GRAPH_NODES = ["g_musicband", "g_album",  "g_song", "g_genre", "g_person",]

        self.group_header = [ 'Gid Meta Stats', 'Graph Edges Stats', "Graph Nodes"]

        return [ HEADER, GID_META_STATS, ENTITY_STATS , GRAPH_NODES]

    def get_new_kramer_datagen_music_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_new_kramer_music_datagen_header_incr, [['kramerdb_datagen.py'], ['Kramer Database']] * 3)

    def get_kramer_datagen_header(self):
        HEADER = [  'Movie', 'TV Video', 'Sequel', 'TV Series', 'Episode' , 'Tournament' ,'Team', 'Channel', 'Chan Folds',
                    'Role', 'Genre', 'Subgenre' , 'Language', 'Decade', 'Concept Fold', 'Person',  'Phrase Fold', 'game',
                    'Song', 'Sportsfilter' , 'wikiconcept', 'Stadium', 'Award', 'Sportsgroup',
                    'Rt', 'Va','Ak', 'Sp', 'Ik','Ti', 'Cs', 'Aq' , \
                    'Crew', 'Important Crew', 'Director', 'ImportantDirector', 'Producer', 'Important Producer', 'Host', 'Important Host',
                    'writer', 'Important Writer', 'composer', 'Important, Composer', 'Genre', 'Language', 'Award',
                    'BandMember', 'Song Artist', 'Parent', 'Parent Tour', 'Participant', 'Partof',
                    'GameParticipant', 'Teamplayer', 'Tournament', 'Tournament Venue', 'sportsevent', 'Wiki Links', 'Fold']

        DATABASE_STATS =[ 'movie', 'tvvideo', 'sequelfolding', 'tvseries', 'episode', 'tournament', 'teamfolding', 'channel', 'foldedonchannelaffiliation',
                          'role' , 'genre' , 'subgenre', 'language', 'decade', 'conceptfolding',
                        'personalityfolding', 'phrasefolding' , 'game', 'song', 'sportsfilter' , 'wikiconcept', 'stadium', 'award', 'sportsgroup',]


        PHRASE_STATS = [ 'Rt', 'Va','Ak', 'Sp', 'Ik','Ti', 'Cs',  'Aq']

        ENTITY_STATS = ['crew', 'importantcrew', 'director', 'importantdirector', 'producer', 'importantproducer', 'host', 'importanthost',
                        'writer', 'importantwriter', 'composer', 'importantcomposer', 'genre', 'language','award',
                        'bandmember', 'songartist', 'parent', 'parenttour', 'participant', 'partof',
                        'gameparticipant', 'teamplayer', 'tournament', 'tournamentvenue', 'sportsevent','wikilinks', 'fold']

        self.group_header = [ 'DataBase Stats', 'Phrase Stats', 'Entity Stats']

        return [ HEADER, DATABASE_STATS, PHRASE_STATS, ENTITY_STATS ]

    def get_kramer_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_kramer_datagen_header, [['kramerdb_datagen.py'], ('nested', ['Kramer Database'], ['gid_meta'], ['type_gid_count']), ['kramerdb_datagen.py'], ('nested', ['Kramer Database'], ['phrase_association'], ['field_gid_count']), ['kramerdb_datagen.py'], ('nested', ['Kramer Database'], ['entity_association'], ['associationtype_gid_count'])])

    def get_phrase_server_header(self):
        HEADER = [ 'Movie', 'TV Video', 'Sequel', 'TV Series', 'Episode', 'Tournament', 'Team', 'Channel', 'Channel Fold', 'Role',
                   'Genre', 'Subgenre', 'Language', 'Decade', 'Concept Fold', 'Person Folding', 'Phrase Folding',
                   'WikiConcepts', 'Song', 'Award', 'Stadium',
                   'Age Filter', 'Avail Filter', 'Award Category', 'Filter', 'IPC', 'Rating Filter', 'Result Filter', 'Sort Filter', 'Star Filter',
                   'Meta Table', 'Gid Meta File', 'Phrase File',
                   'Normalized File', 'Gid Info File', 'Phrase Table', 'Good Keyword']

        PHRASE_STATS = ( 'movie', 'tvvideo', 'sequelfolding', 'tvseries',
             'episode', 'tournament', 'teamfolding', 'channel', 'foldedonchannelaffiliation', 'role',
             'genre', 'subgenre', 'language', 'decade', 'conceptfolding',
             'personalityfolding', 'phrasefolding',
             'wikiconcept', 'song', 'award', 'stadium',
             'age_filter', 'avail_filter', 'award_category', 'filter', 'ipc', 'rating_filter', 'result_filter', 'sort_filter', 'star_filter',
             'Meta Table', 'Gid Meta File', 'Phrase File', 'Normalized File',
             'Gid Info File', 'Phrase Table', 'Good Keyword')

        self.group_header = [ 'Phrase Server Stats' ]

        return [ HEADER, PHRASE_STATS ]

    def get_phrase_server_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_phrase_server_header, [['phrase_server_datagen.py'], ['phrase_server_datagen']])

    def get_graph_datagen_header(self):
        HEADER = [ 'Episode', 'Tournament', 'Song', 'Role', 'Tvseries', 'Movie', 'Person Folding', 'Producer', 'Director', 'Tvseries', 'Crew']

        NODE_STATS = [ 'episode', 'tournament', 'song', 'role', 'tvseries', 'movie', 'personalityfolding' ]

        EDGE_STATS = [ 'producer', 'director', 'tvseries', 'crew' ]

        self.group_header = [ 'Node Stats', 'Edge Stats' ]

        return [ HEADER, NODE_STATS, EDGE_STATS ]

    def get_graph_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_graph_datagen_header, [['graph_data_gen.py'], ('nested', ['GraphStats'], ['nodes']), ['graph_data_gen.py'], ('nested', ['GraphStats'], ['edges'])])

    def get_actionable_header(self):
        HEADER = [ 'Wiki', 'Website', 'Zhedb', 'Wiki Url', 'Spotify', 'Image', 'Netflix', 'Youtube', 'Imdb', 'Fandango', 'baike']

        ACTIONABLE_STATS = [ 'wiki', 'website', 'zhedb', 'wiki_url', 'spotify', 'image', 'netflix', 'youtube', 'imdb', 'fandango', 'baike' ]

        self.group_header = [ 'Actionable Stats' ]

        return [ HEADER, ACTIONABLE_STATS ]

    def get_actionable_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_actionable_header, [['actionable_datagen_redis.py#generateSmartSearchActionable.py'], ['actionable stats']])

    def get_mycloud_header(self):
        HEADER = ['facebook_crawled', 'twitter_crawled', 'youtube_crawled', 'facebook_folded', 'twitter_folded', 'youtube_folded', 'facebook_notified', 'twitter_notified', 'youtube_notified']
        MYCLOUD_STATS = HEADER
        self.group_header = [ 'Mycloud Stats' ]
        return [ HEADER, MYCLOUD_STATS ]

    def get_mycloud_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_mycloud_header, [['fold_utils.py'], ['mycloud stats']])

    def get_clu_diff_stats_header(self):
        HEADER = ['Source count','Add count','Delete count','Mod count','Total','P0','P1','P2','P3','P4','P5','P6','Total','P0','P1','P2','P3','P4','P5','P6','Total']
        CLU_STATS = ['Source count','Add count','Delete count','Mod count']
        COSMO_MERGE_COUNT = ['Total']
        PASSED_SERVICE_ID_COUNT = ['P0','P1','P2','P3','P4','P5','P6','Total']
        FAILED_SERVICE_ID_COUNT = ['P0','P1','P2','P3','P4','P5','P6','Total']
        self.group_header = [ 'Record Count', 'Cosmo Merge Count','Passed Service id count','Failed Service id count']
        return [HEADER, CLU_STATS, COSMO_MERGE_COUNT, PASSED_SERVICE_ID_COUNT, FAILED_SERVICE_ID_COUNT]

    def get_clu_diff_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_clu_diff_stats_header, [['clu_diff.py'], ['CLU Stats'],['clu_diff.py'], ['Total Cosmo Source Id Count'], ['clu_diff.py'],('union', ['Passed Service id count'], ['Total']),['clu_diff.py'], ('union',['Failed Service id count'],['Total'])])


    def get_clu_extract_stats_header(self):
        HEADER = ['Prism', 'Cosmo', 'Web', 'Ftp', 'Broadcast','Verizon','Charter','Bell','Outdated Service Ids']
        CLU_EXTRACT = ['Prism', 'Cosmo', 'Web', 'Ftp', 'Broadcast','Verizon','Charter','Bell', 'Outdated Service Ids']
        self.group_header = ['CLU Extract']
        return [HEADER, CLU_EXTRACT]

    def get_clu_extract_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_clu_extract_stats_header, [['clu_extract_wrapper.py'],['CLU Extract']])

    def get_verizon_lineup_stats_header(self):
        HEADER = ['Total Count','Passed Serviceid count','Failed Serviceid count']
        CLU_STATS = ['Total Count','Passed Serviceid count','Failed Serviceid count']
        self.group_header = [ 'Record Count']
        return [HEADER, CLU_STATS]

    def get_verizon_lineup_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_verizon_lineup_stats_header, [['verizon_source_parser.py'], ['Verizon Lineup Stats']])

    def get_outbound_stats_header(self):
        HEADER = ['Movies', 'TV Series', 'Episodes', 'Personalities']
        OUTBOUND_STATS = ['Movie', 'TvShow', 'Episode', 'Crew']
        self.group_header = ['Record Count']
        return [HEADER, OUTBOUND_STATS]

    def get_outbound_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_outbound_stats_header, [['outbound_datagen.py'], ['outbound data stats']])

    def get_epg_stats_header(self):
        HEADER = ['Burrp', 'Astro', 'Singteltv', 'Starhub']
        EPG_STATS = ['burrp', 'astro', 'singteltv', 'starhub']
        self.group_header = ['Channel Count']
        return [HEADER, EPG_STATS]

    def get_epg_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_epg_stats_header, [['epg_datagen.py'], ['epg stats']])

    def get_tivo_epg_stats_header(self):
        self.group_header = ['Twcc', 'Aoltv']
        field_list = ['Crawled', 'Delivered']
        HEADER = field_list * len(self.group_header)
        EACH_SOURCE_STATS = [['Crawled', 'Delivered']]*len(self.group_header)
        return [ HEADER ] + EACH_SOURCE_STATS

    def get_tivo_epg_stats(self, row_list, obj):
        STATS_LIST = []
        for source in self.group_header:
            STATS_LIST.append(['tivo_epg_datagen.py'])
            STATS_LIST.append(('nested', ['%s tivo epg stats' % source.lower()], [source.lower()]))
        self.get_stats_from_file(row_list, obj, self.get_tivo_epg_stats_header, STATS_LIST)

    def get_operator_header(self):
        HEADER = ['setup', 'fetch_data', 'ltv_parser', 'ltv_view_parser', 'ltv_view_analytics']
        OPERATOR_STATS = ['setup', 'fetch_data', 'ltv_parser', 'ltv_view_parser', 'ltv_view_analytics']
        self.group_header = [ 'OPERATOR STATS' ]
        return [ HEADER, OPERATOR_STATS]

    def get_operator_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_operator_header, [['wrapper_cronjob.py'], ['operator stats']])

    def get_app_header(self):
        HEADER = [ 'Apps Category', 'Invalid Apps', 'No of Apps', 'Null Title', 'Null Category', 'Valid Apps' ]

        APP_STATS = [ 'no_of_apps_category', 'no_of_invalid_apps', 'no_of_apps', 'no_of_apps_with_null_title', 'no_of_apps_with_null_category', 'no_of_valid_apps' ]

        self.group_header = [ 'App Stats' ]

        return [ HEADER, APP_STATS ]

    def get_amazon_app_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_app_header, [['amazonPublish.py'], ['Amazon Apps Categories Details']])

    def get_android_app_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_app_header, [['androPublish.py'], ['Google Play Apps Categories Details']])

    def get_ovisearch_app_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_app_header, [['oviPublish.py'], ['Nokia Apps Categories Details']])

    def get_android_folding_header(self):
        HEADER = [ 'APPS FOLDED', 'KWDS FREQUENCY MODIFIED' ]
        FOLD_STATS = [ 'went_inside_fold', 'frequency_changed' ]
        self.group_header = [ 'Android Folding Stats' ]
        return [ HEADER, FOLD_STATS ]

    def get_android_folding_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_android_folding_header, [['appstore_folding.py'], ['Few Stats']])

    def get_kramer_wikisubset_header(self):
        HEADER = [ 'Gids Dropped', 'Gids in subset', 'Bad Types', 'High Frequency Phrases Matched', 'High Frequency Phrases', 'Bad Type Gids' ]

        SUBSET_STATS = [ 'Total number of gids dropped', 'Total number of gids in subset', 'Number of Bad types', 'Number of High Frequency phrases matched', 'Number of High Frequency phrases', 'Number of Bad type gids' ]

        self.group_header = [ 'Wiki Stats' ]

        return [ HEADER, SUBSET_STATS ]

    def get_freebase_datagen_header(self):
        HEADER = [ 'MID', 'FRB Film', 'FRB TV', 'FRB Episode', 'Title', 'Ig', 'Author', 'Adaptation', 'Program adaptation',
                   'Aka', 'Keyword', 'Genre' , 'Program Rd', 'Writer', 'TV series country',
                   'TV network', 'Cast', 'Personal appearance', 'Gids from merge', 'Gids from freebase',
                   'Movies', 'TV Series', 'Episode', 'Metacritic', 'Hulu', 'Netflix', 'IMDB ttid', 'TVDB',
                   'TV Rage', 'TV Guide', 'Episode Ods', 'TV Series Ods', 'Movie Ods',
                   'Non-english wiki gids', 'Tv Country', 'Movie Country',
                   'PROGRAM Total Overrides', 'PROGRAM Overridden Count',
                   'MID', 'FRB Person', 'Title', 'Persons', 'Parents', 'Sibling', 'Children', 'Spouse',
                   'Person Rd', 'Birth date', 'Occupation', 'Birth place', 'Aka', 'Star type', 'Notable type',
                   'Gender', 'Gids from freebase', 'Gids from merge', 'IMDB nmid',
                   'Social Media Presence', 'Facebook', 'Twitter', 'Last.fm', 'LinkedIn', 'Non-english wiki gids',
                   'PERSON Total Overrides', 'PERSON Overridden Count', 'FRB Band',
                   'Band Members', 'Member Roles', 'Start date', 'End date', 'Non-english wiki gids']

        PROGRAM_SUBSET_STATS = [ 'Program mid', 'FRB Film gids', 'FRB TV gids', 'FRB Episode gids',
                                 'Program name', 'Ig', 'Author', 'Adaptation', 'Program adaptation',
                                 'Program Aka', 'Keyword',
                                 'Genre', 'Program Rd', 'Writer', 'TV series country', 'TV network',
                                 'Cast', 'Personal appearance', 'Merged program gids', 'Freebase program gids',
                                 'Movie', 'Tv show', 'Episode',
                                 'Metacritic', 'Hulu', 'Netflix', 'IMDB ttid', 'TVDB', 'TV Rage', 'TV Guide',
                                 'Episode airdate', 'Tvseries airdate', 'Movie airdate',
                                 'Programs non-english wiki gids', 'TV series country', 'Movie country',
                                 'PROGRAM Total Overrides', 'PROGRAM Overridden Count' ]
        PERSON_SUBSET_STATS = [ 'Person mid', 'FRB Person gids', 'Person name', 'Person',
                                'Parents', 'Sibling', 'Children',
                                'Spouse', 'Person Rd', 'Birth date', 'Occupation', 'Birth place', 'Person Aka',
                                'Star type', 'Notable type', 'Gender', 'Freebase person gids', 'Merged person gids',
                                'IMDB nmid', 'Number of Social Media Presence Urls', 'facebook', 'twitter',
                                'lastfm', 'linkedin', 'Persons non-english wiki gids',
                                'PERSON Total Overrides', 'PERSON Overridden Count']

        MUSIC_GROUP_SUBSET_STATS = [ 'FRB Music Band',
                                     'Number of Band Members', 'Number of Band member Roles',
                                     'Number of band active start date', 'Number of band active end date',
                                     'Music band non-english wiki gids']
        self.group_header = [ "Movie/TV Series/Episode stats", "Personality stats" , 'Music Band stats']

        return [ HEADER, PROGRAM_SUBSET_STATS, PERSON_SUBSET_STATS, MUSIC_GROUP_SUBSET_STATS ]

    def get_freebase_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_freebase_datagen_header, [['freebaseDatagen.py'], ['Freebase datagen']] * 3)

    def get_freebase_awards_datagen_header(self):
        HEADER = [ 'Award mid', 'Category', 'Award year',
                   'Ceremony', 'Work', 'Winner', 'Nominee',
                   'Number of award winner instances', 'Number of award nominee instances', 'Total Overrides',
                   'Overridden Count', 'Award Unique year', 'Award Unique mid']

        SUBSET_STATS = [ 'Award mid', 'Category', 'Award year',
                         'Ceremony', 'Work', 'Winner', 'Nominee',
                         'Number of winner instances', 'Number of nominee instances',
                         'Total Overrides', 'Overridden Count','Award Unique year', 'Award Unique mid'  ]

        self.group_header = [ 'Freebase awards datagen' ]

        return [ HEADER, SUBSET_STATS ]

    def get_freebase_awards_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_freebase_awards_datagen_header, [['freebaseAwardDatagen.py'], ['Freebase awards datagen']])

    def get_freebase_role_datagen_header(self):
        HEADER = [ 'Number of film role instances', 'Number of role occupation instances',
                    'Role mid', 'Wiki gid', 'Aka', 'Cast film pair',
                   'Special performance', 'Notes', 'Role name', 'Role description',
                   'Non-english wiki gids','Total Overrides',
                   'Overridden Count' ]

        SUBSET_STATS = [ 'Number of film role instances', 'Number of role occupation instances', 'Role mid',
                         'Wiki gid', 'Aka', 'Cast film pair', 'Special performance',
                         'Notes', 'Role name', 'Role description', 'Roles non-english wiki gids',
                         'Total Overrides', 'Overridden Count' ]


        self.group_header = [ 'Freebase role datagen' ]

        return [ HEADER, SUBSET_STATS ]

    def get_freebase_role_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_freebase_role_datagen_header, [['freebaseRoleDatagen.py'], ['Freebase role datagen']])

    def get_freebase_website_datagen_header(self):
        HEADER = [ 'Website mid', 'Website gid', 'Title', 'Website Genres', 'Website Pd',
                   'Unique Genres', 'Unique Pd', 'URL', 'Website status' ]
        SUBSET_STATS = [ 'Website mid', 'Wiki gid', 'Title', 'Website Genre', 'Website Pd',
                         'Unique Genre', 'Unique Pd', 'URL', 'Website status' ]

        self.group_header = [ 'Freebase website datagen' ]

        return [ HEADER, SUBSET_STATS ]

    def get_freebase_website_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_freebase_website_datagen_header, [['freebaseWebsiteDatagen.py'], ['Freebase website datagen']])

    def get_freebase_description_datagen_header(self):
        HEADER = [ 'Description' ]
        SUBSET_STATS = [ 'Number of description instances' ]

        self.group_header = [ 'Freebase description datagen' ]

        return [ HEADER, SUBSET_STATS ]

    def get_freebase_description_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_freebase_description_datagen_header, [['freebaseDescriptionDatagen.py'], ['Freebase description datagen']])

    def get_freebase_misc_datagen_header(self):
        HEADER = [ 'Fold' , 'person', 'Phrase', 'Award', 'Wikiconcept' ]
        SUBSET_STATS = [ 'Number of records', 'PersonalityFolding', 'PhraseFolding', 'award', 'wikiconcept' ]

        self.group_header = [ 'Freebase fold datagen' ]

        return [ HEADER, SUBSET_STATS ]

    def get_freebase_misc_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_freebase_misc_datagen_header, [['freebaseMiscDatagen.py'], ['Freebase misc datagen']])

    def get_freebase_sequel_datagen_header(self):
        HEADER = [ 'Ordered Series' , 'Unordered Series']
        SUBSET_STATS = [ 'Number of ordered film series', 'Number of unordered film series' ]

        self.group_header = [ 'Freebase sequel datagen' ]

        return [ HEADER, SUBSET_STATS ]

    def get_freebase_sequel_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_freebase_sequel_datagen_header, [['freebaseSequelDatagen.py'], ['Freebase sequel datagen']])

    def get_freebase_direct_merge_header(self):
        HEADER = [ 'Progam wiki merge' , 'Person wiki merge', 'Role wiki merge', 'Fold wiki merge',
                    'Episode', 'Movie', 'Tv seies', 'Awards', 'PhraseFolding', 'wiki concept',
                    'Merged Sequels', 'Unmerged', 'Unmerged sequels', 'Unmerged roles']
        SUBSET_STATS = [ 'Number of program wiki gids merges', 'Number of person wiki gids merges',
                         'Number of role wiki gids merges', 'Number of fold wiki gids merges',
                          'episode', 'movie', 'tvseries', 'award', 'PhraseFolding',  'wikiconcept',
                          'Number of sequel wiki gids merges', 'Number of unmerged records',
                          'Number of unmerged sequel records', 'Number of unmerged role records' ]

        self.group_header = [ 'Freebase merge' ]

        return [ HEADER, SUBSET_STATS ]

    def get_freebase_direct_merge_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_freebase_direct_merge_header, [['freebaseMerge.py'], ['Freebase merge']])

    def get_role_datagen_header(self):
        HEADER = [ 'Total Roles', 'Wiki Roles', 'Freebase Roles', 'ROLE Roles', 'Empty Pa']

        SUBSET_STATS = [ 'Total number of roles', 'Number of WIKI Roles', 'Number of FRB Roles', 'Number of ROLE Roles', 'Empty Pa count']

        self.group_header = [ 'Role datagen' ]

        return [ HEADER, SUBSET_STATS ]

    def get_role_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_role_datagen_header, [['RoleDatagen.py'], ['Role datagen']])

    def get_freebase_fetch_header(self):
        HEADER = [ 'Downloaded new RDF', 'Films', 'TV programs', 'Episodes', 'Wiki gids', 'Persons', 'Music groups',
                   'Film roles', 'TV series roles', 'Role occupation']

        SUBSET_STATS = [ 'Downloaded freebase', 'Number of film records extracted',
                         'Number of tv programs extracted', 'Number of episode records extracted',
                         'Number of wiki gids extracted',
                         'Number of persons extracted', 'Number of music groups extracted',
                         'Number of film records with roles', 'Number of roles in tv series records',
                         'Number of role occupation records']

        self.group_header = [ 'Freebase datagen' ]

        return [ HEADER, SUBSET_STATS ]

    def get_freebase_fetch_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_freebase_fetch_header, [['freebaseFetch.py'], ['Freebase extraction']])

    def get_cdk_fetch_header(self):
        HEADER = ['Current Vod data for tvpublish', 'Current Vod data in xlsx', 'Current vod rental data for CF']
        cdk_fetch_stats = ['rovi_offerings_twe_prod_current.zip', 'rovi_offerings_twe_prod_current.xlsx', 'rovi_vodrental_twe_prod_current.zip']
        self.group_header = ['CDK Fetch Stats']
        return [ HEADER, cdk_fetch_stats ]

    def get_cdk_fetch_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cdk_fetch_header, [['vtv_fetcher.py'], ['cdk_fetch_datagen']])

    def get_awards_datagen_header(self):
        HEADER = [ 'Ceremony','IMDB Award records', 'Work Gids', 'Total Winner Gids', 'category gids',    \
                  'Total nominees', 'Winner Records', 'Nominee Records', 'Freebase Award records',   \
                  'Normalized categories', 'Unique ceremonies', 'Unique Imdb awards year',   \
                  'Unique Freebase awards year', 'Unique work', 'Unique winner gids',    \
                  'Unique category gids', 'Unique Nominee gids','Unique Normalized category',  \
                  'Records with type award', 'Records with type prize','person', 'song', 'tvseries',   \
                  'album', 'movie','Mapped records', 'Dropped duplicate imdb records', 'Dropped award records']

        TOTAL_STATS = [  'Ceremony','IMDB Award records', 'Work Gids', 'Total Winner Gids', 'category gids',    \
                  'Total nominees', 'Winner Records', 'Nominee Records', 'Freebase Award records', 'Normalized categories']

        UNIQ_STATS = ['Unique ceremonies', 'Unique Imdb awards year',   \
                  'Unique Freebase awards year', 'Unique work', 'Unique winner gids',    \
                  'Unique category gids', 'Unique Nominee gids','Unique Normalized category']

        TYPE_RECORD_STATS = ['Records with type award', 'Records with type prize']

        XT_STATS = ['person', 'song', 'tvseries','album', 'movie']

        MISC_STATS = ['Mapped records', 'Dropped duplicate imdb records', 'Dropped award records']

        self.group_header = [ 'Field wise stats', 'Unique Records', 'Type Wise Records', 'Award Extended Type', 'Misc Stats' ]

        return [ HEADER, TOTAL_STATS, UNIQ_STATS, TYPE_RECORD_STATS, XT_STATS, MISC_STATS ]

    def get_awards_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_awards_datagen_header, [['awards_datagen.py'], ['Populate freebase awards db']] * 5)

    def get_seed_lang_header(self):

        HEADER = ['NOR', 'FRA']
        COUNT_STATS = ['NOR', 'FRA']

        self.group_header = [ 'Count Stats' ]

        return [ HEADER, COUNT_STATS ]


    def get_seed_lang_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_seed_lang_header, [['gen_lang_seed.py'], ['SEED LANG STATS']])

    def get_kramer_news_trends_datagen_header(self):
        HEADER = [ 'Number of trends' , 'Max Number of trends', 'Min Number of trends', 'Avg Number of trends' ]

        SUBSET_STATS = [ 'Number of trends' ]
        AGGREGATE_SUBSET_STATS = [ 'Max Number of trends', 'Min Number of trends', 'Avg Number of trends' ]
        SUBSET_STATS.extend(AGGREGATE_SUBSET_STATS)
        self.group_header = [ 'News trends datagen' ]

        return [ HEADER, SUBSET_STATS ]

    def get_kramer_news_trends_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_kramer_news_trends_datagen_header, [['genNewsTrends.py'], ['Topics in News']])

    def get_awards_static_datagen_header(self):
        HEADER = [ "Normalized Awards", ]

        COUNT_STATS = ["Normalized Awards",]

        self.group_header = [ 'COUNT STATS' ]

        return [ HEADER, COUNT_STATS ]

    def get_awards_static_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_awards_static_datagen_header, [['awards_static_datagen.py'], ['Main Stats']])

    def get_trending_news_header(self):
        HEADER = [ 'Trending News Topics', 'Current Event Topics', 'Clips Tags' , 'Clips Tag Pairs', 'Wiki event pairs',
                    'Google news pairs', 'webclips/wiki pairs', 'webclips/google news pairs', 'google_news/wiki pairs',
                    'Common clips/wiki gids', 'Common clips/google_news gid-kwd pairs', 'webclips gid-kwd pairs',
                    'google_news gid-kwd pairs', 'Youtube', 'Youtube Missed', 'Google', 'Google Missed', 'Twitter',
                    'Twitter Missed', 'Total Count', 'Total Missed', 'Profile Db']

        NEWS_STATS = [ 'Total number of trending news topics', 'Number of trending current event topics',
                       'Number of trending clips tags' , 'Number of trending smart tag pairs',
                       'Number of trending current event pair topics', 'Number of trending news trends pairs',
                       'Common clip-wiki gid pairs', 'Common clips-google_news gid pairs',
                       'Common google_news-wiki gid pairs', 'Common gids', 'Common clips-google_news gid-kwd pair',
                       'webclips gid-kwd pair', 'google_news gid-kwd pair', 'youtube', 'missed_youtube', 'google',
                       'missed_google', 'twitter', 'missed_twitter', 'Topic Count', 'Missed Count', 'Profile Db Count']

        self.group_header = [ 'News Stats' ]

        return [ HEADER, NEWS_STATS ]

    def get_trending_news_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_trending_news_header, [['genNewsTopics.py'], ['Topics in News']])

    def get_wiki_archival_update_stats_header(self):
        HEADER = [ 'Rl Applied', 'Rl matches', 'Vt Applied', 'Vt matches', 'Total Applied', 'Total Matched', 'movie', 'tv show', 'tv episode', 'person', 'media franchise',
                   'Gi', 'Ti', 'Pr', 'Ps', 'Pp', 'Cl', 'Rr', 'Oc', 'Ll', 'Od', 'Sy', 'Ry', 'Pc', 'Sn', 'Np', 'Bd', 'Va',
                   'Gu', 'En', 'Co', 'Di', 'Cc', 'Dd', 'De', 'Ge', 'Iv', 'Ak', 'Ik', 'Bp', 'Ho', 'Vt', 'Er', 'Ca', 'Wr']

        TYPE_STATS = ['movie', 'tv show', 'tv episode', 'person', 'media franchise']
        FIELD_STATS = ['Gi', 'Ti', 'Pr', 'Ps', 'Pp', 'Cl', 'Rr', 'Oc', 'Ll', 'Od', 'Sy', 'Ry', 'Pc', 'Sn', 'Np', 'Bd', 'Va',
                         'Gu', 'En', 'Co', 'Di', 'Cc', 'Dd', 'De', 'Ge', 'Iv', 'Ak', 'Ik', 'Bp', 'Ho', 'Vt', 'Er', 'Ca', 'Wr']
        COPIER_STATS = [ 'Rl-Overridden Count', 'Rl-Override Matches', 'Vt-Overridden Count', 'Vt-Override Matches', 'Overridden Count', 'Override Matches']

        self.group_header = [ 'Override Stats' , "Type Stats", "Field Stats", ]

        return [ HEADER, COPIER_STATS, TYPE_STATS, FIELD_STATS ]

    def get_wiki_archival_update_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_wiki_archival_update_stats_header, [['wiki_overrides.py'], ['override_stats'],
                                                                ['archival_update.py'], ['type_stats'], ['archival_update.py'], ['field_stats']])



    def get_wiki_incr_header(self):
        HEADER = [ 'Total MRF Gids', 'Change MRF Gids', 'New MRF Gids',
                    'iredRR.txt', 'template.db', 'glossary.db', 'relevance_ratio.txt',
                    'weighted_aka.txt', 'text_mc.txt', 'wiki_info.txt', 'wiki_kwds.txt', 'mrf.txt',
                    'wiki_rich_info.txt', 'wiki_links_gid.txt', 'wiki_ugenre_scale.txt', 'wiki_tags.txt',
                    'global_kwd_freq.txt', 'wiki_plots.txt', 'kwdPop.txt', 'person_info.txt',
                    'DATA_WIKIPEDIA_GUIDMAP_FILE', 'DATA_WIKIPEDIA_MC_FILE_kw', 'wiki_links_with_gids.txt',
                    'DATA_WIKIPEDIA_FRANCHISE_FILE', 'DATA_WIKIPEDIA_BLURB_FILE', 'DATA_WIKIPEDIA_POPULARITY_FILE_inlink',
                    'DATA_WIKIPEDIA_IMDB_INFOBOX_FILE', 'DATA_WIKTIONARY_ALTERNATE_SPELL_FILE', 'DATA_WIKIPEDIA_CATEGORY_FILE',
                    'DATA_WIKIPEDIA_SEQUEL_FILE', 'DATA_WIKIPEDIA_GUID2TITLE_FILE', 'DATA_WIKIPEDIA_IMAGE_FILE',
                    'DATA_WIKTIONARY_POPULARITY_FILE', 'DATA_WIKIPEDIA_AUX_FILE', 'DATA_WIKIPEDIA_MC_FILE',
                    'DATA_STEM_HASH_search', 'DATA_STEM_HASH_noun_only',
                    'DB Inserts', 'Failed Updates']


        COUNT_STATS = ['Total Mrf Gids', 'Changed Mrf Gids', 'New Mrf Gids', 'iredRR.txt', 'template.db', 'glossary.db', 'relevance_ratio.txt',
                    'weighted_aka.txt', 'text_mc.txt', 'wiki_info.txt', 'wiki_kwds.txt', 'mrf.txt',
                    'wiki_rich_info.txt', 'wiki_links_gid.txt', 'wiki_ugenre_scale.txt', 'wiki_tags.txt',
                    'global_kwd_freq.txt', 'wiki_plots.txt', 'kwdPop.txt', 'person_info.txt',
                    'DATA_WIKIPEDIA_GUIDMAP_FILE', 'DATA_WIKIPEDIA_MC_FILE_kw', 'wiki_links_with_gids.txt',
                    'DATA_WIKIPEDIA_FRANCHISE_FILE', 'DATA_WIKIPEDIA_BLURB_FILE', 'DATA_WIKIPEDIA_POPULARITY_FILE_inlink',
                    'DATA_WIKIPEDIA_IMDB_INFOBOX_FILE', 'DATA_WIKTIONARY_ALTERNATE_SPELL_FILE', 'DATA_WIKIPEDIA_CATEGORY_FILE',
                    'DATA_WIKIPEDIA_SEQUEL_FILE', 'DATA_WIKIPEDIA_GUID2TITLE_FILE', 'DATA_WIKIPEDIA_IMAGE_FILE',
                    'DATA_WIKTIONARY_POPULARITY_FILE', 'DATA_WIKIPEDIA_AUX_FILE', 'DATA_WIKIPEDIA_MC_FILE',
                    'DATA_STEM_HASH_search', 'DATA_STEM_HASH_noun_only',
                    'DB Inserts', 'Failed Updates']

        self.group_header = [ 'COUNT STATS' ,]

        return [ HEADER, COUNT_STATS ]

    def get_wiki_incr_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_wiki_incr_header, [['wikiDataGen.py'], ['Count Stats']])

    def wiki_archival_header(self):
        HEADER = [ 'Total MRF Gids', 'Change MRF Gids', 'New MRF Gids',
                    'iredRR.txt', 'template.db', 'glossary.db', 'relevance_ratio.txt',
                    'weighted_aka.txt', 'text_mc.txt', 'wiki_info.txt', 'wiki_kwds.txt', 'mrf.txt',
                    'wiki_rich_info.txt', 'wiki_links_gid.txt', 'wiki_ugenre_scale.txt', 'wiki_tags.txt',
                    'global_kwd_freq.txt', 'wiki_plots.txt', 'kwdPop.txt', 'person_info.txt', 'wiki_cast.txt', 'wiki_episodes.txt',
                    'DATA_WIKIPEDIA_GUIDMAP_FILE', 'DATA_WIKIPEDIA_MC_FILE_kw', 'wiki_links_with_gids.txt',
                    'DATA_WIKIPEDIA_FRANCHISE_FILE', 'DATA_WIKIPEDIA_BLURB_FILE', 'DATA_WIKIPEDIA_POPULARITY_FILE_inlink',
                    'DATA_WIKIPEDIA_IMDB_INFOBOX_FILE', 'DATA_WIKTIONARY_ALTERNATE_SPELL_FILE', 'DATA_WIKIPEDIA_CATEGORY_FILE',
                    'DATA_WIKIPEDIA_SEQUEL_FILE', 'DATA_WIKIPEDIA_GUID2TITLE_FILE', 'DATA_WIKIPEDIA_IMAGE_FILE',
                    'DATA_WIKTIONARY_POPULARITY_FILE', 'DATA_WIKIPEDIA_AUX_FILE', 'DATA_WIKIPEDIA_MC_FILE',
                    'DATA_STEM_HASH_search', 'DATA_STEM_HASH_noun_only',
                    'producer', 'Director', 'Host', 'writer', 'release year', 'language', 'cast']


        COUNT_STATS = ['Total Mrf Gids', 'Changed Mrf Gids', 'New Mrf Gids', 'iredRR.txt', 'template.db', 'glossary.db', 'relevance_ratio.txt',
                    'weighted_aka.txt', 'text_mc.txt', 'wiki_info.txt', 'wiki_kwds.txt', 'mrf.txt',
                    'wiki_rich_info.txt', 'wiki_links_gid.txt', 'wiki_ugenre_scale.txt', 'wiki_tags.txt',
                    'global_kwd_freq.txt', 'wiki_plots.txt', 'kwdPop.txt', 'person_info.txt', 'wiki_cast.txt', 'wiki_episodes.txt',
                    'DATA_WIKIPEDIA_GUIDMAP_FILE', 'DATA_WIKIPEDIA_MC_FILE_kw', 'wiki_links_with_gids.txt',
                    'DATA_WIKIPEDIA_FRANCHISE_FILE', 'DATA_WIKIPEDIA_BLURB_FILE', 'DATA_WIKIPEDIA_POPULARITY_FILE_inlink',
                    'DATA_WIKIPEDIA_IMDB_INFOBOX_FILE', 'DATA_WIKTIONARY_ALTERNATE_SPELL_FILE', 'DATA_WIKIPEDIA_CATEGORY_FILE',
                    'DATA_WIKIPEDIA_SEQUEL_FILE', 'DATA_WIKIPEDIA_GUID2TITLE_FILE', 'DATA_WIKIPEDIA_IMAGE_FILE',
                    'DATA_WIKTIONARY_POPULARITY_FILE', 'DATA_WIKIPEDIA_AUX_FILE', 'DATA_WIKIPEDIA_MC_FILE',
                    'DATA_STEM_HASH_search', 'DATA_STEM_HASH_noun_only']

        ENTERTAINMENT_STATS = ['Pr', 'Di', 'Ho', 'Wr', 'Ry', 'Ll', 'Ca']
        self.group_header = [ 'COUNT STATS' , 'Entertainment stats']

        return [ HEADER,  COUNT_STATS, ENTERTAINMENT_STATS]

    def get_wiki_archival_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.wiki_archival_header, [['archival_stats.py'], ['Count Stats'], ['archival_stats.py'], ['Entertainment Stats']])

    def get_wiki_knownfor_header(self):
        HEADER = [ 'Persons Tagged', ]

        KNOWN_STATS = [ 'Persons Tagged', ]

        self.group_header = [ 'Wiki Best Known  Stats' ]

        return [ HEADER, KNOWN_STATS ]

    def get_wiki_knownfor_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_wiki_knownfor_header, [['extract_knownfor.py'], ['Main Stat']])

    def get_wiki_genre_header(self):
        HEADER = [ '1st line genre', '2nd line genre', 'category genre', 'infobox genre', ]

        GENRE_STATS = [ 'FL Genre', 'SL Genre', 'Category Genre', 'Infobox Genre', ]

        self.group_header = [ 'Wiki Genre Stats' ]

        return [ HEADER, GENRE_STATS ]

    def get_wiki_genre_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_wiki_genre_header, [['genre_tagging.py'], ['Main stats']])

    def get_trending_topic_header(self):
        HEADER = ['youtube', 'Youtube Missed', 'Google', 'Google Missed', 'Twitter', 'Twitter Missed', 'Total Count', 'Total Missed', 'Profile Db']

        COUNT_STATS = ['youtube', 'missed_youtube', 'google', 'missed_google', 'twitter', 'missed_twitter', 'Topic Count', 'Missed Count', 'Profile Db Count']

        self.group_header = [ 'Trending Topics' ]

        return [ HEADER, COUNT_STATS ]

    def get_trending_topic_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_trending_topic_header, [['gen_trends_based_pop.py'], ['Trending Topics']])

    def get_popular_culture_status_header(self):
        HEADER = ['Record Count', 'Ig Count']

        POP_CULT_STATS = ['Record count', 'Ig count']

        self.group_header = [ 'Popular Culture Stats' ]

        return [ HEADER, POP_CULT_STATS]

    def get_popular_culture_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_popular_culture_status_header, [['popular_culture.py'], ['POP CULT STATS']])

    def get_sports_keywords_header(self):
        HEADER = ['Kh Count', 'Ke Count', 'Sports Count', 'Max kwds', 'Min kwds', 'Avg kwds']

        SP_KWDS_STATS = ['Kh Count', 'Ke Count', 'Sports Count', 'Max kwds', 'Min kwds', 'Avg kwds']

        self.group_header = [ 'Sports Keywords Stats' ]

        return [ HEADER, SP_KWDS_STATS]

    def get_sports_keywords_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_sports_keywords_header, [['extract_sports_keywords.py'], ['Sp Stats']])

    def get_wiki_pageview_stats_header(self):
        HEADER = ['0-100', '100-200', '200-300', '300-400', '400-500', '500-600']

        POP_BUCKET_STATS = [i for i in range(6)]

        self.group_header = ['Pop Buckets']

        return [ HEADER, POP_BUCKET_STATS ]

    def get_wiki_pageview_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_wiki_pageview_stats_header, [['WikiPageViewDatagen.py'], ['Pop Stats']])

    def get_domains_header(self):
        HEADER = ['movie', 'music', 'tv', 'sports', 'Total', 'Matches']

        DOMAIN_STATS = ['movie', 'music', 'tv', 'sports']
        OVERRIDE_STATS = ['Total Overrides', 'Override Matches']

        self.group_header = [ 'Domains Stats', 'Override Stats' ]

        return [ HEADER, DOMAIN_STATS, OVERRIDE_STATS ]

    def get_domain_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_domains_header, [['infer_domain.py'], ['Domain Stats']] * 2)

    def get_wiki_episode_stats_header(self):
        HEADER = ['episodes_list_count', 'unique_tvseries'] + WIKI_EPISODE_FILEDS
        self.group_header = ['Episode Stats']
        return [HEADER, HEADER]

    def get_wiki_episode_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_wiki_episode_stats_header, [['wiki_episode_stats.py'], ['Episode Stats']])

    def get_kramer_static_header(self):
        HEADER = [ "ipc table", "filter_meta_table", "filter info table", "filter phrase table", "blacklist table", "static phrase table"]

        COUNT_STATS = ["ipc_table", "filter_meta_table", "filter_info_table", "filter_phrase_table","blacklist_table", "static_phrase_table"]

        self.group_header = [ 'COUNT STATS' ]

        return [ HEADER, COUNT_STATS ]

    def get_kramer_static_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_kramer_static_header, [['kramer_static_datagen.py'], ['Main Stats']])

    def get_knowledge_graph_header(self):
        PROGRAM_MERGE_STATS = ['movie','tvseries','episode']
        FOLD_MERGE_STATS = ['Person', 'Genre', 'Team', 'Channel']
        TYPE_STATS = ['episode','tvvideo','movie','MusicArtist','Person','game','tvseries','Team','Genre','SubGenre','classicgame']
        ENTITY_STATS = ['EpisodeSeries','ProgramCrew','SmartTag','Programs','Popularity','ProgramSequel','Person','Channels','Player','VariantTitles','GidMap','ProgramSmartTag','Team','Keywords','Aka','ImportantKeywords','VeveoGidMap','ProgramGenres']

        self.group_header = ['Program Merge Stats', 'Fold Merge Stats', 'Type Count Stats', 'Entity Stats']

        HEADER = PROGRAM_MERGE_STATS + FOLD_MERGE_STATS + TYPE_STATS + ENTITY_STATS

        return [ HEADER, PROGRAM_MERGE_STATS, FOLD_MERGE_STATS, TYPE_STATS, ENTITY_STATS]

    def get_knowledge_graph_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_knowledge_graph_header, [['knowledge_graph_datagen.py'], ('union', ['GMRF Stats'], ['percentage']), ['knowledge_graph_datagen.py'], ('union', ['Merge Stats'], ['Percentage Merge']), ['knowledge_graph_datagen.py'], ['Type Count Stats'], ['knowledge_graph_datagen.py'], ['Entity Stats']])

    def get_directv_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_knowledge_graph_header, [['directv_datagen.py'], ('union', ['New Merge Stats'], ['Merge Percentage']), ['directv_datagen.py'], ('union', ['Merge Stats'], ['Percentage Merge']), ['directv_datagen.py'], ['Type Count Stats'], ['directv_datagen.py'], ['Entity Stats']])

    def get_rovi_music_header(self):
        HEADER = [ 'Total Overrides', 'Overriden Count', 'Genres', 'Parent genres', 'Decades',
                   'Similar records', 'Media Unbound Recommendations',
                   'Influenced', 'Influenced by', 'Merged Genres',
                   'Artists', 'Bands', 'Aka', 'Gender', 'DoB', 'DoD', 'Birth Place', 'Band Members', 'Imp Artist',
                   'Genre', 'Gg', 'Similar Artists', 'Sounds Similar', 'Similar Composers', 'Main Instrument',
                   'Definitive Song', 'Definitive Album', 'Theme', 'Playing style', 'Vocal type', 'Tone',
                   'Decade', 'Image', 'Description', 'Id', 'Release country', 'Country origin', 'Vevo id',
                   'Albums', 'Performer', 'Composer', 'Writer', 'Genre', 'Gg', 'Decade', 'Other decades',
                   'Similar Albums', 'Sounds Similar', 'Main Instrument',
                   'Release Year', 'Original Date', 'No Reco', 'Product code',
                   'Theme', 'Playing style', 'Vocal type', 'Tone', 'Secondary artist', 'Image', 'Description', 'Id',
                   'Tracks', 'Performer', 'Writer', 'Composer', 'Track Album', 'Duration', 'Pick', 'Reco',
                   'No Reco', 'ISRC', 'Record Label', 'Producer', 'Secondary artist' , 'Decades', 'Instruments',
                   'Vevo id']
        ROVI_ARTIST_STATS = [ 'artist', 'band', 'aka', 'gender', 'birth date', 'death date',
                              'birth place', 'band members', 'important artist', 'artist genres', 'artist good genre',
                              'similar artists', 'sounds similar artists', 'similar composers',
                              'artist main instrument', 'definitive song', 'definitive albums',
                              'artist theme', 'artist playing style', 'artist vocal type', 'artist tones',
                              'artist decades', 'artist image', 'artist desc', 'artist id',
                              'artist release country', 'artist country origin', 'artist vevo id' ]
        ROVI_ALBUM_STATS = [ 'albums', 'album performer', 'album composer', 'album writer', 'album genres',
                             'album good genre', 'album decade', 'album md', 'similar albums', 'sounds similar albums',
                             'album main instrument',  'release year', 'original date',
                             'album no reco', 'album product code', 'album theme', 'album playing style',
                             'album vocal type', 'album tones', 'album secondary artist', 'album image',
                             'album desc', 'album id' ]
        ROVI_TRACK_STATS = [ 'tracks', 'song performer', 'song writer', 'song composer', 'track album', 'duration',
                             'track pick', 'track reco', 'track noreco',
                             'song isrc', 'record label', 'producer' , 'track secondary artist',
                             'track decades', 'track instruments', 'track vevo id']
        TOTAL_STATS = [ 'Total Overrides', 'Overriden Count', 'genres', 'parent genres', 'decades', 'recommendations',
                        'media unbound recommendations', 'influenced', 'influenced_by',
                        'merged rovi genres' ]
        self.group_header = [ 'Rovi Music Parser', 'Artist stats', 'Album stats', 'Track stats' ]
        return [ HEADER, TOTAL_STATS, ROVI_ARTIST_STATS, ROVI_ALBUM_STATS, ROVI_TRACK_STATS ]

    def get_rovi_music_incr_header(self):
        HEADER = [ 'Genres', 'Parent genres', 'Decades', 'Similar records', 'Media Unbound Recommendations',
                   'Influenced', 'Influenced by', 'Merged Genres',
                   'Artists', 'Added artists', 'Deleted artists', 'Dangling artists', 'Bands', 'Aka',
                   'Gender', 'DoB', 'DoD', 'Birth Place', 'Band Members', 'Imp Artist',
                   'Genre', 'Gg', 'Similar Artists', 'Sounds Similar', 'Similar Composers', 'Main Instrument',
                   'Definitive Song', 'Definitive Album', 'Theme', 'Playing style', 'Vocal type', 'Tone',
                   'Decade', 'Image', 'Description', 'Id', 'Release country', 'Country origin',
                   'Albums', 'Added albums', 'Deleted albums', 'Dangling albums', 'Performer',
                   'Composer', 'Writer', 'Genre', 'Gg', 'Decades',
                   'Similar Albums', 'Sounds Similar', 'Main Instrument',
                   'Release Year', 'Original Date', 'No Reco', 'Product code',
                   'Theme', 'Playing style', 'Vocal type', 'Tone', 'Secondary artist', 'Image', 'Description', 'Id',
                   'Tracks', 'Added tracks', 'Deleted tracks', 'Dangling tracks',
                   'Performer', 'Writer', 'Composer', 'Track Album', 'Duration', 'Pick', 'Reco',
                   'No Reco', 'ISRC', 'Record Label', 'Producer', 'Secondary artist' , 'Decades', 'Instruments' ]
        ROVI_ARTIST_STATS = [ 'artist', 'added artists', 'deleted artists', 'dangling artist gids', 'band',
                              'aka', 'gender', 'birth date', 'death date',
                              'birth place', 'band members', 'important artist', 'artist genres', 'artist good genre',
                              'similar artists', 'sounds similar artists', 'similar composers',
                              'artist main instrument', 'definitive song', 'definitive albums',
                              'artist theme', 'artist playing style', 'artist vocal type', 'artist tones',
                              'artist decades', 'artist image', 'artist desc', 'artist id',
                              'artist release country', 'artist country origin']
        ROVI_ALBUM_STATS = [ 'albums', 'added albums', 'deleted albums', 'dangling album gids',
                             'album performer', 'album composer', 'album writer', 'album genres',
                             'album good genre', 'album decade', 'similar albums', 'sounds similar albums',
                             'album main instrument',  'release year', 'original date',
                             'album no reco', 'album product code', 'album theme', 'album playing style',
                             'album vocal type', 'album tones', 'album secondary artist', 'album image',
                             'album desc', 'album id' ]
        ROVI_TRACK_STATS = [ 'tracks', 'added tracks', 'deleted tracks', 'dangling track gids',
                             'song performer', 'song writer', 'song composer',
                             'track album', 'duration', 'track pick', 'track reco', 'track noreco',
                             'song isrc', 'record label', 'producer' , 'track secondary artist',
                             'track decades', 'track instruments']
        TOTAL_STATS = [ 'genres', 'parent genres', 'decades', 'recommendations',
                        'media unbound recommendations', 'influenced', 'influenced_by',
                        'merged rovi genres' ]
        self.group_header = [ 'Rovi Music Parser', 'Artist stats', 'Album stats', 'Track stats' ]
        return [ HEADER, TOTAL_STATS, ROVI_ARTIST_STATS, ROVI_ALBUM_STATS, ROVI_TRACK_STATS ]

    def get_rovi_music_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_music_header, [['rovi_music_parser.py'], ['Rovi Music Stats']] * 4)

    def get_rovi_music_incr_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_music_incr_header, [['rovi_music_parser.py'], ['Rovi Music Stats']] * 4)

    def get_music_seed_archival_header(self):
        HEADER = [ 'Artists', 'Artist popularity', 'Penalized artists',
                   'Albums', 'Album popularity', 'Tracks', 'Track popularity',
                   'Recommendations', 'Merged artists', 'Merged albums', 'Artist merged genres',
                   'Album merged genres', 'Unmerged lastfm artists',
                   'Unmerged lastfm albums',
                   'Song folds', 'Popularity', 'Unmerged lastfm songs', 'Intermediate folds',
                   'Lastfm popularity', 'Youtube popularity', 'Wiki popularity', 'Release tag', 'Tracks under songs' ]
        SONG_STATS = ['song folds', 'song folds pop', 'unmerged lastfm songs', 'song intermediate folds',
                      'lfm song pop', 'ytmusic song pop', 'wiki song pop', 'song release tag', 'Tracks folded under songs']
        TOTAL_STATS = ['artists', 'artist pop', 'penalized artists', 'albums', 'album pop',
                       'tracks', 'track pop', 'recommendations',
                       'merged rovi artists', 'merged rovi albums',
                       'person merged genres', 'album merged genres',
                       'unmerged lastfm artists', 'unmerged lastfm albums']
        self.group_header = [ 'Total merge stats', 'Song folds', 'Lexical folds' ]
        return [ HEADER, TOTAL_STATS, SONG_STATS ]

    def get_music_seed_archival_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_seed_archival_header, [['generate_music_seed.py'], ['Music seed datagen']] * 2)

    def get_music_seed_incr_header(self):
        HEADER = [ 'Artists', 'Artist popularity', 'Penalized artists',
                   'Albums', 'Album popularity', 'Tracks', 'Track popularity',
                   'Recommendations', 'Merged artists', 'Merged albums', 'Artist merged genres',
                   'Album merged genres', 'Unmerged lastfm artists',
                   'Unmerged lastfm albums',
                   'Song folds', 'Popularity', 'Unmerged lastfm songs', 'Intermediate folds',
                   'Lastfm popularity', 'Youtube popularity', 'Wiki popularity', 'Release tag', 'Tracks under songs' ]
        SONG_STATS = ['song folds', 'song folds pop', 'unmerged lastfm songs', 'song intermediate folds',
                      'lfm song pop', 'ytmusic song pop', 'wiki song pop', 'song release tag', 'Tracks folded under songs']
        TOTAL_STATS = ['artists', 'artist pop', 'penalized artists', 'albums', 'album pop',
                       'tracks', 'track pop', 'recommendations',
                       'merged rovi artists', 'merged rovi albums',
                       'person merged genres', 'album merged genres',
                       'unmerged lastfm artists', 'unmerged lastfm albums']
        self.group_header = [ 'Total merge stats', 'Song folds', 'Lexical folds' ]
        return [ HEADER, TOTAL_STATS, SONG_STATS ]

    def get_music_seed_incr_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_seed_incr_header, [['generate_music_seed.py'], ['Music seed datagen']] * 2)

    def get_songid_datagen_header(self):
        HEADER = [ 'Uploaded', 'Songs', 'Genres', 'Tracks', 'Primary artists', 'Composers', 'Song keys' ]
        TOTAL_STATS = [ 'uploaded dump', 'songs' ]
        ASSOC_STATS = [ 'songs', 'genre association', 'track association', 'primary artist association',
                        'composer association', 'song key association' ]
        self.group_header = [ 'Total stats', 'Associations' ]
        return [ HEADER, TOTAL_STATS, ASSOC_STATS ]

    def get_songid_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_songid_datagen_header, [['songid_datagen.py'], ['SongID Stats']] * 3)

    def get_cur_music_seed_header(self):
        HEADER = [ 'Artists', 'Artist popularity', 'Albums', 'Album popularity', 'Tracks', 'Track popularity',
                   'Recommendations', 'Song folds', 'Popularity', 'Intermediate folds', 'Tracks under songs' ]
        SONG_STATS = ['song folds', 'song folds pop', 'song intermediate folds', 'Tracks folded under songs']
        TOTAL_STATS = ['artists', 'artist pop', 'albums', 'album pop',
                       'tracks', 'track pop', 'recommendations']
        self.group_header = [ 'Total stats', 'Song folds', 'Lexical folds' ]
        return [ HEADER, TOTAL_STATS, SONG_STATS ]

    def get_cur_music_seed_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur_music_seed_header, [['generate_cur_seed.py'], ['Cur Music seed datagen']] * 2)

    def get_music_kg_incr_header(self):
        HEADER = ['Addition', 'Change', 'Deletion', 'Addition', 'Change', 'Deletion']
        RELATED_STATS = ['Additions related', 'Changed records related', 'Deleted records related']
        POP_STATS = ['Additions pop', 'Changed records pop', 'Deleted records pop']
        self.group_header = ['Related stats', 'Pop stats']

        return [ HEADER, RELATED_STATS, POP_STATS ]

    def get_music_kg_incr_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_kg_incr_header, [['get_kg_data.py'], ['Music KG Stats']] * 2)

    def get_iheart_deployment_header(self):
        HEADER = ['File count stats']
        TOTAL_STATS = ['files_copied']
        self.group_header = ['Count stats']

        return [ HEADER, TOTAL_STATS ]

    def get_iherat_deployment_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_iheart_deployment_header, [['deploy_iheart_data.py'], ['Count stats']])

    def get_music_charts_header(self):
        MUSIC_CHARTS_SOURCES = [
            'top40', 'billboard200', 'emergingartists', 'hot-christian-songs',
            'hot-country-songs', 'hot-dance-electronic-songs',
            'hot-latin-songs', 'hot-r&b-hiphop-songs', 'hot-rock-songs',
            'hot100', 'last24hours', 'trending140', 'twitter-top-tracks',
            'all sources'
        ]

        HEADER = ['Merge count', 'Total count'] * len(MUSIC_CHARTS_SOURCES)
        HEADER.append('Total')
        HEADER.extend(['Override', 'Restrict'])
        self.group_header = [source.capitalize() for source in MUSIC_CHARTS_SOURCES]
        self.group_header.append('Unmerged artists')
        self.group_header.append('Overrides')
        each_source_stats = [[('%s merged' % source), ('%s total' % source)] for source in MUSIC_CHARTS_SOURCES]
        each_source_stats.append(['Artists unmerged count'])
        each_source_stats.append(['overrides', 'restricted'])
        ret_val = [HEADER]
        ret_val.extend(each_source_stats)

        return ret_val

    def get_music_charts_stats(self, row_list, obj):
        stats_list = [['music_charts_datagen.py'], ['Music charts datagen']] * 15
        stats_list.extend([['merge_sanity.py'], ['MUSIC_CHARTS_DATAGEN Sanity Statistics']])
        self.get_stats_from_file(row_list, obj, self.get_music_charts_header, stats_list)

    def get_music_pop_api_header(self):
        HEADER = ['Album Count', 'Song Count', 'Total Track Count',
            'Total Tracks with Popularity'
        ]
        TOTAL_STATS = ['Album count', 'Song count', 'Track count',
            'Total tracks with popularity'
        ]
        self.group_header = ['Total stats']
        return [HEADER, TOTAL_STATS]

    def get_music_pop_api_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_pop_api_header,
            [['Music_popularity.py'],['Music Pop Api Stats']]
        )

    def get_contextual_sources_ner_header(self):
        HEADER = ['Total Albums','Total Artists','Total Songs','Total Tracks','Total Articles' , 'Total Entities identified']
        TOTAL_STATS = ['Album count','Artist count','Song count','Track count','Number of articles','Number of entities']
        self.group_header = ['Total stats']
        return [HEADER, TOTAL_STATS]

    def get_contextual_sources_ner_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_contextual_sources_ner_header,[['generate_trending_from_contextual_sources.py'],['Contextual Sources Ner Stats']])

    def get_historical_charts_data_header(self):
        HEADER=['Total Tracks','Total songs']
        TOTAL_STATS= ['Track count','Song count']
        self.group_header = ['Total stats']
        return [HEADER, TOTAL_STATS]

    def get_historical_charts_data_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_historical_charts_data_header,[['historical_charts.py'],['Historical Chart Stats']])

    def get_music_kg_header(self):
        HEADER = ['KG only gids', 'Rovi gids', 'KG related', 'Media unbound related']
        COUNT_STATS = ['kg only gids', 'rovi gids', 'kg related', 'media unbound related']
        self.group_header = ['Count stats']

        return [ HEADER, COUNT_STATS ]

    def get_music_kg_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_kg_header, [['get_kg_data.py'], ['Music KG Stats']])

    def get_cur_music_datagen_header(self):
        HEADER = [ 'Artists', 'Albums', 'Tracks', 'Labels', 'Label Owners', 'Genre1', 'Genre2', 'Label', 'Track', 'Duration', 'Date available', 'Remix/Demo', 'Live', 'Alternate', 'NoPlaylist']
        TOTAL_STATS = ['artists', 'albums', 'tracks', 'labels', 'label owners']
        TRACK_GENRE_STATS = ['track genre 1', 'track genre 2']
        MALFORMED_STATS = ['Malformed label record', 'Malformed track record', 'Malformed duration', 'Malformed date available']
        REMIX_STATS = ['remix', 'live', 'alternate versions', 'noplaylist']
        self.group_header = [ 'Total stats', 'Track Genre Stats', 'Malformed Stats', 'Track tags']
        return [ HEADER, TOTAL_STATS, TRACK_GENRE_STATS, MALFORMED_STATS, REMIX_STATS ]

    def get_cur_music_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur_music_datagen_header, [['cur_parser.py'], ['Cur Music Stats']] * 4)

    def get_cur_music_incr_datagen_header(self):
        HEADER = [ 'Artists', 'Albums', 'Tracks', 'Labels', 'Label Owners', 'Genre1', 'Genre2', 'Label', 'Track', 'Duration', 'Date available', 'Remix/Demo', 'Live', 'Alternate', 'Added tracks', 'Changed tracks', 'Deleted tracks' ]
        TOTAL_STATS = ['artists', 'albums', 'tracks', 'labels', 'label owners']
        TRACK_GENRE_STATS = ['track genre 1', 'track genre 2']
        MALFORMED_STATS = ['Malformed label record', 'Malformed track record', 'Malformed duration', 'Malformed date available']
        REMIX_STATS = ['remix', 'live', 'alternate versions']
        ACTION_STATS = ['added tracks', 'changed tracks', 'deleted tracks']
        self.group_header = [ 'Total stats', 'Track Genre Stats', 'Malformed Stats', 'Remix/Demo', 'Action Stats']
        return [ HEADER, TOTAL_STATS, TRACK_GENRE_STATS, MALFORMED_STATS, REMIX_STATS, ACTION_STATS ]

    def get_cur_music_incr_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur_music_incr_datagen_header, [['cur_parser.py'], ['Cur Music Stats']] * 5)

    def get_rovi_music_incr_publish_header(self):
        HEADER = [ 'Tracks total', 'Tracks pop', 'Deadincr records' ]
        TOTAL_STATS = [ 'Tracks total', 'Tracks with pop>0', 'deadincr records' ]
        self.group_header = [ 'Total stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_rovi_music_incr_publish_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_music_incr_publish_header,
                                 [['publishMusicRoviTGZ.py'], ['Rovi Music Publish Stats']])

    def get_cur_music_incr_publish_header(self):

        HEADER = [ 'Tracks total', 'Tracks pop', 'Deleted tracks', 'Changed tracks', 'Added tracks', 'Trending',
                   'Deadincr records', 'Artists', 'Album', 'Missing image key', 'Tracks under split_0',
                   'Big Lexical Tracks count', 'Small Lexical Folds', 'Big Lexical Folds', 'Small Lexical Tracks count' ]
        PUBLISH_TOTAL_STATS = [ 'Tracks Total', 'Tracks with pop>0', 'Deleted tracks', 'Changed tracks', 'Added tracks', 'trending', 'deadincr records' ]
        IMAGE_PATCHER_TOTAL_STATS = ['person', 'album', 'missing image key' ]
        SPLIT_TRACKS_TOTAL_STATS = [ 'Tracks under split_0', 'Big Lexical Tracks count', 'Small Lexical Folds', 'Big Lexical Folds', 'Small Lexical Tracks count' ]
        self.group_header = [ 'Publish total stats', 'Image patcher total stats', 'Split tracks total stats' ]
        return [ HEADER, PUBLISH_TOTAL_STATS, IMAGE_PATCHER_TOTAL_STATS, SPLIT_TRACKS_TOTAL_STATS ]

    def get_cur_music_incr_publish_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur_music_incr_publish_header,
                                 [['publishMusicRoviTGZ.py'], ['Rovi Music Publish Stats'], ['image_patcher.py'], ['Image patcher Stats'], ['split_tracks.py'], ['Rovi Music Split Track']])

    def get_rovi_music_incr_split_tracks_header(self):
        HEADER = [ 'Tracks under split_0', 'Big Lexical Tracks count', 'Small Lexical Folds', 'Big Lexical Folds',
                   'Small Lexical Tracks count']
        TOTAL_STATS = [ 'Tracks under split_0', 'Big Lexical Tracks count', 'Small Lexical Folds', 'Big Lexical Folds',
                        'Small Lexical Tracks count' ]
        self.group_header = [ 'Total stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_rovi_music_incr_split_tracks_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_music_incr_split_tracks_header,
                                 [['split_tracks.py'], ['Rovi Music Split Track']])

    def get_rovi_music_incr_image_patcher_header(self):
        HEADER = [ 'Artists', 'Album', 'Missing image key' ]
        TOTAL_STATS = ['person', 'album', 'missing image key' ]
        self.group_header = [ 'Total stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_rovi_music_incr_image_patcher_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_music_incr_image_patcher_header,
                                 [['image_patcher.py'], ['Image patcher Stats']])

    def get_music_popularity_header(self):
        HEADER = [ 'Non-US artists', 'US origin artists', 'US release made artists', 'UK only artists', 'EU only artists' ]
        TOTAL_STATS = [ 'non us artists', 'us origin artists', 'us release made artists', 'uk only artists',
                        'eu only artists' ]
        self.group_header = [ 'Total stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_music_popularity_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_music_popularity_header,
                                 [['us_music_popularity_datagen.py'], ['Music Popularity Stats']])

    def get_cur8_dev_fetch_datagen_header(self):
        HEADER = [ 'Downloaded dumps', 'New users processed', 'Total users processed' ]
        TOTAL_STATS = [ 'incremental downloads', 'new user ids', 'total users processed' ]
        self.group_header = [ 'Total stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_cur8_dev_fetch_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur8_dev_fetch_datagen_header, [['cur8_fetch.py'], ['Cur8 Fetch Stats']])

    def get_cur8_prod_fetch_datagen_header(self):
        HEADER = [ 'Downloaded dumps', 'New users processed', 'Total users processed' ]
        TOTAL_STATS = [ 'incremental downloads', 'new user ids', 'total users processed' ]
        self.group_header = [ 'Total stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_cur8_prod_fetch_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur8_prod_fetch_datagen_header, [['cur8_fetch.py'], ['Cur8 Fetch Stats']])

    def get_cur8_prebuilt_fetch_datagen_header(self):
        HEADER = [ 'Downloaded dumps', 'Total prebuilt curs processed' ]
        TOTAL_STATS = [ 'incremental downloads', 'total prebuilt curs processed']
        self.group_header = [ 'Total stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_cur8_prebuilt_fetch_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur8_prebuilt_fetch_datagen_header, [['cur8_fetch.py'], ['Cur8 Fetch Stats']])

    def get_cur_incr_fetch_datagen_header(self):
        HEADER = [ 'Downloaded dumps', 'Total tracks processed', 'Added records',
                   'Deleted records', 'Changed records', 'Malformed records' ]
        TOTAL_STATS = [ 'incremental downloads', 'total tracks processed', 'added records',
                        'deleted records', 'changed records', 'malformed records']
        self.group_header = [ 'Total stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_cur_incr_fetch_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur_incr_fetch_datagen_header, [['cur_incr_fetch.py'], ['Cur Incr Fetch Stats']])

    def get_cur8_dev_publish_datagen_header(self):
        HEADER = [ 'Users', 'Playlists', 'Prebuilt curs', 'Artists', 'Artist folds' ]
        TOTAL_STATS = [ 'users', 'playlists', 'prebuilt curs', 'artists', 'artist folds']
        self.group_header = [ 'Cur8 Dev Publish Stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_cur8_dev_publish_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur8_dev_publish_datagen_header, [['cur8_publish.py'], ['Cur8 dev Publish Stats']])

    def get_cur8_prod_publish_datagen_header(self):
        HEADER = [ 'Users', 'Playlists', 'Prebuilt curs', 'Artists', 'Artist folds' ]
        TOTAL_STATS = [ 'users', 'playlists', 'prebuilt curs', 'artists', 'artist folds']
        self.group_header = [ 'Cur8 Prod Publish Stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_cur8_prod_publish_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_cur8_prod_publish_datagen_header, [['cur8_publish.py'], ['Cur8 prod Publish Stats']])

    def get_rovi_music_fetch_datagen_header(self):
        HEADER = [ 'Artist', 'Album', 'Track', 'Release', 'Attribute', 'AttributeLink', 'Association', 'Image', 'ImageKey', 'Dump gids' ]
        TOTAL_STATS = [ 'Name.txt', 'Album.txt', 'Track.txt', 'Release.txt', 'Attribute.txt', 'AttributeLink.txt',
                        'Association.txt', 'Image.txt', 'ImageKey.txt', 'dump gids' ]
        self.group_header = [ 'Total stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_rovi_music_fetch_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_music_fetch_datagen_header, [['rovi_music_fetch.py'], ['Rovi Music Fetch Stats']])

    def get_rovi_music_fetch_cur_image_datagen_header(self):
        HEADER = [ 'AudioSample', 'ImageKey' ]
        TOTAL_STATS = [ 'AudioSample.txt', 'ImageKey.txt' ]
        self.group_header = [ 'Total stats' ]
        return [ HEADER, TOTAL_STATS ]

    def get_rovi_music_fetch_cur_image_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_rovi_music_fetch_cur_image_datagen_header,
                                 [['rovi_music_fetch.py'], ['Rovi Music Fetch Stats']])

    def get_gmrf_datagen_header(self):
        HEADER = [ 'Total', 'Merged', 'Unmerged']
        TOTAL_STATS = [ "total", "seed_merged", "unmerged" ]
        self.group_header = ['Gmrf Stats']
        return [ HEADER, TOTAL_STATS ]

    def get_gmrf_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_gmrf_datagen_header, [['gmrf_wrapper.py'], ['GMRFMerge Stats']])

    def get_ott_parser_header(self):
        HEADER       = [ 'total', 'available', 'rating', 'tvshows', 'movies', 'episodes' ]
        PARSER_STATS = [ 'total', 'available', 'rating', 'tvshows', 'movies', 'episodes' ]
        self.group_header = ['Parser Stats']
        return [ HEADER, PARSER_STATS ]

    def get_ott_parser_stats(self, row_list, obj, source_name=''):
        self.get_stats_from_file(row_list, obj, self.get_ott_parser_header, [['%s_parser.py' % source_name.lower()], ['%s Stats' % source_name]])

    def get_soundtrack_datagen_header(self):
        HEADER = [ 'Merged', 'Direct album merge', 'Unmerged']
        PARSER_STATS = [ 'merged', 'direct album merge', 'unmerged' ]
        self.group_header = ['Soundtrack Datagen Stats']
        return [ HEADER, PARSER_STATS ]

    def get_soundtrack_datagen_stats(self, row_list, obj, source_name=''):
        self.get_stats_from_file(row_list, obj, self.get_soundtrack_datagen_header, [['soundtrack_datagen.py'], ['Soundtrack datagen' ]])

    def get_ytmusic_datagen_header(self):
        HEADER = [ 'Total records' ]
        PARSER_STATS = [ 'pop' ]
        self.group_header = ['YTMusic Datagen Stats']
        return [ HEADER, PARSER_STATS ]

    def get_ytmusic_datagen_stats(self, row_list, obj, source_name=''):
        self.get_stats_from_file(row_list, obj, self.get_ytmusic_datagen_header, [['ytmusic_datagen.py'], ['YTMusic datagen' ]])

    def get_ott_datagen_header(self):
        HEADER = ['Number of sources', 'Failed Sources']
        ott_datagen_stats = [ 'num_sources', 'failed_sources']
        self.group_header = ['OTT Datagen Stats']
        return [ HEADER, ott_datagen_stats ]

    def get_ott_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_ott_datagen_header, [['ott_datagen.py'], ['OTT Datagen Stats']])

    def get_s3_data_copier_header(self):
        HEADER = ['Num Failed', 'Failed Sections']
        s3_data_copier_stats = [ 'num_failed', 'failed_sections' ]
        self.group_header = [ 'S3 DATA COPIER STATS' ]
        return [ HEADER, s3_data_copier_stats ]

    def get_s3_data_copier_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_s3_data_copier_header, [['s3_data_copier.py'], ['S3 DATA COPIER STATS']])

    def get_pre_publish_daily_seed_runner_header(self):
        key_names = ['msg', 'copied', 'tarred', 'uploaded']
        HEADER = ['progress msg', 'seed copied', 'seed tarred as', 'seed uploaded as']
        self.group_header = ['Pre Publish Daily Seed Runner Stats']
        return [ HEADER, key_names ]

    def get_pre_publish_daily_seed_runner_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_pre_publish_daily_seed_runner_header, [['create_pre_publish_seed.py'], ['PRE_PUBLISH_SEED_CREATION']])

    def get_pre_publish_runner_header(self):
        HEADER = ['Group Name', 'Num Sources', 'Failed Sources']
        pre_publish_runner_stats = [ 'group', 'num_sources', 'failed_sources']
        self.group_header = ['Pre Publish Runner Stats']
        return [ HEADER, pre_publish_runner_stats ]

    def get_pre_publish_runner_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_pre_publish_runner_header, [['pre_publish_runner.py'], ['pre_publish_runner.py']])

    def get_pre_publish_fetcher_header(self):
        HEADER = ['Seen Sections', 'Failed Sections']
        pre_publish_fetcher_stats = [ 'all_sections', 'failed_sections' ]
        self.group_header = ['PRE PUBLISH FETCHER STATS']
        return [ HEADER, pre_publish_fetcher_stats ]

    def get_pre_publish_fetcher_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_pre_publish_fetcher_header, [['pre_publish_fetcher.py'], ['PRE PUBLISH STATS']])

    def get_nbcu_parser_header(self):
        header_key_tuple_list = [
            #          header                                 key in pickle file
            ("Broadcast History Programs"               , "broadcast_history_programs"),
            ("Broadcast History Sources"                , "num_sources_from_broadcast_history"),
            ("Broadcast History Providers"              , "num_providers_from_broadcast_history"),
            ("Programs with parent (from cluster)"      , "matched_parent_programs"),
            ("Programs without parents"                 , "unmatched_parent_programs"),
            ("Bound data cluster count"                 , "rovi_bound_parse_data_Program_Cluster.txt_cluster_count"),
            ("Written to rovi.data"                     , "num_records_written_to_rovi.data"),
            ("Written to crew.data"                     , "num_records_written_to_crew.data"),
            ("Written to rating.data.aux"               , "num_records_written_to_rating.data.aux"),
            ("Unbound data cluster count"               , "rovi_data_Program_Cluster.txt_cluster_count"),
            ("actual sk to mc record count"             , "sk_to_mc_record_count"),
            ("descriptionless records in sk to mc"      , "descriptionless_record_count"),
        ]
        HEADER = [i[0] for i in header_key_tuple_list ]
        nbcu_parser_stats = [i[1] for i in header_key_tuple_list ]
        self.group_header = ['NBCU Parser Stats']
        return [ HEADER, nbcu_parser_stats ]

    def get_nbcu_parser_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_nbcu_parser_header, [['nbcu_parser.py'], ['NBCU Parser Stats']])

    def get_ott_merger_header(self):
        HEADER = []
        ott_merger_stats = []
        for vt in ("movies", "episodes", "tvseries", "others"):
            HEADER.append("Total %s" % vt)
            ott_merger_stats.append("num_%s_entries" % vt)
            HEADER.append("%s Merge Percentage" % vt)
            ott_merger_stats.append("%s_merge_percentage" % vt)

        ott_merger_stats.append("failed_sources")
        HEADER.append("FAILED SOURCES")
        self.group_header = ['OTT Merger Stats']
        return [ HEADER, ott_merger_stats ]

    def get_ott_merger_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_ott_merger_header, [['ott_merger.py'], ['OTT Merger Stats']])

    def get_ott_episode_merger_header(self):
        self.group_header = []
        return [[]]

    def get_ott_episode_merge_stats(self, row_list, obj):
        STATS_LIST = [['ott_episode_merge.py'], ['OTT Episode Merger Stats']]
        self.get_stats_from_file(row_list, obj, self.get_ott_episode_merger_header, STATS_LIST)

    def get_image_datagen_header(self):
        HEADER = [ 'Total', 'Movies', 'TV Series', 'Personalities' ] * 4

        IMAGE_TOTAL_STATS = [ 'total', 'movies', 'tvshows', 'personalities' ]
        IMAGE_TYPE1_STATS = IMAGE_TOTAL_STATS
        IMAGE_TYPE2_STATS = IMAGE_TOTAL_STATS
        IMAGE_TYPE3_STATS = IMAGE_TOTAL_STATS

        self.group_header = [ 'Images', 'Type 1', 'Type 2', 'Type 3' ]

        return [ HEADER, IMAGE_TOTAL_STATS, IMAGE_TYPE1_STATS, IMAGE_TYPE2_STATS, IMAGE_TYPE3_STATS ]

    def get_phrase_file_header(self):

        HEADER = ['gid-count', ]
        COUNT_STATS = ['gid-count',]

        self.group_header = [ 'Count Stats' ]

        return [ HEADER, COUNT_STATS ]


    def get_phrase_file_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_phrase_file_header, [['create_gid_phrases.py'], ['COUNT STATS']])


    def get_image_datagen_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_image_datagen_header, [['image_datagen.py'], ['Image Stats'], ['image_datagen.py'], ('nested', ['Image Stats'], ['Type 1']), ['image_datagen.py'], ('nested', ['Image Stats'], ['Type 2']), ['image_datagen.py'], ('nested', ['Image Stats'], ['Type 3'])])

    def get_copier_header(self):
        HEADER = [ 'Fail' ]

        COPIER_STATS = [ 'failed' ]

        self.group_header = [ 'Copier Stats' ]

        return [ HEADER, COPIER_STATS ]

    def get_copier_stats(self, row_list, obj):
        self.get_stats_from_file(row_list, obj, self.get_copier_header, [['data_copier.py'], ['Failed Copiers']])

    def get_report_header(self):
        HEADER = [ 'Report' ]

        REPORT_STATS = [ 'report' ]

        self.group_header = [ 'Test Report' ]

        return [ HEADER, REPORT_STATS ]

    def get_report_stats(self, row_list, obj):
        if not row_list:
            self.stats_header = self.get_report_header()
            return

        HEADER, REPORT_STATS = self.stats_header

        REPORT_DICT = { 'DATAGEN_TEST'    : 'datagen_test_report',
                        'ALL_DATA_REPORT' : 'all_data_report',
                        'DATAGEN_SUMMARY' : 'datagen_summary_report',
                      }

        file_prefix = REPORT_DICT.get(self.local_dir_name, 'report')

        report_link = "<a href='%s'>%s</a>" % ('http://%s/REPORTS/%s/%s_%s.html' % (self.ip, self.remote_dir_name, file_prefix, self.current_date), self.current_date)
        row_list.append(report_link)

    def get_status_color(self, status):
        COLOR_DICT = { 'SUCCESS' : 'badge-success', 'FAILURE' : 'badge-important', 'DORMANT' : 'badge-warning' }

        return '<span class="badge %s">%s</span>' % (COLOR_DICT.get(status, ''), status)

    def get_program_stats(self, row_list, obj, actual_file_name, final_script):
        if actual_file_name:
            file_dir, file_name = actual_file_name.split('/')
            date_str = parser.parse(file_name, fuzzy=True).strftime(TIMEFORMAT)
        else:
            date_str = self.current_date.strftime(TIMEFORMAT)

        if not obj:
            status_str = self.get_status_color('DORMANT')
            row_list.extend([ date_str, status_str ])
            return False

        #Status Display
        full_status = True
        failed_script = ''
        for script_name in obj:
            status_flag = obj[script_name]['status'][0]
            if status_flag:
                full_status = False
                failed_script = script_name
        if final_script and not obj.get(final_script):
            full_status = False
            failed_script = final_script
        if full_status:
            status_str = self.get_status_color('SUCCESS')
        else:
            status_str = self.get_status_color('FAILURE')

        #Hyperlink to html files
        pickle_file_name = os.path.basename(actual_file_name)
        link = "http://%s/REPORTS/%s/%s" % (self.ip, self.remote_dir_name, pickle_file_name)
        final_link = "<a href='%s'>Final</a>" % link.replace('.pickle', '_final.html')
        incr_link = "<a href='%s'>Increment</a>" % link.replace('.pickle', '.html')

        peak_time = self.get_time_stats(obj)

        peak_memory, peak_diff = self.get_memory_stats(obj)

        row_list.extend([date_str, status_str, failed_script, final_link, incr_link, peak_time, peak_memory, peak_diff])

        return full_status

    def calculate_diff(self, actual_file_name, domain_list, row_list, common_length):
        if len(domain_list) <= 1:
            return

        for i in reversed(range(len(domain_list))):
            date_str, status_str = domain_list[i][:2]
            if 'SUCCESS' in status_str:
                break
        else:
            return

        prev_row_list = domain_list[i]
        if len(prev_row_list) != len(row_list):
            return

        for i, value in enumerate(row_list):
            if i < common_length:
                continue

            try:
                value = int(value)
                last_value = int(prev_row_list[i])
            except ValueError:
                continue
            except TypeError:
                continue

            INFO_VALUE = 5
            diff_value = abs(last_value - value)

            direction, color = '', ''
            if diff_value > 0 and diff_value <= INFO_VALUE:
                color = 'warning'
            if last_value > value:
                direction = 'up'
                if not color:
                    color = 'success'
            elif last_value < value:
                direction = 'down'
                if not color:
                    color = 'important'

            try:
                last_value = '{:,}'.format(last_value)
            except (ValueError, TypeError):
                pass

            if direction and color:
                prev_row_list[i] = '<span class="badge badge-%s">%s<i class="icon-arrow-%s"></i></span>' % (color, last_value, direction)
            else:
                prev_row_list[i] = last_value

    def generate_summary(self):
        summary_list = []

        date_diff = (self.yesterday - self.BASE_DATE).days
        print 'Date Diff: ', date_diff

        no_stats_func_list = set()

        table_list = []

        for dir_info in self.STATS_DIR_LIST:
            self.local_dir_name, self.server_name, self.ip, frequency, report_prefix, self.remote_dir_name, date_opt, title = dir_info
            if self.options.run_only and self.local_dir_name not in self.options.run_only:
                continue
            print dir_info

            domain_list = []

            heading = '%s - <small>%s</small>' % (title, self.ip)

            self.current_date = self.yesterday

            if date_opt == 'N':
                yesterday = self.run_graph_date
                time_format = '%Y%m%d'
            else:
                yesterday = self.run_date
                time_format = TIMEFORMAT

            stats_func, args, new_stats_func, new_stats_date, final_script, new_final_script = self.STATS_FUNC_DICT.get(self.local_dir_name, (None, ''))
            prev_stats_func = stats_func
            prev_final_script = final_script
            new_stats_datestamp = None
            if new_stats_date:
                new_stats_datestamp = datetime.strptime(new_stats_date , '%Y-%m-%d').date()

            header_row, table_format = self.get_common_header(heading, stats_func)
            max_column_length = len(header_row)

            domain_list.append(header_row)
            table_list.append(table_format)

            local_pickle_prefix = os.path.join(self.local_dir_name, report_prefix)
            file_list = self.get_files_in_date_range(local_pickle_prefix, yesterday, num_of_days = date_diff, time_format = time_format)
            first = False

            last_status, last_peak_time, last_peak_memory = '' , '', ''
            total_count, pass_count = 0, 0
            #Processing each pickle file from respective directory
            for file_name in file_list:
                self.logger.info('Processing: %s' % file_name)
                
                row_list = []
                obj = None
                actual_file_name = ''
                stats_func = prev_stats_func
                if new_stats_func:
                    file_datestamp = datetime.strptime(file_name.split('_')[-1] , '%Y-%m-%d').date()
                    if (new_stats_datestamp and file_datestamp > new_stats_datestamp):
                        stats_func = new_stats_func
                        final_script = new_final_script
                    else:
                        final_script = prev_final_script
                try:
                    actual_file_list = glob.glob("%s/%s*" % (self.PICKLE_FILES_DIR, file_name))
                    actual_file_list.sort(reverse=True)
                    actual_file_name = actual_file_list[0]
                    obj = vtv_unpickle(actual_file_name, None)
                except (IndexError, AttributeError):
                    self.logger.error('Unpickle failed for %s' % actual_file_name)

                if frequency in ['MONTHLY', 'WEEKLY'] and not obj:
                    continue

                full_status = self.get_program_stats(row_list, obj, file_name, final_script)

                total_count += 1
                if full_status:
                    pass_count += 1

                if not stats_func:
                    no_stats_func_list.add(self.local_dir_name)

                common_length = len(row_list)
                if obj and full_status and stats_func:
                    if args:
                        stats_func(row_list, obj, args)
                    else:
                        stats_func(row_list, obj)
                else:
                    row_list.extend(['']*(max_column_length - common_length))

                if not last_status:
                    last_date_str, last_status, failed_script, final_link, incr_link, last_peak_time, last_peak_memory = row_list[:7]

                self.current_date -= timedelta(days=1)

                self.calculate_diff(actual_file_name, domain_list, row_list, common_length)

                domain_list.append(row_list)
            try:
                percentage = (pass_count * 100) / total_count
            except:
                percentage = 0

            if not last_status:
                last_status = self.get_status_color('DORMANT')
                row_list = [self.yesterday, last_status] + ['']*(max_column_length - 2)
                domain_list.append(row_list)

            percent_str = '<font color="%s">%d</font>' % (self.get_percent_color(percentage), percentage)
            summary_list.append((heading, last_status, percent_str, last_peak_time, last_peak_memory, domain_list))

        self.create_report_file(self.yesterday, summary_list, table_list)

        if no_stats_func_list:
            print 'No Stats Func:\n    ', '\n    '.join(no_stats_func_list)

    def create_report_file(self, date_yesterday, summary_list, table_list):
        graph_list = ['Date', 'New']

        JINJA_LIST = []
        count = 0
        old_server_name = ''
        blue_list = []
        col_span = 6
        for dir_info in self.STATS_DIR_LIST:
            local_dir_name, server_name, ip, frequency, report_prefix, remote_dir_name, date_opt, title = dir_info
            count += 1
            if old_server_name != server_name:
                JINJA_LIST.append(JINJA_LOOP_FORMAT_STR % (count, col_span, server_name, server_name))
                blue_list.append(count - 1)
            old_server_name = server_name
        if old_server_name and old_server_name != server_name:
            JINJA_LIST.append(JINJA_LOOP_FORMAT_STR % (count, col_span, server_name, server_name))
            blue_list.append(count - 1)
        blue_list.append(count)
        blue_list.pop(0)

        jinja_text = open(JINJA_FILE_NAME).read()
        jinja_text = jinja_text.replace('#BLUE_ITEMS#', ', '.join(map(str, blue_list)))
        jinja_text = jinja_text.replace('#LIST_ITEMS#', ''.join(JINJA_LIST))
        jinja_text = jinja_text.replace('#TABLE_ITEMS#', '\n'.join(table_list))

        open(HTML_FILE_NAME, 'w').write(jinja_text)

        jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader(os.getcwd()))
        jinja_template = jinja_environment.get_template(HTML_FILE_NAME)
        table_html = jinja_template.render(yesterday=date_yesterday, big_list=summary_list, graph_data=graph_list)

        file_prefix = 'DEBUG_' if self.options.debug else ''
        file_format = '%s%s_report_%%s.html' % (file_prefix, self.script_prefix)
        today_file_name = os.path.join(self.SUMMARY_REPORTS_DIR, file_format % self.today_str)

        open(today_file_name, 'w').write(table_html)

        if not self.options.debug:
            latest_file_name = os.path.join(self.SUMMARY_REPORTS_DIR, file_format % 'latest')
            copy_file(today_file_name, latest_file_name, self.logger)

    def run_summary(self):
        self.init_summary()
        self.generate_summary()

        print 'Done - Summary Report Generation'

    def run_main(self):
        if self.options.run in [ 'all', 'report' ]:
            self.run_report()

        if self.options.run in [ 'all', 'scp' ]:
            self.run_scp()

        if self.options.run in [ 'all', 'html' ]:
            self.run_summary()

    def post_print_stats(self):
        self.print_report_link()

    def cleanup(self):
        self.move_logs(self.OUT_DIR, [ ('.', 'datagen_summary*log') ] )
        self.remove_old_dirs(self.OUT_DIR, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

if __name__ == '__main__':
    vtv_task_main(Report)
