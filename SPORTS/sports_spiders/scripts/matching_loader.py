#!/usr/bin/env python
# -*- coding: utf-8 -*-

# $Id: matching_loader.py,v 1.1 2018/01/25 06:16:29 headrun Exp $

import os, sys, glob, json
from collections import Counter, defaultdict
import redis

from vtv_task import VtvTask, vtv_task_main
from vtv_utils import   remove_dir, make_dir, make_dir_list, remove_file, \
                        remove_file_list
from utils import make_metacontent_load_query, modify_record, get_metacontent_table_name, \
                get_candidates_key, get_candidates_string, get_verification_table_name, \
                REDIS_KEY_SEPARATOR, UNVERIFIED, MORE_POPULAR, LESS_POPULAR, \
                HIGH_CONFIDENCE, LOW_CONFIDENCE, make_verification_table_insert_query
from data_schema import RECORD_SEPARATOR, FIELD_SEPARATOR, PAIRS_SEPARATOR, get_schema
from data_constants import CONTENT_TYPE_MOVIE, CONTENT_TYPE_TVSERIES

class MatchingToolLoader(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.init_config(self.options.config_file)
        self.section = self.options.section
        self.default_section = self.options.default_section
        if not self.section:
            print 'No Matching Tool Section given... Exiting now..'
            self.logger.info('No Matching Tool Section given... Exiting now..')
            sys.exit(1)

        self.db_ip = self.get_config_value('DB_IP', self.section) or self.get_config_value('DB_IP', self.default_section)
        self.db_name = self.get_config_value('DB_NAME', self.section) or self.get_config_value('DB_NAME', self.default_section)
        self.redis_ip = self.get_config_value('REDIS_IP', self.section) or self.get_config_value('REDIS_IP', self.default_section)
        self.redis_port = int(self.get_config_value('REDIS_PORT', self.section)) or int(self.get_config_value('REDIS_PORT', self.default_section))
        self.guid_pair_db = int(self.get_config_value('GUID_PAIR_DB', self.section)) or int(self.get_config_value('GUID_PAIR_DB', self.default_section))
        self.candidates_db = int(self.get_config_value('CANDIDATE_DB', self.section)) or int(self.get_config_value('CANDIDATE_DB', self.default_section))
        self.parent_prefix_tuple = self.get_config_value('PARENT_PREFIX', self.section)
        self.child_prefix_tuple = self.get_config_value('CHILD_PREFIX', self.section)

        print 'DB IP            - %s' % self.db_ip
        print 'DB NAME          - %s' % self.db_name
        print 'REDIS IP         - %s' % self.redis_ip
        print 'REDIS_PORT       - %s' % self.redis_port
        print 'GUID PAIR DB     - %s' % self.guid_pair_db
        print 'CANDIDATES DB    - %s' % self.candidates_db

        self.source = self.options.source
        self.l_source, self.u_source = self.source.lower(), self.source.upper()
        print self.source

        self.user_id = int(self.options.user_id)

        self.input_file = self.options.input_file
        self.input_file_type = self.options.input_file_type
        print self.input_file
        print self.input_file_type

        self.merge_file = self.options.merge_file
        self.candidates_file = self.options.candidates_file
        print self.merge_file
        print self.candidates_file

        self.program_type_file = self.options.program_type_file
        self.ptype_file_type = self.options.ptype_file_type
        self.type_key = self.options.type_key
        self.program_type_default = self.options.program_type_default

        if self.program_type_default.lower() == CONTENT_TYPE_TVSERIES.lower():
            self.program_type_default = 'TVSHOW'

        self.popularity_file = self.options.popularity_file
        self.popularity_default = self.options.popularity_default
        self.pop_file_type = self.options.pop_file_type
        self.pop_key = self.options.pop_key
        if self.popularity_default == '0':
            self.popularity_default = LESS_POPULAR
        else:
            self.popularity_default = MORE_POPULAR
        self.popularity_threshold = float(self.options.popularity_threshold)
        self.pop_for_sk = self.options.pop_for_sk

        self.confidence_threshold = float(self.options.confidence_threshold)

        self.program_type_map = {}
        self.popularity_map = {}
        self.all_ids = set()

        # OUT_DIR, OUT_DATA_DIR, REPORTS_DIR are created here - Add all initializations before this.
        self._stats = Counter()
        self.out_dir_name = 'MATCHING_TOOL_LOADER_DATAGEN'
        self.out_data_dir_name = 'matching_tool_loader_data'
        self.OUT_DIR = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, self.out_dir_name)
        self.REPORTS_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR, self.out_dir_name)
        self.OUT_DATA_DIR = os.path.join(self.OUT_DIR, self.out_data_dir_name)
        make_dir_list([self.OUT_DIR, self.OUT_DATA_DIR, self.REPORTS_DIR], self.logger)
        # self.options.report_file_name = os.path.join(self.REPORTS_DIR, '%s.html' % self.name_prefix)


    def set_options(self):
        # Basic options to clear logs and reports
        self.parser.add_option('', '--clear-log-and-report', action='store_true', default=False, help='remove log and report files etc')
        self.parser.add_option('', '--clear-logs-and-report-dirs', action='store_true', default=False, help='remove log directories and reports directories etc: use this option with caution - as all the previous logs and reports will be deleted permanently')

        # Actual options to be added here
        config_file = os.path.join(self.system_dirs.VTV_ETC_DIR, 'matching_tool.cfg')
        self.parser.add_option("", '--config-file', default=config_file, help='config file to be used')

        # Tool options
        self.parser.add_option("", '--section', default='', help='Section in the config file')
        self.parser.add_option("", '--metacontent', default=False, metavar="BOOL", action="store_true", help='If you want to load the metacontent')
        self.parser.add_option("", '--merge-data', default=False, metavar="BOOL", action="store_true", help='If you want to load the merges')
        self.parser.add_option("", '--merge-meta', default=False, metavar="BOOL", action="store_true", help='load merge file and its meta-content')
        self.parser.add_option("", '--default-section', default='', help='default section in the config file HEADRUN')
        self.parser.add_option("", '--source', default='', help='Source name in the metacontenttable or verificationtable')

        # Loading Options
        # self.parser.add_option("", '--multiple-accepts', default=False, metavar="BOOL", action="store_true", help='')
        self.parser.add_option("", '--user-id', default=1, help='')

        # Metacontent into the DB options
        self.parser.add_option("", '--input-file', default='', help='Metacontent Input file')
        self.parser.add_option("", '--input-file-type', default='generic', help='Metacontent file type')

        # Merge Data into the Redis
        self.parser.add_option("", '--merge-file', default='', help='guid merge list file')
        self.parser.add_option("", '--candidates-file', default='', help='optional: candidates file generated by merge')

        # Program Type File
        self.parser.add_option("", '--program-type-file', default='', help='File to get the Program Type for the Gids')
        self.parser.add_option("", '--ptype-file-type', default='generic', help='file type of the above file')
        self.parser.add_option("", '--program-type-default', default=CONTENT_TYPE_MOVIE, help='Default program type to be used if not found')
        self.parser.add_option("", '--type-key', default='Vt', help='key where program_type is present in the file')

        # Popularity File
        self.parser.add_option("", '--popularity-file', default='', help='Popularity input file')
        self.parser.add_option("", '--pop-file-type', default='fold', help='file type of the above file')
        self.parser.add_option("", '--popularity-default', default=1, help='0 is LessPopular and any other thing is HighPopularity')
        self.parser.add_option("", '--pop-key', default='Bp', help='key where popularity is present in the file')
        self.parser.add_option("", '--popularity-threshold', default='0.00000001', help='If this is empty everything is default popularity')
        self.parser.add_option("", '--pop-for-sk', default=False, metavar="BOOL", action="store_true", help='If popularity is for sk or the gids')

        # Confidence
        self.parser.add_option("", '--confidence-threshold', default=20, help='Confidence threshold for the merges')


    def post_print_stats(self):
        self.print_report_link()
        for stats, value in sorted(self._stats.items()):
            print "%s: %d" % (stats, value)
            self.logger.info("%s: %d" % (stats, value))


    def finish(self):
        dir_list = []
        self.set_source_data_version(dir_list)
        file_list = []
        tar_files = []
        self.tar_file_prefix = ''
        if tar_files:
            self.tgz_file_name = self.archive_data_files(self.OUT_DIR, self.OUT_DATA_DIR, tar_files, self.tar_file_prefix)


    def load_metacontent(self, merged_gid_set=set()):
        found_meta_gid_set = set()
        file_list = []
        if self.options.merge_meta:
            input_files = self.get_config_value('DATA_FILES', self.section) or  self.get_config_value('DATA_FILES', self.default_section)
            for in_file in input_files.split(','):
                dir_name, filename, file_type = in_file.split('<>')
                filename = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, dir_name, filename)
                file_list.append((filename, file_type))
        else:
            file_list = [(self.input_file, self.input_file_type)]
        for input_file, input_file_type in file_list:
            if not (self.source and input_file and self.db_ip and self.db_name):
                print 'Not enough information to load into the tool..'
                print 'Need source, input_file, db_ip, db_name'
                print 'Exiting...'
                sys.exit(1)

            self.open_server_cursor(self.db_ip, self.db_name)
            table_name = get_metacontent_table_name(self.source)

            schema = {}
            count = 0
            for record in self.read_data_file(input_file, input_file_type, schema, change_schema=True):
                modified_record = modify_record(record, schema)
                json_string = json.dumps(modified_record)
                guid = modified_record['Gi']
                if (merged_gid_set and guid not in merged_gid_set):
                    continue
                found_meta_gid_set.add(guid)
                query = make_metacontent_load_query(guid, json_string, table_name)
                self.cursor.execute(query)
                count += 1
                if count % 10000 == 0:
                    self.db.commit()
                    print '%s upload to database' % count
                    self.logger.info('%s upload to database' % count)
            self.db.commit()

        return found_meta_gid_set


    def get_bucket_info(self, sk, candidates_info):
        gids, scores = [[c[i] for c in candidates_info] for i in (0,1,)]
        scores = [float(s) for s in scores]

        program_type = self.program_type_map.get(sk, self.program_type_default).upper()

        pop = self.popularity_default
        if self.popularity_map and self.popularity_threshold:
            if self.pop_for_sk:
                sk_pop = self.popularity_map.get(sk)
                if sk_pop:
                    pop = LESS_POPULAR
                    if sk_pop > self.popularity_threshold:
                        pop = MORE_POPULAR
            else:
                max_pop = max([self.popularity_map.get(g) for g in gids])
                pop = LESS_POPULAR
                if max_pop and max_pop > self.popularity_threshold:
                    pop = MORE_POPULAR

        max_score = max(scores)
        confidence = LOW_CONFIDENCE
        if max_score > self.confidence_threshold:
            confidence = HIGH_CONFIDENCE

        attributes = (self.u_source, program_type, confidence, pop, UNVERIFIED)
        redis_key = REDIS_KEY_SEPARATOR.join(attributes)
        return redis_key


    def load_into_db(self, db_load_list):
        self.open_server_cursor(self.db_ip, self.db_name)
        table = get_verification_table_name(self.l_source)
        for info in db_load_list:
            sk, new_c_list, new_bucket = info
            for c in new_c_list:
                gid, score, action = c
                self._stats['Newly Loaded into the Tool'] += 1
                query = make_verification_table_insert_query(table, self.user_id, gid, sk, score, action, new_bucket)
                self.cursor.execute(query)
        self.db.commit()
        self.close_cursor()


    def load_merge_map_to_redis(self, merge_map):
        redis_candidates_conn = redis.Redis(self.redis_ip, port=self.redis_port, db=self.candidates_db)
        redis_guids_conn = redis.Redis(self.redis_ip, port=self.redis_port, db=self.guid_pair_db)
        db_load_list = []

        for sk, candidates_info in merge_map.iteritems():
            c_key = get_candidates_key(self.u_source, sk)
            existing_candidates_info = get_candidates_string(redis_candidates_conn, c_key)
            existing_c_list = []
            existing_candidates_string = ''
            old_bucket = ''
            if existing_candidates_info:
                existing_candidates_string, old_bucket = existing_candidates_info.split(PAIRS_SEPARATOR)

                existing_c_list = existing_candidates_string.split(RECORD_SEPARATOR)

            new_c_list = []
            for c_i in candidates_info:
                gid, score = c_i
                temp_gid = '%s%s' % (gid, FIELD_SEPARATOR)
                if any([temp_gid in gsa for gsa in existing_c_list]):
                    # print '%s already present in existing candidates %s' % (gid, existing_c_list)
                    self.logger.info('%s already present in existing candidates %s' % (gid, existing_c_list))
                    self._stats['Candidate already existing'] += 1
                    continue

                new_c_list.append((gid, score, UNVERIFIED))

            if not new_c_list:
                # print 'No new candidates for %s' % (sk)
                self.logger.info('No new candidates for %s' % (sk))
                continue

            new_c_string = RECORD_SEPARATOR.join([FIELD_SEPARATOR.join(g_s_a) for g_s_a in new_c_list])
            full_c_string = new_c_string
            if existing_candidates_string:
                full_c_string = '%s%s%s' % (new_c_string, RECORD_SEPARATOR, existing_candidates_string)

            if old_bucket:
                attributes = old_bucket.split(REDIS_KEY_SEPARATOR)
                attributes[-1] = UNVERIFIED
                new_bucket = REDIS_KEY_SEPARATOR.join(attributes)
                redis_guids_conn.lrem(old_bucket, sk)
            else:
                new_bucket = self.get_bucket_info(sk, candidates_info)

            redis_guids_conn.lpush(new_bucket, sk)
            full_c_info = '%s%s%s' % (full_c_string, PAIRS_SEPARATOR, new_bucket)
            redis_candidates_conn.set(c_key, full_c_info)

            db_load_list.append((sk, new_c_list, new_bucket, ))

        self.load_into_db(db_load_list)


    def load_merge_data(self):
        if self.options.merge_meta:
            list_file = self.get_config_value('GUID_MERGE_FILES', self.section) or  self.get_config_value('GUID_MERGE_FILES', self.default_section)
            dir_name, filename, file_type = list_file.split('<>')
            merge_file = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, dir_name, filename)
        else:
            merge_file  = self.merge_file
        if not all([self.source, merge_file, self.db_ip, self.db_name, self.redis_ip, self.redis_port, str(self.guid_pair_db), str(self.candidates_db), ]):
            print 'Not enough information to load into the tool..'
            print 'Exiting...'
            sys.exit(1)

        merge_map = defaultdict(list)
        for m_f in (merge_file, self.candidates_file, ):
            if not m_f:
                continue
            if not os.path.exists(m_f):
                print 'File %s doesnot exist.'
                self.logger.info('File %s doesnot exist.')
                continue

            for line in self.read_data_lines(m_f):
                field_list = line.split(FIELD_SEPARATOR)
                if len(field_list) == 2:
                    gid, sk = field_list
                    score = 0
                else:
                    gid, sk, score = field_list
                if (self.parent_prefix_tuple and not gid.startswith(self.parent_prefix_tuple)):
                    continue
                if (self.child_prefix_tuple and not sk.startswith(self.child_prefix_tuple)):
                    continue
                    
                merge_map[sk].append((gid, '%s' % score, ))
                self.all_ids.add(gid)
                self.all_ids.add(sk)

        if not merge_map:
            print 'No merge_map generated from the given merge_file and candidates_file...'
            print 'Exiting...'
            sys.exit(1)

        self.get_programtypes_and_popularity()

        return merge_map

    def read_programtypes(self):
        if not os.path.exists(self.program_type_file):
            print 'Program Type File %s doesnot exist' % self.program_type_file
            self.logger.info('Program Type File %s doesnot exist' % self.program_type_file)
            print 'Default Program Type is %s' % self.program_type_default
            return

        schema = get_schema(['Gi', self.type_key])
        for record in self.read_data_file(self.program_type_file, self.ptype_file_type, schema):
            gid, program_type = record
            if gid not in self.all_ids:
                continue
            if program_type == CONTENT_TYPE_TVSERIES:
                program_type = 'tvshow'
            self.program_type_map[gid] = program_type


    def read_popularity(self):
        if not os.path.exists(self.popularity_file):
            print 'Popularity File %s doesnot exist' % self.popularity_file
            self.logger.info('Popularity File %s doesnot exist' % self.popularity_file)
            print 'Default Popularity is %s' % self.popularity_default
            return

        schema = get_schema(['Gi', self.pop_key])
        for record in self.read_data_file(self.popularity_file, self.pop_file_type, schema):
            gid, popularity = record
            if gid not in self.all_ids:
                continue
            self.popularity_map[gid] = int(popularity)
            pass


    def get_programtypes_and_popularity(self):
        self.read_programtypes()
        #get Bp field from seed data and load it
        self.read_popularity()


    def run_main(self):
        if self.options.merge_meta:
            merge_map = self.load_merge_data()
            found_meta_gid_set = self.load_metacontent(self.all_ids)
            modified_merge_map = defaultdict(list)

            for sk, gid_score_set in merge_map.iteritems():
                if sk not in found_meta_gid_set:
                    self.logger.info('Meta not present for %s' % sk)
                    continue
                for gid, score in gid_score_set:
                    if gid not in found_meta_gid_set:
                        self.logger.info('Meta not present for %s, sk %s ' % (gid, sk))
                        continue
                    modified_merge_map[sk].append((gid, score, ))

            final_count = len(modified_merge_map)
            total = len(merge_map)
            print 'final: ', final_count, 'total: ', total, 'missing meta-data: ', (total - final_count)
            self.load_merge_map_to_redis(modified_merge_map)

        else:
            if self.options.metacontent:
                found_meta_gid_set = self.load_metacontent()

            if self.options.merge_data:
                merge_map = self.load_merge_data()
                self.load_merge_map_to_redis(merge_map)

        self.add_stats('Stats', self._stats)
        self.finish()


    def cleanup(self):
        self.move_logs(self.OUT_DIR, [ ('.', '%s*log'%self.script_prefix) ] )
        self.log_dirs_to_keep = 7
        self.remove_old_dirs(self.OUT_DIR, self.logs_dir_prefix, self.log_dirs_to_keep)
        if self.options.clear_log_and_report:
            files_to_be_removed = []
            files_to_be_removed.append(os.path.join(self.OUT_DIR, self.logs_dir_name, self.options.log_file_name))
            files_to_be_removed.append(self.options.report_file_name)
            files_to_be_removed.append(self.pickle_file_name)
            remove_file_list(files_to_be_removed, self.logger)
        if self.options.clear_logs_and_report_dirs:
            remove_dir(os.path.join(self.OUT_DIR, self.logs_dir_name), self.logger)
            remove_dir(self.REPORTS_DIR, self.logger)



if __name__ == '__main__':
    vtv_task_main(MatchingToolLoader)
