# -*- coding: utf-8 -*-
import sys, json, os, logging, re
reload(sys)  
sys.setdefaultencoding('utf8')

from vtv_task import VtvTask, vtv_task_main
from vtv_utils import execute_shell_cmd
from data_schema import FIELD_SEPARATOR
from vtv_db import get_mysql_connection
from data_schema import get_schema
from utils import make_metacontent_load_query, modify_record, get_metacontent_table_name
from StringUtil import parseMultiValuedVtvString

FILE_SEPARATOR     = '|'
FIELD_CMD = """ egrep -e '%%s' %%s | awk '{ if (substr($0,1,3) == "Gi:") { if (NR > 1) { print "" } ; printf "%s",$0 } else { printf "%s",$0 } } ' >> %%s """
SEPARATOR = '%%%%s%s' % FILE_SEPARATOR
FIELD_CMD = FIELD_CMD % (SEPARATOR, SEPARATOR)

#src_file_name = '/var/tmp/madhav_test/tvseries.data'
FIELD_LIST = ['Gi', 'Ti']
new_list_file = os.path.join('/var/tmp/madhav_test', 'new_list_file.list')

FIELD_MAP   = {'Zp': 'Sp'}
TAG_SECTION = {
                ('Sp', ): {('', ): ['/home/veveo/datagen/LOCAL/sports_data/sport.data'] },
                ('Ge', ): {('', ): ['/data/DATAGEN/MATCHING_DATAGEN/DB/DATA/genre.data','/data/DATAGEN/MATCHING_DATAGEN/ROVI/DATA/genre.data','/data/DATAGEN/MATCHING_DATAGEN/WIKI/DATA/genre.data']},
                ('Cl', ): {('', ): ['/home/veveo/datagen/LOCAL/misc_kg_data/region.data']},
                ('Ll', ): {('', ): ['/home/veveo/datagen/LOCAL/misc_kg_data/language.data']},
                ('Di', 'Pa', 'Pr', 'Wr', 'Zc', 'Co'): {('FRB', ): ('/home/veveo/datagen/current/freebase_data/person.data', ), ('RV', ): ('/home/veveo/datagen/current/rovi_data/person.data', ), ('W', 'PERSON', ): ('/home/veveo/datagen/current/wiki_blended_data/person.data', )},
              }

class Merge(VtvTask):
    
    def __init__(self):
        VtvTask.__init__(self)
        self.init_config(self.options.config_file)
        self.db_name    = "MATCHING_TOOL"
        self.db_ip      = "10.8.24.136"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')
        self.out_dir = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, 'MATCHING_TOOL_LOADER_DATAGEN')
        

    def selected_list(self, pair_list):
        source_to_table_map = {'sport': 'sports', 'team': 'teams'}
        all_vals = []
        sel_qry = 'select guid1, guid2 from verification_%sverificationtable'
        values = self.section.lower()
        values = source_to_table_map.get(values, values)
        self.cursor.execute(sel_qry % values)
        db_data = self.cursor.fetchall()
        db_data = set(db_data)
        for db_d in db_data:
            guid1, guid2 = db_d
            vals = '%s<>%s' %(guid1, guid2)
            all_vals.append(vals)

        file_vals = set()
        for filename in pair_list:
            for pl in self.read_data_lines(filename):
                file_vals.add(pl)
        if self.options.all_guids:
            #loading_pairs = set(all_vals) | file_vals
            loading_pairs = set(all_vals)
        else:
            #loading_pairs = file_vals - set(all_vals)
            loading_pairs = file_vals

        set_of_gid_prefix_to_be_loaded = set()
        gid_set_to_be_loaded =set()
        for loading_pair in loading_pairs:
            for gid in loading_pair.split('<>'):
                prefix, _ = self.get_gid_prefix_and_number(gid) or ''
                if prefix.startswith('W'):
                    prefix = 'W'
                set_of_gid_prefix_to_be_loaded.add(prefix)
                gid_set_to_be_loaded.add(gid)

        return set_of_gid_prefix_to_be_loaded, gid_set_to_be_loaded
        
    def load_gid_to_title(self, gid_title_file_name):
        gid_to_title_dict = {}
        for clv in self.read_data_lines(gid_title_file_name):
            gi, ti = clv.split('|', 1)
            gi     = gi.split(': ', 1)[-1]
            title     = ti.split(': ', 1)[-1]
            if 'eng' in title:
                title = title.split(FIELD_SEPARATOR)
                for ti in title:
                    if 'eng' in ti:
                        gid_to_title_dict[gi] = ti

        return gid_to_title_dict

    def generate_gid_field_info(self, record, fields, gid_to_title_dict):
        for field in fields:
            gid_titles = []
            output_values = []
            val = record[self.schema[field]]
            if not val:
                continue
            print val
            if field == 'Pa':
                for va in val.split('<<>>'):
                    gid = va.strip('<>') 
                    output = gid_to_title_dict.get(gid, gid)
                    output_values.append(output + '<>')
                record[self.schema[field]] = '<<>>'.join(output_values)
            else:
                for gid in parseMultiValuedVtvString(val, only_titles=True):
                    output = gid_to_title_dict.get(gid, gid)
                    output_values.append(output)
                record[self.schema[field]] = FIELD_SEPARATOR.join(output_values)
        print record
        return record

    def set_options(self):
        config_file = os.path.join(self.system_dirs.VTV_ETC_DIR, 'matching_tool.cfg')
        self.parser.add_option("", '--config-file', default=config_file, help='config file to be used')
        self.parser.add_option("", '--data-type', default='', help='Section in the config file')
        self.parser.add_option("", '--all-guids', default=False, metavar="BOOL", action="store_true",help='If you want to take the diff')
        self.parser.add_option("", '--manual-config', default=None, help='config file to be used')

    def list_files(self):
        self.section = self.options.data_type.upper()
        file_list = {}
        pair_list = []
        if not self.options.manual_config:
            input_file = self.get_config_value('DATA_FILES_MAP', self.section).split(',')
        else:
            self.init_config(self.options.manual_config)
            input_file = self.get_config_value('DATA_FILES_MAP', self.section).split(',')
        for in_file in input_file:
            prefix, dir_name, filename, file_type = in_file.split(FIELD_SEPARATOR)
            filename = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, dir_name, filename)
            file_list.update({prefix: filename})

        self.init_config(self.options.config_file)
        pair_file = self.get_config_value('GUID_MERGE_FILES', self.section).split(',')
        for pair in pair_file:
            dir_name, filename, file_type = pair.split(FIELD_SEPARATOR)
            filename = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, dir_name, filename)
            pair_list.append((filename)) 
        return file_list, pair_list
        

    def dump_gid_title(self, file_list, dst_file_name):
        '''Gi and Ti from the main data file'''
        field_pattern = '|'.join([ '^%s:' % f for f in FIELD_LIST])
        for filename in file_list:
            cmd = FIELD_CMD % (field_pattern, filename, dst_file_name)
            self.run_cmd(cmd)
            fp = open(dst_file_name, 'a')
            fp.write('\n')
            fp.close()

    def run_main(self):
        self.section = self.options.data_type.lower()
        table_name = 'verification_%smetacontent' %(self.section)
        gid_prefix_to_file_list, pair_list = self.list_files()
        set_of_gid_prefix_to_be_loaded, gid_set_to_be_loaded = self.selected_list(pair_list)
        if 'W' in set_of_gid_prefix_to_be_loaded:
            crew_gid_prefix = ('W', 'PERSON')

        dump_file_set = set()
        gid_title_file_name = os.path.join(self.out_dir, 'gid_title.txt')
        open(gid_title_file_name, 'w').close()
        for key, val in TAG_SECTION.iteritems():
            dump = False
            for field in key:
                if (field == 'Sp'):
                    continue
                else:
                    dump = True
            if dump:
                for prefix_tuple, file_tuple in val.iteritems():
                    if (('', ) == prefix_tuple):
                        dump_file_set.update(file_tuple)
                    else:
                        for prefix in prefix_tuple:
                            if prefix in set_of_gid_prefix_to_be_loaded:
                                dump_file_set.update(file_tuple)
        self.dump_gid_title(dump_file_set, gid_title_file_name)

        self.counter = 0
        fields = self.get_config_value('FIELD_LIST', self.section).split(',')
        
        file_list_to_be_loaded = []
        if '' in set_of_gid_prefix_to_be_loaded:
            #load all files present in gid_prefix_to_file_list
            pass
        else:
            for gid_prefix in set_of_gid_prefix_to_be_loaded:
                file_list = gid_prefix_to_file_list.get(gid_prefix)
                if file_list:
                    file_list_to_be_loaded.append(file_list)
                else:
                    import pdb;pdb.set_trace()

        gid_to_title_dict = self.load_gid_to_title(gid_title_file_name)
        for file_ in file_list_to_be_loaded:
            some_dict = {}
            self.schema = get_schema(['Gi', 'Ti', 'Ak', 'Oa', 'Ol', 'Ep', 'Ae', 'Di', 'Pa', 'Ge', 'Cl', 'Ll', 'Sp', 'Mi'])
            va = []
            for record in self.read_data_file(file_, 'generic', self.schema):
                gid = record[self.schema['Gi']]
                if gid in gid_set_to_be_loaded:
                    record      = self.generate_gid_field_info(record, fields, gid_to_title_dict)
                    json_record = modify_record(record, self.schema)
                    query       = make_metacontent_load_query(gid, json.dumps(json_record), table_name)
                    self.cursor.execute(query)
                

        self.conn.commit()
        self.cursor.close()
        print file_

if __name__ == '__main__':
    vtv_task_main(Merge)
