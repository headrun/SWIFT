#!/usr/bin/env python

from sqlUtil import SqlUtil
import os, sys
import optparse

#sys.path.insert(0, "/home/veveo/release/server")
sys.path.insert(0, "/home/amit-kumar/related_20130905/pratham/build/vkc/release/server")
from vtv_utils import initialize_logger, get_compact_traceback, find_and_kill_process, execute_shell_cmd

class RelatedDiffTool:
    def __init__(self, db_name, data_tables, data_locations, data_format, logger):
        self.logger = logger
        self.data_dirs = data_tables.split(',')
        self.data_locations = data_locations
        self.db_name = db_name
        self.data_format = data_format
        if not self.data_format:
            self.data_format = 'Sk'
            if db_name == 'vtv':
                self.data_format = 'Gi'
                
#self.default_rel_location = "/data/RELATED/rel_output"
        self.default_rel_location = "/home/amit-kumar/data/rel_output/skyb"
        self.sql_util_obj = SqlUtil(self.db_name, self.logger)
        self.tables = {}
        self.init_db()
        self.init_table_info()

    def init_table_info(self):
        # getting table and data file name
        table_datas = self.tables.setdefault(self.db_name, set())
        if self.data_locations:
            locations = set(self.data_locations.strip().split(','))
            for location in locations:
                table_datas.add(location.split(':'))

        if not table_datas:
            rel_out_file = "DATA_SIMILAR.tribuneID.with_score"
            if self.data_format == 'Gi':
                rel_out_file = "DATA_SIMILAR"
            for data_dir in self.data_dirs:
                data_file = os.path.join(self.default_rel_location, data_dir, rel_out_file)
                table_datas.add((data_dir, data_file))

    def init_db(self):
        # checking and creating database
        #print "creating database %s"%(self.db_name)
        self.sql_util_obj.checkAndCreateDatabase()
        # creating database connection
        self.sql_util_obj.initializeConnection()
    
    def init_tables(self):
        # Create tables and upload related data
        table_datas = self.tables.get(self.db_name, set())
        for table_data in table_datas:
            self.create_table_and_upload_data(table_data)   

    def create_table_and_upload_data(self, table_data):
        table_name, data_file = table_data[0].strip(), table_data[1].strip()
        table_name = '%s_table'%(table_name) 
        print "Creating table %s"%(table_name)
        table_schema = '(GID  CHAR(20) NOT NULL, TITLE CHAR(40), RELATED LONGTEXT)'
        if data_file and os.path.exists(data_file):
            #creating database load compatible file
            db_file = self.create_db_compatible_file(data_file)
            #creating table, if already present then truncating it
            self.sql_util_obj.checkAndCreateTable(table_name, table_schema)
            #uploading data in db table
            self.sql_util_obj.insertData(db_file, table_name, '#')
        else:
            self.logger.info("Data file %s not present. Cant get realted for this"%(data_file))
            return False

    def get_line_info(self, line):
        ''' gid, title and related in '|' separated 'gid|title|score '''
        src_gid, src_title, related = '', '', ''     
        if self.data_format == 'Sk':
            src_gid, src_title, related = line.split('|', 2)
        else:
            import re
            gid_format = re.compile(r'(?P<gid>.*?)#{(?P<score>[0-9]*?)}')
            data = line.split('#<>#')
            src_gid = data[0].lstrip('Gi: ').strip()
            src_title = data[1].lstrip('Ti: ').strip()            
            rel_data = data[2].lstrip('Rl: ').strip()
            rel_list = []
            for rel in rel_data.split('<>'):
                match_obj = gid_format.match(rel)
                rel_gid = match_obj.group('gid')
                rel_score = match_obj.group('score')
                rel_list.extend([rel_gid, '', rel_score])
            related = '|'.join(rel_list)    
        return src_gid, src_title, related

    def create_db_compatible_file(self, data_file):       
        db_file = "/tmp/%s.db"%(os.path.basename(data_file))
        df = open(db_file, 'w')
        f = open(data_file)
        for line in f:
            line = line.strip()
            if line:
                src_gid, src_title, related = self.get_line_info(line)
                db_line = "%s\n"%('#'.join([src_gid.strip(), src_title.strip(), related.strip()]))       
                df.write(db_line)
        f.close()
        df.close()
        return db_file       

    def generate_rel_color_level(self, all_rel_set, table_rel_dict):
        tables = sorted(table_rel_dict.keys())
        rel_color_map = {}
        for rel in all_rel_set:
            table_list = []
            for table in tables:
                rel_set = table_rel_dict[table]
                if rel in rel_set:
                    table_list.append(table)        
            rel_color_map[rel] = '<>'.join(table_list)   
        return rel_color_map    
            
    def generateDifferenceReport(self, datas):
        gid_title_map = {}
        table_rel_dict = {}
        all_rel_set = set()
        for table, related in datas.iteritems():
            rel = related.split('|')
            rel_list = table_rel_dict.setdefault(table, [])
            for i in range(len(rel)/3):
                i = 3*i
                rel_gid, rel_title = rel[i], rel[i+1]
                rel_list.append(rel_gid)
                gid_title_map[rel_gid] = rel_title
            all_rel_set.update(set(rel_list))
        rel_color_map = self.generate_rel_color_level(all_rel_set, table_rel_dict)        
        return (table_rel_dict, rel_color_map, gid_title_map)    
                    
    def gererateRelatedComparision(self, gid):
        datas = {}
        tables = self.tables.get(self.db_name, set())
        for table in tables:
            table_name = "%s_table"%(table[0])
            result = self.sql_util_obj.selectDataByGid(table_name, gid)
            if len(result) > 1:
                self.logger.error("More than 1 entry for gid %s in table %s. Cannot generate any difference for this. Continuing..."%(gid, table))
                continue
            if len(result) == 0:
                continue    
            datas[table[0]] = result[0][2]
        #generate difference report
        return self.generateDifferenceReport(datas)   

    def extract_related(self, related):
        rel_list = []
        rel = related.split('|')
        for i in range(len(rel)/3):
            rel_gid = rel[3*i]
            rel_list.append(rel_gid)
        return rel_list    

    def generateStatsReport(self, datas):
        table_rel_map = {}
        for table in datas:
            gid_rel_dict = table_rel_map.setdefault(table, {})    
            for entry in datas[table]:
                gid, title, related = entry
                rel_list = self.extract_related(related)
                gid_rel_dict[gid] = rel_list
        diff_gids = {}
        tables = datas.keys()
        diff_gids[tables[0]] = set(table_rel_map[tables[0]].keys()) - set(table_rel_map[tables[1]].keys())
        diff_gids[tables[1]] = set(table_rel_map[tables[1]].keys()) - set(table_rel_map[tables[0]].keys())
        common_gids = set(table_rel_map[tables[1]].keys()).intersection(set(table_rel_map[tables[0]].keys()))
        not_same_rels_dict = {}
        not_same_order_rels = set()
        for table in datas:
            gids = set(table_rel_map[table].keys())
            common_gids = common_gids.intersection(gids)
        for gid in common_gids:      
            all_list = []
            for table in table_rel_map:
                rel_list = table_rel_map[table][gid]
                all_list.append(rel_list)
            #Assuming only 2 data will be compared
            if not set(all_list[0]) == set(all_list[1]):
                diff = max(len(set(all_list[0]) - set(all_list[1])), len(set(all_list[1]) - set(all_list[0])))
                diff_key = min(diff/5, 4)
                not_same_rels = not_same_rels_dict.setdefault(diff_key, set()) 
                not_same_rels.add(gid)
            elif not all_list[0] == all_list[1]:
                not_same_order_rels.add(gid)
        return common_gids, diff_gids, not_same_rels_dict, not_same_order_rels        
                
    def generateRelatedStats(self):
        datas = {}
        tables = self.tables.get(self.db_name, set())
        for table in tables:
            table_name = "%s_table"%(table[0])
            result = self.sql_util_obj.selectAll(table_name)
            datas[table[0]] = result
        return self.generateStatsReport(datas)    
    
def main(logger, options):
    rdt = RelatedDiffTool(options.data_type, options.data_dir, options.data_location, options.data_format, logger)
    rdt.init_db()
    rdt.gererateRelatedComparision('')

if __name__ == '__main__':
    #Define options
    default_logger = os.path.splitext(sys.argv[0])[0] + os.path.extsep + 'log'
    parser = optparse.OptionParser()
    parser.add_option('', '--data-type', default='vtv', help='vtv, skyb, pa, dtv')
    parser.add_option('', '--data-dir', default='', help='20130901, 20130902 ...Comma separated')
    parser.add_option('', '--data-location', default='', help='table1:filepath1, table2:filepath2 ')
    parser.add_option('', '--data-format', default=None, help='Sk, Gi')

    logger = initialize_logger()
    #Parse options
    options, args = parser.parse_args()
    try:
        main(logger, options)
    except Exception, e:
        print "Exception: %s" % get_compact_traceback(e)
        sys.exit(2)
