#!/usr/bin/env python
# $Id: dumpToDb.py,v 1.1 2016/03/23 12:50:45 headrun Exp $
import os, sys
from vtv_task import VtvTask, vtv_task_main
from sqlUtil import SqlUtil
import MySQLdb
from vtv_utils import execute_shell_cmd

class VtvDumpToDb(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.out_dir = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, "RECO_DUMP_TO_DB")
        self.data_format = 'm'
        self.input_dir = self.options.input_dir
        self.movie_input = "DATA_MC_FILE"
        self.tvseries_input = "DATA_MC_FILE"
        self.initialize()

        self.db_name = "RECO_DUMP"
        self.sql_obj = SqlUtil(self.db_name, self.logger)
        self.sql_obj.initializeConnection()

    def initialize(self):
        self.logger.info(self.options)
        if self.options.operator in ('vtv_seed', 'vtv'):
            self.data_format = 's'
            self.movie_input = "movie.data.gen.merge"
            self.tvseries_input = "tvseries.data.gen.merge"
        if not self.input_dir:
            if self.options.operator in ('skyb', 'pa', 'dtv'):
                self.logger.error("Input directory not provided for operator %s"%(self.options.operator))
                self.makeExit()
            self.logger.warning("Input directory not provided. Considering current seed as input dir.")
            self.input_dir = self.datagen_dirs.vtv_seed_dir
        self.pop_file = os.path.join(self.input_dir, 'DATA_POP.BASE') 
        self.guid_merge_file = os.path.join(self.input_dir, 'guid_merge.list')

    def generateSqlFile(self):
        #Movie
        movie_input_file = os.path.join(self.input_dir, self.movie_input)
        self.logger.info("Generating movie sql file")
        cmd = "python genEpgBasedSimilarProgramsDistributed.py -i %s -o related_movie -t movie --input-type %s --pop-file %s -m related_movie.marshal -g %s -r rating_distance_map -O movie_sql_dump"%(movie_input_file, self.data_format, self.pop_file, self.guid_merge_file)
        status, output = execute_shell_cmd(cmd)
        if status:
            self.logger.error("Movie Sql dum Fail")
            self.makeExit()
        #TVseries
        tvseries_input_file = os.path.join(self.input_dir, self.tvseries_input)
        self.logger.info("Generating tvseries sql file")
        cmd = "python genEpgBasedSimilarProgramsDistributed.py -i %s -o related_tvseries -t tvseries --input-type %s --pop-file %s -m related_tvseries.marshal -g %s -r rating_distance_map -O tvseries_sql_dump"%(tvseries_input_file, self.data_format, self.pop_file, self.guid_merge_file)
        status, output = execute_shell_cmd(cmd)
        if status:
            self.logger.error("TVseries Sql dump Fail")
            self.makeExit()
        self.logger.info("Sql Dump generation completed")

    def prepare_database(self):
        #Creating and truncating table
        if self.options.operator == 'vtv_seed':
            table_name = self.options.operator
            self.sql_obj.clearTable(table_name)
        else:
            table_name = self.options.operator+'_'+self.options.day
            table_schema = "(GID  CHAR(100) NOT NULL, VAL CHAR(100), REC_TYPE CHAR(40))"
            self.sql_obj.checkAndCreateTable(table_name, table_schema)        
        #upload movie data to table
        delimiter = "#<>#"
        dump_file = os.path.join(os.getcwd(), "movie_sql_dump")
        self.sql_obj.insertData(dump_file, table_name, delimiter)
        #upload tv data to table
        delimiter = "#<>#"
        dump_file = os.path.join(os.getcwd(), "tvseries_sql_dump")
        self.sql_obj.uploadData(dump_file, table_name, delimiter)
       
    def makeExit(self):
        self.cleanup()
        sys.exit()
         
    def cleanup(self):
        #Moving test case files to logs dir 
        file_pats = [('.', '*.%s' % x) for x in ('log',)]
        file_pats += [('.', '*_%s' % x) for x in ('sql_dump',)]
        self.move_logs(self.out_dir, file_pats)

    def set_options(self):
        self.parser.add_option("-o", "--operator", default = 'vtv_seed', help = "skyb, pa, dtv, vtv")
        self.parser.add_option("-D", "--day", default = 'latest', help = "Day, whose data going to be uploaded")
        self.parser.add_option("-i", "--input-dir", default = '', help = "Input data dir")

    def run_main(self):
        self.call_function(self.generateSqlFile)
        self.call_function(self.prepare_database)

if __name__ == '__main__':
    vtv_task_main(VtvDumpToDb) 
