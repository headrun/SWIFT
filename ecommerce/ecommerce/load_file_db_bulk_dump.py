#!/usr/bin/env python
import os
import glob
from ast import literal_eval
from configparser import ConfigParser

CFG = ConfigParser()
CFG.read('source_names.cfg')

class LoadFiles():
    def __init__(self):
        self.path = os.path.join(os.getcwd(), "spiders/OUTPUT/processing")
        self.processed = os.path.join(os.getcwd(), "spiders/OUTPUT/processed")
        self.indian_sources = literal_eval(CFG.get('IND', 'sources'))
        self.indian_db = CFG.get('IND', 'db')
        self.us_sources = literal_eval(CFG.get('US', 'sources'))
        self.us_db = CFG.get('US', 'sources')
        self.main()

    def ensure_files_exists(self):
        files = glob.glob(os.path.join(self.path, '*.queries'))
        if len(files) != 0:
            return files
        else:
            return []

    def move_file_crawled(self, file_nane):
        os.chdir(self.path)
        q_mv = "mv %s %s" % (file_nane, self.processed)
        os.system(q_mv)

    def dump_file_into_db(self, query_file, table):
        source = query_file.split('/')[-1].split('_')[0]
        if source in self.indian_sources:
            database = self.indian_db

        elif source in self.us_sources:
            database = self.us_db

        cmd = 'mysql -uroot -pEcomm@34^$ -hlocalhost -A %s --local-infile=1 -e "%s"'
        query = "LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s CHARACTER SET utf8 FIELDS TERMINATED BY '#<>#'" % (query_file, table)
        if 'info' in table:
            query += "SET created_at=NOW(), modified_at=NOW();"
        else:
            query += "SET created_at=NOW();"

        try:
            os.system(cmd % (database, query))
            return True
        except:
            print("%s fail to load" % query_file)
            return False

    def main(self):
        files = self.ensure_files_exists()
        for _file in files:
            fi_name = _file.split('/')[-1]
            if 'insights' in fi_name:
                table = 'products_insights'
            else:
                table = 'products_info'
            status = self.dump_file_into_db(_file, table)
            if status:
                self.move_file_crawled(fi_name)


if __name__ == '__main__':
    LoadFiles()
