#!/usr/bin/env python
import os
import glob

class LoadFiles(object):
    def __init__(self):
        self.path = os.path.join(os.getcwd(), "spiders/OUTPUT/processing")
        self.processed = os.path.join(os.getcwd(), "spiders/OUTPUT/processed")
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
        cmd = 'mysql -uroot -phdrn59! -hlocalhost -A EBAY --local-infile=1 -e "%s"'
        query = "LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s CHARACTER SET utf8 FIELDS TERMINATED BY '#<>#'"   % (query_file, table)
        #query += " SET created_at=NOW(), modified_at=NOW();"
        try:
            os.system(cmd % query)
            return True
        except:
            print("%s fail to load" % query_file)
            return False

    def main(self):
        files = self.ensure_files_exists()
        for _file in files:
            fi_name = _file.split('/')[-1]
            if 'ebay_browse' in fi_name:
                table = 'ebay_sold_items'
            status = self.dump_file_into_db(_file, table)
            if status:
                self.move_file_crawled(fi_name)


if __name__ == '__main__':
    LoadFiles()
