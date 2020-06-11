import sys
import os
import re
import traceback
import datetime, collections
from datetime import timedelta

from vtv_utils import VTV_CONTENT_VDB_DIR, copy_file, execute_shell_cmd
from vtv_task import VtvTask, vtv_task_main
from vtv_db import get_mysql_connection
from data_schema import get_schema

import genericFileInterfaces
import foldFileInterface
from guidPairLoaderUtils import guidloadGuidPairList

class IsoPopulation(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.sports_db_ip = '10.4.18.183'
        self.sports_db = 'SPORTSDB'

    def set_options(self):
        self.parser.add_option('-c', '--country', default='', help='country')
        self.parser.add_option('-i', '--iso_input', default='', help="iso")
        self.parser.add_option('-n', '--normalize', default='', help="iso")

    def populate_iso(self, country, iso_input):

        self.open_cursor(self.sports_db_ip, self.sports_db)
        iso_qry = 'update sports_locations set iso = %s where country = %s and iso =""'
        iso_vals = (iso_input, country)
        self.cursor.execute(iso_qry, iso_vals)

    def normalize_iso(self):

        iso_dict = {}
        self.open_cursor(self.sports_db_ip, self.sports_db)
        iso_qry = 'select country, iso from sports_locations where iso !="" group by country'
        self.cursor.execute(iso_qry)
        recs = self.cursor.fetchall()
        for rec in recs:
            con = rec[0]
            _iso = rec[1]
            iso_dict[con] = _iso
        empt_iso_qry = 'select country from sports_locations where country !="" and iso=""'
        self.cursor.execute(empt_iso_qry)
        empt_rows = self.cursor.fetchall()
        for row in empt_rows:
            con = row[0]
            try:
                fi_iso = iso_dict[con]
            except:
                fi_iso = ''
                continue
            up_qry = 'update sports_locations set iso = %s where iso = "" and country = %s'
            up_vals = (fi_iso, con)
            self.cursor.execute(up_qry, up_vals)

    def run_main(self):
        if self.options.normalize == "refresh":
            self.normalize_iso()
        elif self.options.country and self.options.iso_input:
            self.populate_iso(self.options.country, self.options.iso_input)
        else:
            print 'INPUT IS NOT CLEAR.....!!!'

if __name__ == '__main__':
    vtv_task_main(IsoPopulation)
    sys.exit( 0 )
