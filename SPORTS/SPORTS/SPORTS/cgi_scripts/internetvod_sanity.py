#!/usr/bin/env python

import codecs, datetime, os

import jinja2

from vtv_task import VtvTask, vtv_task_main
from vtv_utils import get_latest_file

COLUMN_TYPE_HASH = {
    'release_year'  :   'int',      'release_date'  : 'date',   'us_release_date'       :   'date',     'dvd_release_date'  : 'date',   'theatre_release_date'  : 'date',
    'season_number' :   'int',      'episode_number': 'int',    'season_episode_number' :   'int',      'duration'          : 'int',
}

WRONG_COLUMN_NAMES_HASH = {
    'theatrical_release_date'   : 'theatre_release_date',   'release_time'  : 'release_date',   'date_released' : 'release_date',   'released'      : 'release_date', 
    'theater_release_date'      : 'theatre_release_date',   'season_num'    : 'season_number',  'season_no'     : 'season_number',  'episode_num'   : 'episode_number',
    'episode_no'                : 'episode_number',         'run_time'      : 'duration',       'runtime'       : 'duration',
}

class InternetvodSanity(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.mysql_ip = '10.4.2.207'
        self.db_name = 'INTERNETVOD'
        my_name = 'INTERNETVOD_SANITY'
        base_dir = os.path.join(self.system_dirs.VTV_REPORTS_DIR, 'SANITY_REPORTS')
        self.REPORTS_DIR = self.OUT_DIR = os.path.join(base_dir, my_name)
        self.log_file_pattern = '%s*.log' % self.script_prefix
        self.errors = []

    def check_column_names_types(self):
        base_query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s'" % self.db_name
        column_names = COLUMN_TYPE_HASH.keys() + WRONG_COLUMN_NAMES_HASH.keys()
        column_str = ','.join(["'%s'" % x for x in column_names])
        query = "%s AND COLUMN_NAME IN (%s)" % (base_query, column_str)
        self.cursor.execute(query)
        for row in self.cursor.fetchall():
            table_name, column_name, column_type = row
            expected_column_name = WRONG_COLUMN_NAMES_HASH.get(column_name, column_name)
            expected_column_type = COLUMN_TYPE_HASH[expected_column_name]
            if expected_column_name != column_name or expected_column_type != column_type:
                self.errors.append((table_name, column_name, column_type, expected_column_name, expected_column_type))

    def get_latest_log_file_link(self):
        log_file_link = '/REPORTS/SANITY_REPORTS/INTERNETVOD_SANITY/%s' % self.logs_dir_name
        return os.path.join(log_file_link, get_latest_file(self.log_file_pattern, self.logger))

    def write_jinja(self):
        jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader(os.getcwd()))
        table_html = jinja_environment.get_template('%s.jinja' % self.script_prefix).render(
            today_date = datetime.datetime.now(), sanity_count = len(self.errors), log_file = self.get_latest_log_file_link(), errors = self.errors
        )
        codecs.open(os.path.join(self.REPORTS_DIR, 'INTERNETVOD_SANITY_REPORT.html'), 'w', 'utf8').write(table_html)

    def run_main(self):
        self.open_cursor(self.mysql_ip, self.db_name)
        self.check_column_names_types()
        self.close_cursor()
        self.write_jinja()

    def cleanup(self):
        self.move_logs(self.OUT_DIR, [ ('.', self.log_file_pattern), ])
        self.remove_old_dirs(self.OUT_DIR, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


if __name__ == '__main__':
    vtv_task_main(InternetvodSanity)
