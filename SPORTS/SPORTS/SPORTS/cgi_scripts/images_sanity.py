#!/usr/bin/env python

import sys, os
from datetime import date, datetime, timedelta
import MySQLdb
import jinja2
import codecs

from vtv_utils import copy_file, get_latest_file
from vtv_task import VtvTask, vtv_task_main
from vtv_user import get_vtv_user_name_and_password
import ssh_utils
import html_markup
import StringUtil, foldFileInterface

REPORT_DIR  = '/data/REPORTS/SANITY_REPORTS/IMAGES_SANITY/REPORT_DIR'
LOG_DIR     = '/data/REPORTS/SANITY_REPORTS/IMAGES_SANITY/LOG_DIR'
date_now = date.today()
cur_date = date_now.strftime("%Y-%m-%d")

class CrawlerSanity(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        my_name = 'IMAGES_SANITY'
        self.OUT_DIR    = self.OUT_DIR = os.path.join(LOG_DIR, my_name)
        self.init_config(self.options.config_file)
        self.datatestdb_ip   = eval(self.get_default_config_value('DATATESTDB_IP'))
        self.datatestdb      = eval(self.get_default_config_value('DATATESTDB_NAME'))
        self.db_ip      = eval(self.get_default_config_value('DB_IP'))
        self.imagedb    = eval(self.get_default_config_value('IMAGEDB_NAME'))
        self.datatestdb_sources      = eval(self.get_config_value('SOURCES', 'DATATESTDB'))
        self.imagedb_sources    = eval(self.get_config_value('SOURCES', 'IMAGEDB'))
        self.gid_image_meta_id_map  = {}
        self.id_image_map   = {}
        self.id_title_map = {}
        self.missing_gids_file = open('missing_gids_file', 'w')
        self.missing_gim_gids_file = open('missing_gim_gids_file', 'w')
        self.all_missing_gids_file = open('all_gids_missing_file', 'w')

    def dump_report(self):
        test_report_file_format = 'IMAGES_SANITY.html' 
        test_report_file_name = test_report_file_format + '_%s.html' %  cur_date
        test_report_file_name = os.path.join(REPORT_DIR, test_report_file_name)

        return test_report_file_name, test_report_file_format

    def get_latest_log(self):
        log_file_prefix = '%s/IMAGES_SANITY/logs_images_sanity_%s' % (LOG_DIR, cur_date) 
        self.latest_log_file =  os.path.join(log_file_prefix, get_latest_file('images_sanity*log', self.logger))

    def datatestdb_stats(self):
        self.open_cursor(self.datatestdb_ip, self.datatestdb)
        self.final_stats_map = {}
        self.all_test_cases_gid_map = {}
        s_query = "select distinct(suite_name) from test_suites"
        self.cursor.execute(s_query)
        self.suite_names  = self.cursor.fetchall()
        #print self.suite_names

        for suite_name in self.suite_names:
            test_cases_gid_map = {}
            id_suitemap = {}
            query = "select id, suite_name, title from test_suites where suite_name=%s"
            values = (suite_name)
            self.cursor.execute(query, values)
            test_suites = self.cursor.fetchall()
            for row in test_suites:
                id, suite_name, title = row
                id_suitemap.setdefault(suite_name, []).append((id, title))
                self.id_title_map[id] = title
            for name, val in id_suitemap.iteritems():
                for item in val:
                    id, title   = item 
                    query = "select suite_id, record from test_cases where suite_id = %s" %int(id)
                    self.cursor.execute(query)
                    data = self.cursor.fetchall()
                    for row in data:
                        suite_id, rec = row
                        guid = rec.split('#<>#')[0]
                        test_cases_gid_map.setdefault(suite_id, []).append(guid)

            self.all_test_cases_gid_map[suite_name] = test_cases_gid_map
    	self.close_cursor()

    def imagedb_stats(self):
        self.open_cursor(self.db_ip, self.imagedb)
        query = "select gid, image_meta_id from gid_image_map"
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        for row in data:
            gid, image_meta_id = row
            self.gid_image_meta_id_map.setdefault(gid, []).append(image_meta_id)

        query = "select gid, title from gid_meta"
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        for row in data:
            id, image_url = row
            self.id_image_map.setdefault(id, image_url)

        self.close_cursor()

    def set_options(self):
        chinese_config = os.path.join(os.path.abspath(os.curdir), 'images.cfg')
        self.parser.add_option("-c", "--config-file", default = chinese_config, help = "config file")

    def get_stats(self):
        for suite_name, test_cases_gid_map in self.all_test_cases_gid_map.iteritems():
            missing_gids_dict = {}
            final_dict = {}
            log_dict = {}
            for category, guids in test_cases_gid_map.iteritems():
                image_meta_ids = set()
                #print category, len(guids)
                gids_not_in_imagedb = set(guids) - set(self.gid_image_meta_id_map.keys())
                for gid in gids_not_in_imagedb:
                    self.all_missing_gids_file.write('%s\n' % gid)
                    self.all_missing_gids_file.flush()

                gids_in_imagedb = list(set(guids).intersection(set(self.gid_image_meta_id_map.keys())))
                #print "Total: %s Intersection %s" % (len(set(guids)), len(gids_in_imagedb))
                for val in gids_in_imagedb:
                    image_meta_ids.add(self.gid_image_meta_id_map[val][0])

                #print "Verifying>>", len(image_meta_ids), len(self.gid_image_meta_id_map.keys())
                images_not_in_image_db = set(gids_in_imagedb) - set(self.id_image_map.keys())
                images_not_present_in_gid_meta_table = set(guids) - set(self.id_image_map.keys())
                images_in_gid_meta_table = list(set(guids).intersection(set(self.id_image_map.keys())))
                images_not_present_in_gim = set(images_in_gid_meta_table) - set(self.gid_image_meta_id_map.keys())
                #print ">>>>>>>>>>>>>>>>>", images_not_present_in_gim
                images_not_in_gim_not_in_gm = set(images_not_in_image_db) - set(self.id_image_map.keys())
                missing_images = images_not_present_in_gid_meta_table - images_not_in_gim_not_in_gm
                #import pdb; pdb.set_trace()
                all_missing_images = len(images_not_in_gim_not_in_gm) + len(missing_images)
                if images_not_present_in_gim:
                    all_missing_images = all_missing_images + len(images_not_present_in_gim)
                #print "ALL>>>>>>", len(all_missing_images)
                for gim_gid in list(images_not_present_in_gim):
                    self.missing_gim_gids_file.write('%s\n' % (gim_gid))
                    self.missing_gim_gids_file.flush()
                #print "1>>>", len(images_not_present_in_gid_meta_table), self.id_title_map.get(category, '')
                missing_gids_dict.setdefault(category, []).append((len(gids_not_in_imagedb), len(images_not_in_image_db)))
                #print "%s Gids missing in %s Category" % (len(gids_not_in_imagedb), self.id_title_map[category])
                final_dict[self.id_title_map[category]] = (len(gids_not_in_imagedb), len(images_not_present_in_gid_meta_table), all_missing_images)
                cat_name =  "Suite_name:" + suite_name + ", Category:" + self.id_title_map.get(category, '')
                log_dict[cat_name] = 'Missing Gids::' + '%s' % list(images_not_present_in_gid_meta_table)
                for m_gid in list(images_not_present_in_gid_meta_table):
                    self.missing_gids_file.write('%s \n' % (m_gid))
                    self.missing_gids_file.flush()
                #print "2>>>", len(list(images_not_present_in_gid_meta_table))
            self.logger.info('%s' % (log_dict))
            self.final_stats_map[suite_name] = final_dict

        #print "Final stats map>>", self.final_stats_map
 

    def run_main(self):
        self.get_latest_log()
        test_report_file_names, latest_report_file_format = self.dump_report()
        copy_file(test_report_file_names, latest_report_file_format, self.logger)
        #print self.latest_log_file
        self.latest_log_file = self.latest_log_file.replace('/home/', '/')
        testdb_records = self.datatestdb_stats()
        imagedb_records = self.imagedb_stats()
        self.get_stats()
        full_info = {}
        new_info = []
        for suite_name, final_dict in self.final_stats_map.iteritems():
            big_list = []
            for cat, count in final_dict.iteritems():
                if count[0] != 0 or count[1] != 0:
                    #print "Count2>:>>>>>>0>>>>>", count[2], count[0]
                    big_list.append((cat, count[0], count[1], count[2]))
                    new_info.append((suite_name, cat, count[0], count[1], count[2]))

        new_info = sorted(new_info, key=lambda x:x[4])
        THIS_DIR = os.path.dirname(os.path.abspath('sports_datagen/'))
        jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader(THIS_DIR))
        table_html = jinja_environment.get_template('images_sanity.jinja').render(today_date = datetime.now(), 
                                                        full_info = full_info, big_list = big_list,new_info = new_info, 
                                                        log_file = self.latest_log_file)
        codecs.open(os.path.join(REPORT_DIR, 'IMAGES_SANITY.html'), 'w', 'utf8').write(table_html)

    def cleanup(self):
        self.move_logs(self.OUT_DIR, [ ('.', 'images_sanity*log'), ])
        self.remove_old_dirs(self.OUT_DIR, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

if __name__ == '__main__':
    vtv_task_main(CrawlerSanity)
    sys.exit(0)
