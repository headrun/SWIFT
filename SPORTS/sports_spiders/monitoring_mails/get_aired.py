from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2, VTV_DATAGEN_CURRENT_DIR
from ssh_utils import scp
import foldFileInterface
from datetime import datetime, timedelta
from vtv_task import VtvTask, vtv_task_main
import time
import sys
import os, glob
import re

CUR_DIR       = os.getcwd()
DATAGEN_DIR   = os.path.join(CUR_DIR, 'datagen_test')
ERROR_PATTERN = 'FIELD: Ry'
GID       = 'GID: (.*?):'
GOT       = 'GOT: (.*?) '
NEED      = 'NEED: (.*?) '
FIELDS    = [ 'Gi', 'Ti', 'Ak', 'Ik', 'Va', 'Ll',
              'Ca', 'Ci', 'Vt', 'Rr', 'Iv', 'Ge',
              'Cl', 'Sy', 'Di', 'Pr', 'Wr', 'Er',
              'Ry', 'Od', 'Ho' ]
DELEM     = '<>'
SEPERATOR = '|'

VT_LIST    = ['movie', 'tv show', 'tv episode']

FIRST_AIRED = 'first_aired'

INFO_FIELDS    = ['Gi', 'Ti', 'Rd']
ENTERTAINMENT_FILE = 'DATA_WIKIPEDIA_ENTERTAINMENT_MC_FILE'
ENTERTAINMENT_FILE_NEW = 'DATA_WIKIPEDIA_ENTERTAINMENT_MC_FILE_NEW'
INFOBOX_FILE = 'DATA_WIKIPEDIA_IMDB_INFOBOX_FILE'

DATE1 = '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$'
YEAR1 = '^([0-9]{4})-[0-9]{1,2}-[0-9]{1,2}$'

CUR_DATE      = str(datetime.utcnow().date())
YESTERDAY     = str((datetime.utcnow() - timedelta(days=1)).date())


def check_dir(path):
    if not os.path.isdir(path):
        os.mkdir(path)

def get_table_data(title, headers, body):
    text = '<br /><br /><b>%s</b><br /><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % title
    for header in headers:
        text += '<th>%s</th>' % header
    text += '</tr>'

    for data in body:
        text += '<tr>'
        for d in data:
            text += '<td>%s</td>' % d
        text += '</tr>'
    text += '</table>'
    return text

class DategenRd(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.text             = ''
        self.failures         = {}
        self.aired            = {}
        self.infobox          = {}
        self.ry_missing       = {}
        self.pg_types_count   = {}
        self.types_count      = {}
        self.mry_aod          = {}
        self.missing_rd       = []
        self.skipped_info     = set()
        self.vt_fields        = set()
        self.all_fields       = set()
        self.log_ip           = "10.4.2.187"
        if self.options.lang:
            self.lang         = self.options.lang
        else:
            self.lang         = 'eng'
        self.data_dir         = os.path.join(CUR_DIR, 'wiki_lang')
        self.local_dir        = os.path.join(self.data_dir, '%s_data' % self.lang)
        self.ent_file         = os.path.join(self.local_dir, ENTERTAINMENT_FILE)
        self.info_file        = os.path.join(self.local_dir, INFOBOX_FILE)
        self.logger           = initialize_timed_rotating_logger('ry_od_errors.log')

        if self.lang == 'eng':
            self.datagen_dir = os.path.join(VTV_DATAGEN_CURRENT_DIR, 'wiki_archival_data')
        else:
            self.datagen_dir = os.path.join(VTV_DATAGEN_CURRENT_DIR, 'wiki_archival_%s_data' % self.lang)
        for dirs in [self.data_dir, self.local_dir, DATAGEN_DIR]:
            check_dir(dirs)

    def send_mail(self):
        subject    = 'Ry and Od stats for %s' % self.lang
        server     = '10.4.1.112'
        sender     = 'sudheer@headrun.com'
        receivers = ['sudheer@headrun.com']
        receivers  = ['sudheer@headrun.com', 'raman@veveo.net']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', self.text, '')

    def get_schema(self, fields):
        return dict(zip(fields, range(len(fields))))

    def get_failures(self):
        os.chdir(DATAGEN_DIR)
        datagen_file = max(glob.iglob('*.log'), key=os.path.getctime)
        for data in open(datagen_file):
            if ERROR_PATTERN in data:
                gid  = re.findall(GID, data)[0]
                got  = re.findall(GOT, data)[0]
                need = re.findall(NEED, data)[0]
                self.failures[gid] = (need, got)
        os.chdir(CUR_DIR)

    def compare_infobox(self):
        for missing in self.missing_rd:
            rd_fields = missing[2].split(DELEM)
            for rich_data in rd_fields:
                key = rich_data.split(SEPERATOR)
                if key[0] == FIRST_AIRED:
                    self.aired[missing[0]] = key[1]
                self.all_fields.add(key[0])

    def copy_files(self):
        for data_file in [ENTERTAINMENT_FILE, INFOBOX_FILE]:
            status = scp(self.vtv_password, '%s@10.4.2.187:%s' % (self.vtv_username, os.path.join(self.datagen_dir, data_file)), self.local_dir)
            if status != 0:
                print 'Failed to copy the file: 10.4.2.187:%s' % os.path.join(self.datagen_dir, data_file)
                sys.exit()

        status = scp(self.vtv_password, '%s@%s:/data/DATAGEN/DATAGEN_TEST/logs_datagen_test_%s/datagen_test_*.log' % (self.vtv_username, self.log_ip, CUR_DATE), DATAGEN_DIR)
        if status:
            status = scp(self.vtv_password, '%s@%s:/data/DATAGEN/DATAGEN_TEST/logs_datagen_test_%s/datagen_test_*.log' % (self.vtv_username, self.log_ip, YESTERDAY), DATAGEN_DIR)

    def get_infobox(self):
        info_schema = self.get_schema(INFO_FIELDS)
        for curTuple in foldFileInterface.dataFileDictIterator(self.info_file, info_schema):
            gid = curTuple[info_schema['Gi']]
            rich_data = curTuple[info_schema['Rd']]
            title = curTuple[info_schema['Ti']]
            if FIRST_AIRED in rich_data:
                rd_fields = rich_data.split(DELEM)
                for rd in rd_fields:
                    key = rd.split(SEPERATOR)
                    if key[0] == FIRST_AIRED:
                        self.infobox[gid] = key[1]
                        break
            if gid in self.failures:
                self.missing_rd.append((gid, title, rich_data))

    def get_differences(self):
        e_schema = self.get_schema(FIELDS)
        for vt in VT_LIST:
            self.ry_missing[vt]  = set()
            self.mry_aod[vt]     = 0
            self.types_count[vt] = 0
            self.pg_types_count[vt] = {'Ry': 0, 'Od': 0}

        entertainment_file = open(os.path.join(self.local_dir, ENTERTAINMENT_FILE_NEW), 'w')
        for curTuple in foldFileInterface.dataFileDictIterator(self.ent_file, e_schema):
            record = curTuple
            gid = record[e_schema['Gi']]
            pgm_type = record[e_schema['Vt']]
            release_year = record[e_schema['Ry']]
            original_date = record[e_schema['Od']]
            self.vt_fields.add(pgm_type)

            if pgm_type in VT_LIST:
                self.types_count[pgm_type] += 1
                if original_date and not release_year:
                    year = re.findall('^([0-9]{4})-[0-9]{1,2}-[0-9]{1,2}$', original_date)
                    if year:
                        record[e_schema['Ry']] = year[0]
                    self.mry_aod[pgm_type] += 1
                if release_year:
                    self.pg_types_count[pgm_type]['Ry'] += 1
                if original_date:
                    self.pg_types_count[pgm_type]['Od'] += 1

                if gid in self.infobox:
                    if not release_year or not original_date:
                        try:
                            year = re.findall('^\w+ [0-9]{1,2}, ([0-9]{4})$', self.infobox[gid])
                            if year:
                                year = year[0]
                                pgm_date = time.strptime(self.infobox[gid], '%B %d, %Y')
                                pgm_date = '%s-%s-%s' % (pgm_date.tm_year, pgm_date.tm_mon, pgm_date.tm_mday)
                                record[e_schema['Ry']] = year
                                record[e_schema['Od']] = pgm_date

                            year = re.findall('^[0-9]{1,2} \w+ ([0-9]{4})$', self.infobox[gid])
                            if year:
                                year = year[0]
                                pgm_date = time.strptime(self.infobox[gid], '%d %B %Y')
                                pgm_date = '%s-%s-%s' % (pgm_date.tm_year, pgm_date.tm_mon, pgm_date.tm_mday)
                                record[e_schema['Ry']] = year
                                record[e_schema['Od']] = pgm_date

                            year = re.findall(YEAR1, self.infobox[gid])
                            if year:
                                year = year[0]
                                pgm_date = re.findall(DATE1, self.infobox[gid])[0]
                                record[e_schema['Ry']] = year
                                record[e_schema['Od']] = pgm_date
                        except:
                            print "failed for: %s" % self.infobox[gid]

                        if not record[e_schema['Ry']]:
                            self.skipped_info.add(self.infobox[gid])

                        self.ry_missing[pgm_type].add(gid)
            foldFileInterface.writeSingleRcord(entertainment_file, record, e_schema)
        entertainment_file.close()

        if self.options.fix:
            return

        report_list = []
        headers      = ['Type']
        headers.extend(VT_LIST)
        title = 'Total Records'
        data = [ title, self.types_count[VT_LIST[0]],
                 self.types_count[VT_LIST[1]],
                 self.types_count[VT_LIST[2]] ]
        report_list.append(data)

        title = 'Available Od'
        data = [ title, self.pg_types_count[VT_LIST[0]]['Od'],
                 self.pg_types_count[VT_LIST[1]]['Od'],
                 self.pg_types_count[VT_LIST[2]]['Od'] ]
        report_list.append(data)

        title = 'Available Ry'
        data = [ title, self.pg_types_count[VT_LIST[0]]['Ry'],
                 self.pg_types_count[VT_LIST[1]]['Ry'],
                 self.pg_types_count[VT_LIST[2]]['Ry'] ]
        report_list.append(data)

        title = 'Available Od IMDB'
        data = [ title, len(self.ry_missing[VT_LIST[0]]),
                 len(self.ry_missing[VT_LIST[1]]),
                 len(self.ry_missing[VT_LIST[2]]) ]
        report_list.append(data)

        title = 'Available Ry IMDB'
        data = [ title, len(self.ry_missing[VT_LIST[0]]),
                 len(self.ry_missing[VT_LIST[1]]),
                 len(self.ry_missing[VT_LIST[2]]) ]
        report_list.append(data)

        title = 'Ry not found for'
        data = [ title,
                 self.types_count[VT_LIST[0]] - len(self.ry_missing[VT_LIST[0]]),
                 self.types_count[VT_LIST[1]] - len(self.ry_missing[VT_LIST[1]]),
                 self.types_count[VT_LIST[2]] - len(self.ry_missing[VT_LIST[2]]) ]
        report_list.append(data)

        title = 'Od not found for'
        data = [ title,
                 self.types_count[VT_LIST[0]] - len(self.ry_missing[VT_LIST[0]]),
                 self.types_count[VT_LIST[1]] - len(self.ry_missing[VT_LIST[1]]),
                 self.types_count[VT_LIST[2]] - len(self.ry_missing[VT_LIST[2]]) ]
        report_list.append(data)

        title = 'Missing Ry and available Od'
        data = [ title, self.mry_aod[VT_LIST[0]],
                 self.mry_aod[VT_LIST[1]],
                 self.mry_aod[VT_LIST[2]] ]
        report_list.append(data)

        self.text = get_table_data('Ry and Od Stats', headers, report_list)

        self.send_mail()

    def set_options(self):
        self.parser.add_option('', '--lang', default='eng', help='3 letter language')
        self.parser.add_option('', '--fix', default=None, help='re generates file with Od and Ry')

    def run_main(self):
        self.copy_files()
        self.get_failures()
        self.get_infobox()
        self.get_differences()
        self.compare_infobox()


if __name__ == '__main__':
    vtv_task_main(DategenRd)
