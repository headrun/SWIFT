'''import os, re
import glob
import ssh_utils
import MySQLdb
import time
from datetime import datetime, timedelta
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2


CUR_DATE      = str(datetime.utcnow().date())
YESTERDAY     = str((datetime.utcnow() - timedelta(days=1)).date())
CUR_DIR       = os.getcwd()
DATAGEN_DIR   = os.path.join(CUR_DIR, 'datagen_test')
ERROR_PATTERN = 'FIELD: %s'
TE_PATTERN    = ERROR_PATTERN % 'Te'
TO_PATTERN    = ERROR_PATTERN % 'To'
TOU_GID       = 'TOURNAMENTS DB GID: (.*?):'
GID_PATTERN   = 'GID: (.*?):'
GOT           = 'GOT: (.*?) '
NEED          = 'NEED: (.*?) '
SUITE_ID      = 'SUITE: (.*?) GID:'
TEST_ID       = 'ID: (.*?) SUITE:'

ql_ip = '10.4.18.183'
db_name  = 'SPORTSDB'

REPORT_DIR  = '/data/REPORTS/SANITY_REPORTS/'
LOG_DIR     = '/REPORTS/SANITY_REPORTS/DATAGEN_SANITY/'

date_now = date.today()
cur_date = date_now.strftime("%Y-%m-%d")

class DataGenSanity(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        my_name = 'DATAGEN_SANITY'''
