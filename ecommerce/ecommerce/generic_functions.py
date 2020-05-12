import os
from datetime import datetime
from scrapy import signals
OUTPUT_DIR = os.path.join(os.getcwd(), 'OUTPUT')
CRAWL_OUT_PATH = os.path.join(OUTPUT_DIR, 'crawl_out')
PROCESSING_PATH = os.path.join(OUTPUT_DIR, 'processing')
PROCESSED_PATH = os.path.join(OUTPUT_DIR, 'processed')

def make_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


def create_default_dirs():
    DEFAULT_DIRS = [
            'crawl_out', 'processing', 'processed', 'un-processed'
            ]
    make_dir_list(DEFAULT_DIRS)

def get_current_ts_with_ms():
    dt = datetime.now().strftime("%Y%m%dT%H%M%S%f")
    return dt

def make_dir_list(dir_list, par_dir=OUTPUT_DIR):
    make_dir(par_dir)
    for dir_name in dir_list:
        make_dir(os.path.join(par_dir, dir_name))
        
def get_out_file(source):
    out_file = os.path.join(CRAWL_OUT_PATH, "%s_out_file_%s.queries" %(source, get_current_ts_with_ms()))
    out_put = open(out_file, 'a+')
    return out_put
def crawlout_processing(source):
    os.chdir(CRAWL_OUT_PATH)
    cmd = 'mv %s  %s'%(source.name, PROCESSING_PATH)
    os.system(cmd)