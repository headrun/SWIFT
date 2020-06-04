import logging
import traceback
import re
from hashlib import md5
from itertools import chain
from datetime import datetime
from inspect import isgenerator
from collections import defaultdict
from os import path, getcwd, mkdir, system
from pydispatch import dispatcher
from MySQLdb import connect, IntegrityError,\
        ProgrammingError
from MySQLdb.cursors import Cursor,\
DictCursor, SSCursor, SSDictCursor
from scrapy import signals
from scrapy.spiders import Spider
from scrapy.http import Request

from scrapy.utils.project import get_project_settings
from ecommerce.table_schemas import CRAWL_TABLE_CREATE_QUERY,\
CRAWL_TABLE_SELECT_QUERY

LOGGERS = {}

SETTINGS = get_project_settings()
BATCH_SIZE = 500
YIELD_SIZE = 500

DB_HOST = SETTINGS['DB_HOST']
DB_UNAME = SETTINGS['DB_USERNAME']
DB_PASSWD = SETTINGS['DB_PASSWORD']
URLQ_DB_NAME = SETTINGS['URLQ_DATABASE_NAME']
LOGS_DIR = SETTINGS['LOGS_DIR']

MYSQL_CONNECT_TIMEOUT_VALUE = 30
OUTPUT_DIR = path.join(getcwd(), 'OUTPUT')
QUERY_FILES_DIR = path.join(OUTPUT_DIR, 'crawl_out')
QUERY_FILES_PROCESSING_DIR = path.join(OUTPUT_DIR, 'processing')


class EcommSpider(Spider):

    def __init__(self, name=None, **kwargs):
        self.crawl_type = kwargs.get('crawl_type', 'keepup')
        self.content_type = kwargs.get('content_type', '')
        self.allow_duplicate_urls = False
        self.request_headers = {}
        self.limit = kwargs.get('limit', 0) or getattr(self.__class__, 'limit', 1000)

        self.create_logger_obj()
        self.create_default_dirs()
        self.initialize_default_variables()
        self.crawl_table_name = self.ensure_tables()

        self._start_urls = None
        if hasattr(self.__class__, 'start_urls'):
            self._start_urls = getattr(self.__class__, 'start_urls')
            try:
                delattr(self.__class__, 'start_urls')
            except AttributeError:
                pass

        dispatcher.connect(self._spider_closed, signals.spider_closed)
        super(EcommSpider, self).__init__(name, **kwargs)

    def create_logger_obj(self):
        cur_dt = str(datetime.now().date())
        if not path.exists(LOGS_DIR):
            mkdir(LOGS_DIR)
        self.log_file_name = "spider_%s.log" % (cur_dt)
        self.log = get_logger(path.join(LOGS_DIR, self.log_file_name))

    def create_default_dirs(self):
        default_dirs = ['crawl_out', 'processing', 'processed', 'un-processed']
        if not path.exists(OUTPUT_DIR):
            mkdir(OUTPUT_DIR)

        for _dir in default_dirs:
            _dir = path.join(OUTPUT_DIR, _dir)
            if not path.exists(_dir):
                mkdir(_dir)

    def initialize_default_variables(self):
        self._close_called = False
        self._sks = defaultdict(set)
        self.urlq_cursor = None
        self.got_page_sks_len = 0
        self.crawl_vals_set = set()

        self.insights_file = None
        self.metadata_file = None

    def get_urlq_cursor(self):
        if self.urlq_cursor: return self.urlq_cursor
        self.urlq_conn, self.urlq_cursor = get_mysql_connection(db_name=URLQ_DB_NAME)
        return self.urlq_cursor

    def ensure_tables(self):
        source = self.name.split('_')[0]
        crawl_table_name = "%s_crawl" % (source)

        show_query = 'SHOW TABLES LIKE "%s_%%";' % (source)
        self.get_urlq_cursor().execute(show_query)
        if self.get_urlq_cursor().rowcount > 0:
            self.log.info("Tables: %s Already Exist.", crawl_table_name)
            return crawl_table_name

        self.get_urlq_cursor().execute(CRAWL_TABLE_CREATE_QUERY.replace('#CRAWL-TABLE#', crawl_table_name))
        self.log.info("Tables: %s Newly Created.", crawl_table_name)

        return crawl_table_name

    def get_recs_by_batch_wise(self, batch_size=BATCH_SIZE):
        while True:
            recs = self.get_urlq_cursor().fetchmany(batch_size)
            if not recs:
                break
            for rec in recs:
                yield rec

    def start_requests(self):
        start_urls = self._start_urls or getattr(self, 'start_urls', None)
        source, content_type, crawl_type = self.get_source_content_and_crawl_type(self.name)

        requests = []
        if crawl_type == "terminal":
            requests = self.get_terminal_requests(content_type, requests)
        elif start_urls:
            requests = self.get_start_urls_requests(start_urls, requests)

        return requests

    def get_start_urls_requests(self, start_urls, requests):
        if not isinstance(start_urls, (tuple, list)) and not isgenerator(start_urls):
            start_urls = [start_urls]

        for start_url in start_urls:
            if isinstance(start_url, Request):
                requests.append(start_url)
            else:
                req_meta = {'data': None}
                req = Request(
                    #start_url, self.parse, None, meta=req_meta,
                    start_url, self.parse, meta=req_meta,
                    headers=self.request_headers,
                    dont_filter=self.allow_duplicate_urls,
                )
                requests.append(req)

        return requests

    def get_terminal_requests(self, content_type, requests):
        sel_query = CRAWL_TABLE_SELECT_QUERY % (self.crawl_table_name, content_type, self.limit)
        execute_query(self.get_urlq_cursor(), sel_query)

        selected_sks = set()
        for sk, url, meta_data in self.get_recs_by_batch_wise():
            if not sk.strip():
                continue
            try:
                if meta_data:
                    meta_data = eval(meta_data)
                req_meta = {'data': meta_data, 'sk': sk}
                req = Request(
                    #url, self.parse, None, meta=req_meta,
                    url, self.parse, meta=req_meta,
                    headers=self.request_headers,
                    dont_filter=self.allow_duplicate_urls,
                )
                requests.append(req)
                selected_sks.add(sk)
            except Exception:
                traceback.print_exc()
                self.log.error("Error: %s", traceback.format_exc())

        self.log.info("Total Sks Picked From Crawl Tables: %s",
                      len(selected_sks))
        if len(selected_sks) > 0:
            self.update_selected_sks_with_nine_status(
                selected_sks, content_type)

        return requests

    def update_selected_sks_with_nine_status(self, selected_sks, content_type):
        self.log.info("In Update Selected Sks With Nine Status Func Called")
        sks = ', '.join(['"%s"' % sk for sk in selected_sks])
        delete_query = 'DELETE FROM ' + self.crawl_table_name + \
            ' WHERE crawl_status=9 AND content_type="%s" AND sk in (%s);'
        self.get_urlq_cursor().execute(delete_query % (content_type, sks))

        update_query = 'UPDATE ' + self.crawl_table_name + \
            ' SET crawl_status=9, modified_at=NOW() WHERE content_type="%s"'
        update_query += ' AND crawl_status=0 AND sk="%s";'
        update_query_new = 'UPDATE ' + self.crawl_table_name + \
            ' SET crawl_status=9, modified_at=NOW() WHERE content_type="%s"'
        update_query_new += " AND crawl_status=0 AND sk='%s';"
        try:
            self.get_urlq_cursor().execute(delete_query % (content_type, sks))
        except:
            self.get_urlq_cursor().execute(delete_query, (content_type, sks))

        for selected_sk in selected_sks:
            try:
                self.get_urlq_cursor().execute(update_query % (content_type, selected_sk))
            except ProgrammingError:
                try:
                    self.get_urlq_cursor().execute(update_query_new, (content_type, selected_sk))
                except ProgrammingError:
                    self.log.info(
                        "IntegrityError: Unable to update the status for this Sk: %s", selected_sk)
                except Exception:
                    self.log.error("Error Query: %s - Error: %s",
                                   update_query_new, traceback.format_exc())
            except IntegrityError:
                try:
                    self.log.info(
                        "IntegrityError: Unable to update the status for this Sk: %s", selected_sk)
                except IntegrityError:
                    self.log.info(
                        "IntegrityError: Unable to update the status for this Sk: %s", selected_sk)
                except Exception:
                    self.log.error("Error Query: %s - Error: %s",
                                   update_query, traceback.format_exc())
            except Exception:
                self.log.error("Error Query: %s - Error: %s",
                               update_query, traceback.format_exc())

        del selected_sks

    def update_urlqueue_with_resp_status(self):
        self.log.info("In Update Url Queue With Respective Status")
        source, content_type, crawl_type = self.name.split('_')
        self.log.info("Source: %s - Content Type: %s - Type: %s",
                      source, content_type, crawl_type)

        delete_query = 'DELETE FROM ' + self.crawl_table_name + \
            ' WHERE crawl_status=%s AND content_type="%s" AND sk in (%s);'
        update_query = 'UPDATE ' + self.crawl_table_name + \
            ' SET crawl_status=%s, modified_at=NOW() WHERE crawl_status=9 AND'
        update_query += ' content_type="%s" AND sk="%s";'

        for k, vals in self._sks.items():
            self.log.info("Source: %s - Content Type: %s - Status: %s - Sks Length: %s",
                          source, content_type, k, len(vals))
            sks = ", ".join(['"%s"' % val for val in vals])
            self.get_urlq_cursor().execute(delete_query % (k, content_type, sks))

            for sk in vals:
                try:
                    self.get_urlq_cursor().execute(update_query % (k, content_type, sk))
                except IntegrityError:
                    self.log.error(
                        "IntegrityError: Unable to update Status=1 for given Sk: %s", sk)
                except:
                    self.log.error("Error Query: %s - Error: %s",
                                   update_query, traceback.format_exc())

    def reset_cnt_and_crawl_sec_vals(self):
        self.crawl_vals_set = set()

    def insert_crawl_tables_data(self):
        crawl_table_query = 'INSERT INTO %s_crawl' % self.source + \
            ' (sk, url, meta_data, crawl_type, content_type, '
        crawl_table_query += 'crawl_status, created_at, modified_at) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())'
        crawl_table_query += ' ON DUPLICATE KEY UPDATE url=%s, meta_data=%s, crawl_type=%s, crawl_status=%s, modified_at=NOW();'


        self.log.info("Source: %s - Crawl Sks Length: %s", self.source, len(self.crawl_vals_set))
        if len(self.crawl_vals_set) > 0:
            self.get_urlq_cursor().executemany(crawl_table_query, self.crawl_vals_set)

    def get_page(self, spider_name, url, sk, meta_data=None):
        self.source, content_type, crawl_type = spider_name.split('_')
        if len(self.crawl_vals_set) == YIELD_SIZE:
            self.insert_crawl_tables_data()
            self.reset_cnt_and_crawl_sec_vals()

        meta_data = repr(meta_data) if meta_data else ''
        self.crawl_vals_set.add((
            sk, url, meta_data, self.crawl_type, content_type, 0,
            url, meta_data, self.crawl_type, 0
        ))

    def got_page(self, sk, got_pageval=1):
        if not sk:
            raise

        if self.got_page_sks_len == 100:
            self.update_urlqueue_with_resp_status()
            self._sks = defaultdict(set)
            self.got_page_sks_len = 0

        self._sks[got_pageval].add(sk)
        self.got_page_sks_len += 1

    def get_insights_file(self):
        if self.insights_file:
            return self.insights_file

        insights_queries_filename = path.join(
            QUERY_FILES_DIR, "%s_insights_%s.queries" % (self.name, get_current_ts_with_ms()))
        self.insights_file = open(insights_queries_filename, 'w')

        return self.insights_file


    def get_metadata_file(self):
        if self.metadata_file:
            return self.metadata_file

        metadata_queries_filename = path.join(
            QUERY_FILES_DIR, "%s_metadata_%s.queries" % (self.name, get_current_ts_with_ms()))
        self.metadata_file = open(metadata_queries_filename, 'w')

        return self.metadata_file

    def close_all_opened_query_files(self):
        files_list = [self.insights_file, self.metadata_file]
        for f in files_list:
            if f is not None:
                f.flush()
                f.close()
                move_file(f.name, QUERY_FILES_PROCESSING_DIR)

    def get_source_content_and_crawl_type(self, spider_name):
        content_type = ''

        if "_browse" in spider_name:
            source, crawl_type = self.name.split('_')[0], 'browse'
        else:
            source, content_type, crawl_type = self.name.split('_')

        return source.strip(), content_type.strip(), crawl_type.strip()

    def close_conn(self):
        self.log.info("Close MySQL Cursor Function Called")
        close_mysql_connection(self.urlq_conn, self.urlq_cursor)
        self.log.info("Successfully Closed MySQL Connection")

    def _spider_closed(self, spider, reason):
        if spider.name != self.name: return
        if self.crawl_vals_set: self.insert_crawl_tables_data()

        if '_browse' not in self.name and len(self._sks) > 0:
            self.update_urlqueue_with_resp_status()

        self.close_all_opened_query_files()
        self.close_conn()
        self.initialize_default_variables()



def execute_query(cursor, query, values=''):
    try:
        if values:
            cursor.execute(query, values)

        cursor.execute(query)
    except:
        traceback.print_exc()

def get_current_ts_with_ms():
    dt = datetime.now().strftime("%Y%m%dT%H%M%S%f")
    return dt

def get_mysql_connection(server=DB_HOST, db_name=URLQ_DB_NAME, cursorclass=""):
    try:
        cursor_dict = {'dict': DictCursor, 'ssdict': SSDictCursor, 'ss': SSCursor}
        cursor_class = cursor_dict.get(cursorclass, Cursor)

        conn = connect(
            host=server, user=DB_UNAME, passwd=DB_PASSWD, db=db_name,
            connect_timeout=MYSQL_CONNECT_TIMEOUT_VALUE, cursorclass=cursor_class,
            charset="utf8", use_unicode=True
        )
        if db_name: conn.autocommit(True)
        cursor = conn.cursor()

    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        conn, cursor = None, None

    return conn, cursor

def close_mysql_connection(conn, cursor):
    if cursor: cursor.close()
    if conn: conn.close()

def get_logger(log_file):
    if LOGGERS.get('spider_process'):
        return LOGGERS.get('spider_process')
    else:
        logger = logging.getLogger('spider_process')
        logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)d - %(funcName)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        LOGGERS.update(dict(name=logger))
    return logger

def xcode(text, encoding='utf8', mode='strict'):
    return text.encode(encoding, mode) if isinstance(text, str) else text

def encode_md5(x):
    return md5(xcode(x)).hexdigest()

def move_file(source, dest):
    cmd = "mv %s %s" % (source, dest)
    system(cmd)

def clean(text):
    if not text:
        return text

    value = text
    value = re.sub("&amp;", "&", value)
    value = re.sub("&lt;", "<", value)
    value = re.sub("&gt;", ">", value)
    value = re.sub("&quot;", '"', value)
    value = re.sub("&apos;", "'", value)
    value = re.sub("              ", " ", value)

    return value

def compact(text, level=0):
    if text is None:
        return ''

    if level == 0:
        text = text.replace("\n", " ")
        text = text.replace("\r", " ")

    compacted = re.sub("\s\s(?m)", " ", text)
    if compacted != text:
        compacted = compact(compacted, level+1)

    return compacted.strip()

def textify(nodes, sep=' '):
    if not isinstance(nodes, (list, tuple)):
        nodes = [nodes]

    def _t(x):
        if isinstance(x, (str, str)):
            return [x]

        if hasattr(x, 'xmlNode'):
            if not x.xmlNode.get_type() == 'element':
                return [x.extract()]
        else:
            if isinstance(x.root, (str, str)):
                return [x.root]

        return (n.extract() for n in x.select('.//text()'))

    nodes = chain(*(_t(node) for node in nodes))
    nodes = (node.strip() for node in nodes if node.strip())

    return sep.join(nodes)

def extract(sel, xpath, sep=' '):
    return clean(compact(textify(sel.xpath(xpath).extract(), sep)))

def extract_data(data, path, delem=''):
    return delem.join(i.strip() for i in data.xpath(path).extract() if i).strip()

def extract_list_data(data, path):
    return data.xpath(path).extract()

def get_nodes(data, path):
    return data.xpath(path)

