from scrapy.spider import Spider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
import time, datetime
import MySQLdb
import pytz
import urllib
import logging
import logging.handlers
import robotparser
import os

def mysql_connection():
    conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
    cursor = conn.cursor()
    return conn, cursor

def get_tzinfo(city='', country = ''):
    conn, cursor = mysql_connection()
    query = 'select tzinfo from sports_timezones where %s like "%%%s%%"'
    if city:
        cursor.execute(query % ('city', city))
        tz_info = cursor.fetchall()
        if len(tz_info) > 1:
            alt_query = 'select tzinfo from sports_timezones where %s = "%s"'
            cursor.execute(alt_query % ('city', city))
            tz_info = cursor.fetchall()
        conn.close()
        if tz_info and len(tz_info) == 1:
            return tz_info[0][0].split('hour')[0].strip('UTC/GMT').strip()
    elif country:
        if len(country) == 2:
            field = 'iso3166_1_2'
        elif len(country) == 3:
            field = 'iso3166_1_3'
        else:
            field = 'country'
        cursor.execute(query % (field, country))
        result = cursor.fetchall()
        conn.close()
        if result:
            return result[0][0].split('hour')[0].strip('UTC/GMT').strip()
    else:
        conn.close()
        return ''

def check_dir_access(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

def init_logger(log_fname, debug_mode = False):
    global log
    log_dir = 'logs'
    log_file = os.path.join(log_dir, '%s.log' %log_fname)
    check_dir_access(log_dir)
    log = logging.getLogger(log_file)
    handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=104857600, backupCount=3)
    formatter = logging.Formatter("%(asctime)s - %(message)s")
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)

def get_utc_time(date_val, pattern, tz_info):
    utc          = pytz.utc
    eastern      = pytz.timezone(tz_info)
    fmt          = '%Y-%m-%d %H:%M:%S'
    date         = datetime.datetime.strptime(date_val, pattern)
    date_eastern = eastern.localize(date, is_dst=None)
    date_utc     = date_eastern.astimezone(utc)
    utc_date     = date_utc.strftime(fmt)

    return utc_date

def get_local_timzone(country):
    pass

def log(fun):
    def inner(*args, **kwargs):
        try:
            try:
                init_logger(args[0].name)
            except:
                init_logger('ERROR')
            log.info('%s(): %s - %s' %(fun.func_name, args[1].url, args[1].status))
        except:
            log.info('Wrong function defined to logger: %s()' %fun.func_name)
        return fun(*args, **kwargs)
    return inner

def extract_data(data, path, delem=''):
    xpath_value = data.xpath(path)
    return textify(xpath_value, delem=delem).strip()

def extract_list_data(data, path):
    xpath_value = data.xpath(path)
    return textify(xpath_value, list_data=True)

def get_nodes(data, path):
    return data.xpath(path)

def textify(node, delem="\n", list_data=False):
    """
    Extracts text from the subtree of the node
    """
    #FIXME: optimize by removing string concatenation
    # and using lists instead
    if isinstance(node, str) or isinstance(node, unicode):
        return node

    if isinstance(node, list):
        texts = []
        items = node
        for item in items:
            item_text = textify(item)
            texts.append(item_text)
        if list_data:
            return texts
        return delem.join(texts)

    value = node.extract()
    if value is None: value = ''
    if isinstance(value, list):
        value = ' '.join(value)

    return value

class VTVSpider(Spider):
    def __init__(self, *args, **kwargs):
        super(Spider, self).__init__(*args, **kwargs)
        self.spider_type = kwargs.get('spider_type', '')
        for url in self.start_urls:
            crawl_status, robotsurl = get_crawl_access(url)
            if not crawl_status:
                self.start_urls.remove(url)

def get_crawl_access(url):
    urlgatekeeper = URLGatekeeper()
    try:
        rp, robotsurl = urlgatekeeper._getrp(url)
        crawl_status = rp.can_fetch('*', url)
        return crawl_status, robotsurl
    except:
        return True, ''

class URLGatekeeper:
    """a class to track robots.txt rules across multiple servers"""
    def __init__(self):
        self.rpcache = {} # a dictionary of RobotFileParser objects, by domain
        self.robotsurl = ''
        __version__ = "1.371"
        self.urlopener = urllib.FancyURLopener()
        self.urlopener.version = "feedfinder/" + __version__ + " " + self.urlopener.version + " +http://www.aaronsw.com/2002/feedfinder/"
        self.urlopener.addheaders = [('User-agent', self.urlopener.version)]
        robotparser.URLopener.version = self.urlopener.version
        robotparser.URLopener.addheaders = self.urlopener.addheaders

    def _getrp(self, url):
        protocol, domain = urlparse.urlparse(url)[:2]
        if self.rpcache.has_key(domain):
            return self.rpcache[domain], self.robotsurl
        baseurl = '%s://%s' % (protocol, domain)
        self.robotsurl = urlparse.urljoin(baseurl, 'robots.txt')
        rp = robotparser.RobotFileParser(self.robotsurl)
        try:
            rp.read()
        except:
            pass
        self.rpcache[domain] = rp
        return rp, self.robotsurl
