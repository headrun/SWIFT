from scrapy.spider import Spider
import datetime
import MySQLdb
import pytz
import urllib
import logging
import logging.handlers
import robotparser
import os

def mysql_connection():
    conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
    cursor = conn.cursor()
    return conn, cursor

def get_refined_tzinfo(tz_data):
    tzinfo = tz_data[0][0].split('hour')[0].strip('UTC/GMT').strip()
    return tzinfo

def get_weight(lbs = ''):
    weight = ''
    if lbs:
        up_weight = float(lbs.strip()) * float(0.453592)
        up_weight = round(float(up_weight))
        weight = str(up_weight).strip('.0') + " kg"

    return weight

def get_height(feets = '', inches = ''):
    height = ''
    feets = feets.strip()
    inches = inches.strip()

    if feets and inches:
        feets = float(feets) * float(30.48)
        inches = float(inches) * float(2.54)
        height = round(float(feets) + float(inches))
        height = str(height).strip('.0') + " cm"

    elif feets and not inches:
        feets = float(feets) * float(30.48)
        height = feets.strip('.0') + " cm"

    return height

def get_tzinfo(city='', country = '', game_datetime= ''):
    conn, cursor = mysql_connection()
    tzinfo = ''
    query = 'select tzinfo, dst_start, dst_end from sports_timezones where %s like "%%%s%%"'
    if country:
        if len(country) == 2:
            field = 'iso3166_1_2'
        elif len(country) == 3:
            field = 'iso3166_1_3'
        else:
            field = 'country'

    if city and country:
        query = 'select tzinfo, dst_start, dst_end from sports_timezones where %s like "%%%s%%" and city="%s"'
        cursor.execute(query % (field, country, city))
        tz_data = cursor.fetchall()
        conn.close()
        if tz_data and len(tz_data) == 1:
            tzinfo = get_refined_tzinfo(tz_data)

    elif city:
        cursor.execute(query % ('city', city))
        tz_data = cursor.fetchall()
        if len(tz_data) > 1:
            alt_query = 'select tzinfo, dst_start, dst_end from sports_timezones where %s = "%s"'
            cursor.execute(alt_query % ('city', city))
            tz_data = cursor.fetchall()
        conn.close()
        if tz_data and len(tz_data) == 1:
            tzinfo = get_refined_tzinfo(tz_data)

    elif country:
        cursor.execute(query % (field, country))
        tz_data = cursor.fetchall()
        conn.close()
        if tz_data:
            tzinfo = get_refined_tzinfo(tz_data)
    else:
        conn.close()
        return ''

    if game_datetime and tzinfo:
        tzinfo = adjust_dst(tzinfo, game_datetime, tz_data)
        return tzinfo
    else:
        return tzinfo

def adjust_dst(tzinfo, game_datetime, dst_data):
    dst_tzinfo = tzinfo
    if tzinfo:
        game_date = datetime.datetime.strptime(game_datetime , '%Y-%m-%d %H:%M:%S')
        dst_start = dst_data[0][1]
        dst_end = dst_data[0][2]
        if dst_start and dst_end:
            if game_date > dst_start:
                if game_date < dst_end:
                    if ':' in tzinfo:
                        tzinfo = tzinfo.replace(':', '.')
                        tzinfo_ = float(tzinfo) + 1
                        dst_tzinfo = str(tzinfo_).replace('.3', ':30')
                    else:
                        dst_tzinfo = int(tzinfo) + 1
                    if "-" not in tzinfo:
                        dst_tzinfo = "+" + str(dst_tzinfo)

    return dst_tzinfo

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
    return delem.join(data.select(path).extract()).strip()

def extract_list_data(data, path):
    return data.select(path).extract()

def get_nodes(data, path):
    return data.select(path)

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
