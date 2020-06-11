from scrapy.spiders import Spider
from datetime import date
import datetime
import time
import json
import MySQLdb
import pytz
import urllib
import urllib
import logging
import logging.handlers
import os
import requests
from hashlib import md5
from pydispatch import dispatcher
from urllib import robotparser
from scrapy import signals
from sports_spiders.vtv_utils import vtv_send_html_mail_2


def mysql_connection():
    conn = MySQLdb.connect(host="localhost", user="root", passwd="$p0rTs@34#$", db="SPORTSDB", charset='utf8mb4', use_unicode=True)
    cursor = conn.cursor()
    return conn, cursor


def get_refined_tzinfo(tz_data):
    tzinfo = tz_data[0][0].split('hour')[0].strip('UTC/GMT').strip()
    return tzinfo


def get_age(born='', pattern=''):
    age = ''
    if born:
        today = date.today()
        data = (datetime.datetime(*time.strptime(born, pattern)[0:6])).date()
        age = today.year - data.year - \
            ((today.month, today.day) < (data.month, data.day))
    return age


def url_status(url, name, message):
    try:
        connection = urllib.urlopen(url)
        status = connection.getcode()
    except urllib.HTTPError as e:
        status = e.getcode()
    today = date.today()
    file_name = '%s_logfiles.log' % (today)
    if status != 200:
        logging.basicConfig(filename=file_name,
                            filemode='w+', level=logging.DEBUG)
        logging.warning('No response - %s - %s - %s' % (status, url, name))

    r = requests.get(url)
    new_url = r.url
    if new_url != url:
        logging.basicConfig(filename=file_name,
                            filemode='w+', level=logging.DEBUG)
        logging.warning('Redirecting URL - %s - %s - %s' % (status, url, name))
    if message:
        logging.basicConfig(filename=file_name,
                            filemode='w+', level=logging.DEBUG)
        logging.warning('%s - %s - %s - %s' % (message, status, '', name))


def get_weight(lbs=''):
    weight = ''
    if lbs:
        up_weight = float(lbs.strip()) * float(0.453592)
        up_weight = round(float(up_weight))
        weight = str(up_weight).replace('.0', '') + " kg"

    return weight


def get_height(feets='', inches=''):
    height = ''
    feets = feets.strip()
    inches = inches.strip()

    if feets and inches:
        feets = float(feets) * float(30.48)
        inches = float(inches) * float(2.54)
        height = round(float(feets) + float(inches))
        height = str(height).replace('.0', '') + " cm"

    elif feets and not inches:
        feets = float(feets) * float(30.48)
        height = str(feets).replace('.0', '') + " cm"

    return height


def get_sport_id(game):
    conn, cursor = mysql_connection()
    query = 'select id from sports_types where title = "%s"' % (game)
    cursor.execute(query)
    data = cursor.fetchone()
    if data:
        sport_id = str(data[0])
    else:
        sport_id = ''
    conn.close()
    return sport_id


def get_country(city='', state=''):
    conn, cursor = mysql_connection()
    query = 'select country from sports_locations where city="%s" and state="%s" and country !=""'
    values = (city, state)
    cursor.execute(query, values)
    data = cursor.fetchone()
    if not data:
        query = 'select country from sports_locations where state="%s" and country !=""'
        values = (state)
        cursor.execute(query % values)
        data = cursor.fetchone()
    country = ''
    if data:
        country = str(data[0])
    conn.close()
    return country


def get_state(city='', country=''):
    conn, cursor = mysql_connection()
    query = 'select state from sports_locations where city=%s and country=%s and state !=""'
    values = (city, country)
    cursor.execute(query, values)
    data = cursor.fetchone()
    state = ''
    if data:
        state = data[0]
    conn.close()
    return state


def get_stadium(home_sk, source):
    conn, cursor = mysql_connection()
    stadium_title = 'select B.title from sports_teams A, sports_stadiums B, sports_source_keys C where A.stadium_id = B.id and A.participant_id = C.entity_id and source_key="%s" and source="%s"' % (
        home_sk, source)
    cursor.execute(stadium_title)
    stadiums = cursor.fetchone()
    if stadiums:
        stadium = str(stadiums[0])
    else:
        stadium = ''
    conn.close()
    return stadium


def get_birth_place_id(city, state, country):
    conn, cursor = mysql_connection()
    loc_id = 'select id from sports_locations where city ="%s" and state = "%s" and country = "%s"' % (
        city, state, country)
    cursor.execute(loc_id)
    loc_id = cursor.fetchall()
    if not loc_id:
        loc_id = 'select id from sports_locations where city ="%s" and country = "%s"' % (
            city, country)
        cursor.execute(loc_id)
        loc_id = cursor.fetchall()
    if not loc_id:
        loc_id = 'select id from sports_locations where city ="%s" and state = "%s"' % (
            city, state)
        cursor.execute(loc_id)
        loc_id = cursor.fetchall()
    if not loc_id:
        loc_id = 'select id from sports_locations where city ="%s" and state = "" and country = ""' % (
            city)
        cursor.execute(loc_id)
        loc_id = cursor.fetchall()
    if loc_id:
        loc_id = str(loc_id[0][0])
    else:
        loc_id = ''
    '''if not loc_id:
        LOC_INSERTION = 'INSERT IGNORE INTO sports_locations (continent, country, state, city, street, zipcode, latlong, iso, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())'
        values = ('', country, state, city, '', '', '', '')
        cursor.execute(LOC_INSERTION, values)
        loc_query = 'select id from sports_locations where continent=%s and country=%s and state=%s and city=%s and street=%s and zipcode=%s and latlong=%s and iso=%s'
        cursor.execute(loc_query, values)
        loc_id = cursor.fetchone()[0]'''
    conn.close()
    return loc_id


def get_player_details(details, pl_id):
    conn, cursor = mysql_connection()
    birth_place = details.get('birth_place', '')
    age = details.get('age', '')
    height = details.get('height', '')
    weight = details.get('weight', '')
    role = details.get('pos', '')
    loc_id = details.get('loc_id', '')
    ref_url = details.get('ref_url', '')
    birth_date = details.get('birth_date', '')
    res_id = details.get('res_id', '')
    pl_img = details.get('pl_img', '')
    game = details.get('game', '')
    sport_id = details.get('sport_id', '')
    gender = details.get('gender', '')

    pl_id = pl_id
    debut = details.get('debut', '')
    if debut and debut != "0000-00-00 00:00:00":
        query = 'update sports_players set debut=%s where participant_id=%s'
        values = (debut, pl_id)
        cursor.execute(query, values)

    if gender:
        query = 'update sports_players set gender=%s where participant_id=%s'
        values = (gender, pl_id)
        cursor.execute(query, values)

    if sport_id and game:
        query = 'update sports_participants set game=%s, sport_id=%s where id =%s'
        values = (game, sport_id, pl_id)
        cursor.execute(query, values)

    if birth_place and len(birth_place.split(',')) == 3:
        query = 'update sports_players set birth_place=%s where participant_id=%s'
        values = (birth_place, pl_id)
        cursor.execute(query, values)
    if birth_place and len(birth_place.split(',')) == 2:
        query = 'update sports_players set birth_place=%s where participant_id=%s'
        values = (birth_place, pl_id)
        cursor.execute(query, values)

    if age and age != '0':
        query = 'update sports_players set age=%s where participant_id=%s'
        values = (age, pl_id)
        cursor.execute(query, values)
    if height and height != "0 cm" and len(height) > 6:
        query = 'update sports_players set height=%s where participant_id=%s'
        values = (height, pl_id)
        cursor.execute(query, values)
    if weight and weight != "0 kg" and len(weight) > 5:
        query = 'update sports_players set weight=%s where participant_id=%s'
        values = (weight, pl_id)
        cursor.execute(query, values)
    if role:
        query = 'update sports_players set main_role=%s where participant_id=%s and main_role=""'
        values = (role, pl_id)
        cursor.execute(query, values)
    if loc_id:
        query = 'update sports_players set birth_place_id=%s where participant_id=%s'
        values = (loc_id, pl_id)
        cursor.execute(query, values)
    if ref_url:
        query = 'update sports_participants set reference_url=%s where id=%s'
        values = (ref_url, pl_id)
        cursor.execute(query, values)
    if res_id:
        query = 'update sports_participants set location_id=%s where id=%s'
        values = (res_id, pl_id)
        cursor.execute(query, values)
    if pl_img:
        query = 'update sports_participants set image_link=%s where id=%s'
        values = (pl_img, pl_id)
        cursor.execute(query, values)

    if birth_date and "0000-00-00" not in birth_date:
        query = 'update sports_players set birth_date=%s where participant_id=%s and birth_date="0000-00-00 00:00:00"'
        values = (birth_date, pl_id)
        cursor.execute(query, values)
    conn.close()


def get_tzinfo(city='', country='', game_datetime=''):
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
        game_date = datetime.datetime.strptime(
            game_datetime, '%Y-%m-%d %H:%M:%S')
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


def init_logger(log_fname, debug_mode=False):
    global log
    log_dir = 'logs'
    log_file = os.path.join(log_dir, '%s.log' % log_fname)
    check_dir_access(log_dir)
    log = logging.getLogger(log_file)
    handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=104857600, backupCount=3)
    formatter = logging.Formatter("%(asctime)s - %(message)s")
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)


def get_utc_time(date_val, pattern, tz_info):
    utc = pytz.utc
    eastern = pytz.timezone(tz_info)
    fmt = '%Y-%m-%d %H:%M:%S'
    date = datetime.datetime.strptime(date_val, pattern)
    date_eastern = eastern.localize(date, is_dst=None)
    date_utc = date_eastern.astimezone(utc)
    utc_date = date_utc.strftime(fmt)

    return utc_date


def log(fun):
    def inner(*args, **kwargs):
        try:
            try:
                init_logger(args[0].name)
            except:
                init_logger('ERROR')
            log.info('%s(): %s - %s' % (fun.__name__, args[1].url, args[1].status))
        except:
            log.info('Wrong function defined to logger: %s()' % fun.__name__)
        return fun(*args, **kwargs)
    return inner


def extract_data(data, path, delem=''):
    return delem.join(data.xpath(path).extract()).strip()


def extract_list_data(data, path):
    return data.xpath(path).extract()


def get_nodes(data, path):
    return data.xpath(path)


def get_md5(data):
    return md5.md5(data).hexdigest()


class VTVSpider(Spider):
    def __init__(self, *args, **kwargs):
        self.spider_type = kwargs.get('spider_type', '')
        for url in self.start_urls:
            crawl_status, robotsurl = get_crawl_access(url)
            if not crawl_status:
                self.start_urls.remove(url)
        from scrapy.crawler import Crawler
        self.crawler = Crawler(Spider)
        print(self.crawler.stats.get_stats())
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self):
        conn, cursor = mysql_connection()
        #spider_stats    = self.crawler.stats.get_stats()
        spider_stats = {}
        start_time = spider_stats.get('start_time')
        finish_time = spider_stats.get('finish_time')
        spider_stats['start_time'] = str(start_time)
        spider_stats['finish_time'] = str(finish_time)
        query = 'insert into WEBSOURCEDB.crawler_summary(crawler, start_datetime, end_datetime, type, count, aux_info, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, now(), now())'
        values = (self.name, start_time, finish_time,
                  'sports', '', json.dumps(spider_stats))
        #cursor.execute(query, values)
        cursor.close()
        conn.close()

        receivers = ["charan@headrun.com"]
        sender = "sports@headrun.com"
        subject = "Alert on %s" % self.name
        table = '<table border = "1" style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px">'
        table += "<tr><th align = 'left'>spider_name</th><th align = 'left'>error</th>"
        send_mail = False
        if 'player' not in self.name and 'item_scraped_count' not in spider_stats.keys():
            table += "<tr><td align = 'right'>%s</td><td align = 'right'>%s</td>" % (
                self.name, 'no items scrapped')
            send_mail = True

        if 'log_count/ERROR' in spider_stats.keys():
            table += "<td align = 'right'>%s</td><td align = 'right'>%s</td>" % (
                self.name, 'crawler running with errors')
            send_mail = True

        if send_mail:
            table += "</table>\n"
            #vtv_send_html_mail_2('spider_mail.log', 'localhost', sender, receivers, subject, '', ''.join(table), '')


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
        self.rpcache = {}  # a dictionary of RobotFileParser objects, by domain
        self.robotsurl = ''
        __version__ = "1.371"
        self.urlopener = urllib.URLopener()
        self.urlopener.version = "feedfinder/" + __version__ + " " + \
            self.urlopener.version + " +http://www.aaronsw.com/2002/feedfinder/"
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
