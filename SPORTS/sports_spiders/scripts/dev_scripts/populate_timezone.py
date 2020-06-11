from lxml.html import fromstring
import urllib2
import socket
import MySQLdb
import re
import time
import random

socket.setdefaulttimeout(10)

random_sleep = [10, 30, 35, 15, 5, 2]
random_sleep = [1, 2]
conn = MySQLdb.connect(user="root", host="10.4.18.183", db='SPORTSDB', charset='utf8', use_unicode=True)
cursor = conn.cursor()

domain = 'http://www.timeanddate.com'
references = ["http://www.timeanddate.com/worldclock/",
              "http://www.timeanddate.com/worldclock/custom.html?continent=africa",
              "http://www.timeanddate.com/worldclock/custom.html?continent=namerica",
              "http://www.timeanddate.com/worldclock/custom.html?continent=samerica",
              "http://www.timeanddate.com/worldclock/custom.html?continent=asia",
              "http://www.timeanddate.com/worldclock/custom.html?continent=australasia",
              "http://www.timeanddate.com/worldclock/custom.html?continent=europe"]


def get_root(reference):
    time.sleep(random.choice(random_sleep))
    user_agent = 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36'
    req = urllib2.Request(reference, headers={ 'User-Agent': user_agent })
    response = urllib2.urlopen(req)
    return response

def populate_terminal(response):
    data = response.read()
    root = fromstring(data)

    try:
        country = root.xpath('//span[contains(text(), "Country: ")]/../text()')[0].strip()
        lat_long = root.xpath('//span[contains(text(), "Lat/long: ")]/../text()')[0].strip()
        time_zone = ''
        try:
            time_zone = root.xpath('//h2/a[contains(text(), "Time zone")]/..//following-sibling::div[1]/small/text()')[0].split('(')[0].strip()
        except:
            pass
        tz_info = root.xpath('//h2/a[contains(text(), "Time zone")]/..//following-sibling::div[1]/text()')[0]
        dst_info = root.xpath('//h2/a[contains(text(), "DST starts")]/..//following-sibling::div[1]/text()')
        dst_data = 0
        if dst_info:
            dst_info = ' '.join(dst_info)
            if 'Forward' in dst_info:
                dst_data = re.findall('Forward (.*?) hour', dst_info, re.M | re.I)
                if dst_data:
                    dst_data = '+' + dst_data[0]
                else:
                    dst_data = 0
            elif 'Back' in dst_info:
                dst_data = re.findall('Back (.*?) hour', dst_info, re.M | re.I)
                if dst_data:
                    dst_data = '-' + dst_data[0]
                else:
                    dst_data = 0

        city = root.xpath('//div[@class="main-content-div"]/header[@class="fixed"]/h1/text()')[0].split(',')[0].split(' in ')[-1]
        if "No UTC/GMT offset" in tz_info:
            tz_info = "UTC/GMT +0 hours"
        query = "INSERT IGNORE into sports_timezones(city, state, country, lat_long, tzinfo, time_zone, is_dst) values(%s, %s, %s, %s, %s, %s, %s) on duplicate key update tzinfo=%s, is_dst=%s, time_zone=%s"
        values = (city, '', country, lat_long, tz_info, time_zone, dst_data, tz_info, dst_data, time_zone)
        #print "in terminal %s" % str(values)
        cursor.execute(query, values)
    except:
        print 'Failed somewhere'
        pass

def populate_timezone(response):
    root = fromstring(response.read())
    nodes = root.xpath('//div[@class="main-content-div"]//table/tr/td/a/@href')
    for node in nodes:
        reference = domain + node
        data = get_root(reference)
        populate_terminal(data)

for reference in references:
    response = get_root(reference)
    populate_timezone(response)
