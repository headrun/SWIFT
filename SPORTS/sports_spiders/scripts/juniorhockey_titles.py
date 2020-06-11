import MySQLdb
from scrapy.http import Request
from scrapy.selector import Selector
import urllib
import json
conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd = 'veveo123', db="SPORTSDB", charset='utf8', use_unicode=True)
cursor = conn.cursor()

URL = "http://cluster.leaguestat.com/feed/?feed=modulekit&view=teamsbyseason&key=c680916776709578&fmt=json&client_code=whl&lang=en&season_id=257&league_code=&fmt=json"
URL = "http://cluster.leaguestat.com/feed/?feed=modulekit&view=teamsbyseason&key=c680916776709578&fmt=json&client_code=lhjmq&lang=en&season_id=184&league_code=&fmt=json"
URL = "http://cluster.leaguestat.com/feed/?feed=modulekit&view=teamsbyseason&key=c680916776709578&fmt=json&client_code=ohl&lang=en&season_id=56&league_code=&fmt=json"
response = urllib.urlopen(URL)
html = response.read()
for each in json.loads(html)['SiteKit']['Teamsbyseason']:
    team_sk         =  each['id']
    display_title   =  each['city']
    short_title     =  each['nickname']
    callsign        =  each['code']
    query = "select entity_id  from sports_source_keys where entity_type='participant' and source='ohl' and source_key =%s"%team_sk
    cursor.execute(query)
    data = cursor.fetchall()
    par_id = data[0][0]
    if len(data)==1:
        up_query = "update sports_teams set display_title = %s, short_title =%s, callsign=%s where participant_id =%s limit 1"
        values   = (display_title, short_title, callsign,par_id)
        cursor.execute(up_query, values)
        print up_query%values
conn.close()
