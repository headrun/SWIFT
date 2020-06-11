import urllib
import MySQLdb
import traceback
from scrapy.http import TextResponse
from scrapy.selector import Selector
url1 = "https://maps.googleapis.com/maps/api/place/textsearch/xml?query=%s&key=AIzaSyCZF-6qfjKQFzlIA4WhpVno6RFPOCGfxko"

address = file("STADIUMS_ADDRESS_1", "ab+")
not_available = file("not_available", "ab+")

sports_cursor = MySQLdb.connect(db="SPORTSDB", host="10.4.18.183").cursor()
guid_merge    = MySQLdb.connect(db="GUIDMERGE", host="10.4.2.187").cursor()


merged_gids = []
query = 'select * from sports_wiki_merge'
guid_merge.execute(query)
records = guid_merge.fetchall()

for i in records:
    exposed_gid, child_gid, action, modified_date = i
    if child_gid.startswith("STAD"):
        merged_gids.append(child_gid)

unavail = [gid.strip() for gid in file("not_available", "r+").readlines()]

#query = 'select stadium_id from select id, gid, title from sports_stadiums'
#query= "select stad.id, stad.gid, stad.title from sports_teams T, sports_participants P, sports_tournaments ST, sports_stadiums stad, sports_locations sl where P.id = T.participant_id and T.stadium_id=stad.id and stad.location_id=sl.id and T.tournament_id = ST.id and P.id in (select participant_id from sports_tournaments_participants where tournament_id= 559)"
query = "select id, gid, title from sports_stadiums where location_id =''"
sports_cursor.execute(query)
records = sports_cursor.fetchall()

print len(records)
count = 0
for i in records:
    id_, gid, title = i
    if gid not in merged_gids:
        try:
            url = url1 % urllib.quote(title)
        except:
            continue
        count += 1
        data    = urllib.urlopen(url).read()
        hdoc    = TextResponse(url=url, body=data)
        sel     = Selector(hdoc)
        results = sel.xpath('//result')
        flag    = False
        for result in results:
            typ = ''.join(result.xpath('.//type[1]//text()').extract())
            addr = ''.join(result.xpath('.//formatted_address//text()').extract())
            if typ == "stadium":
                flag = True
                try:
                    address.write("%s#<>#%s#<>#%s#<>#%s\n" %(id_, gid, title, addr))
                except:
                    print id_, gid, title, addr
        
        if not flag:
            not_available.write("%s\n" %gid)
