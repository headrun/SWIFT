import urllib2
import MySQLdb

#conn   = MySQLdb.connect(host="10.4.18.183", db="SPORTSDB", user="root")
conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", \
                    db="SPORTSDB", charset='utf8', use_unicode=True)
cursor = conn.cursor()
query = 'select id, reference_url, status from sports_games where status = "hole" and \
             tournament_id = 9 and game_datetime like "%2014-12%"'
cursor.execute(query)
data = cursor.fetchall()
if data:
    count = 0
    for d in data:
        id_, ref, status = d[0], d[1], d[2]
        try:
            code = urllib2.urlopen(ref).code
            if code != 200:
                query = 'update sports_games set status= "Hole" where id = %s'
                cursor.execute(query)
                print "updated game status as hole"
        except:
            print "i am in except"
            print id, ref, status
            count += 1
            print count
