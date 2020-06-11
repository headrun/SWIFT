import MySQLdb
import codecs
k = codecs.open('final_lines.txt','w','utf-8')
conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset="utf8", use_unicode=True)
cur = conn.cursor()
#WIKI39991434<>TEAM101900<>CS Universitatea Craiova<>CS Universitatea Craiova<>team
lines = open('sport_1_25.txt','r').readlines()
for i in lines:
    gi, ti, wt, tt, ty = i.strip(' \n').split('<>')
    print wt, ti
    '''
    if '(' in wt or ',' in wt: 
        k.write('%s'%i.decode('ascii', errors='ignore'))
        continue
    elif 'men' in tt and 'soccer' in tt:
        k.write('%s'%i.decode('ascii', errors='ignore'))
        continue
    '''
    qry = 'update sports_participants set title = %s where gid = %s'
    vals = (tt, ti)
    cur.execute(qry, vals) 
  
