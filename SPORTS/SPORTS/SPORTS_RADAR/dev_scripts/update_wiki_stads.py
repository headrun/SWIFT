
import MySQLdb

conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset="utf8", use_unicode=True)
cur = conn.cursor()

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

f = open('stadiums_deleted_merge.txt', 'w')
k = open('stadiums_duplicate_enties.txt', 'w')

input_lines = open('final_stad_loc_output.txt').readlines()
for line in input_lines:
    if '###' not in line or '###C' in line: continue
    wiki_gid = line.split('<>')[0].replace('<WIKI>', 'WIKI').strip()
    child_gid = line.split('<>')[1].strip()
    wiki_title = line.split(':-:-:-:')[-1].split('<>')[0].strip()
    if '###T' in line:
        qry = 'update sports_stadiums set title = %s where gid = %s limit 1'
        vals = (wiki_title, child_gid)
        try:
            cur.execute(qry, vals)
        except:
            k.write('%s'%line)
    elif '###W' in line:
        qry = 'delete from GUIDMERGE.sports_wiki_merge where exposed_gid = %s and child_gid = %s'
        vals = (wiki_gid, child_gid)
        cur.execute(qry, vals)
    else:
        pass         
