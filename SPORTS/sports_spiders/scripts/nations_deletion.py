import MySQLdb

def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB",charset='utf8', use_unicode=True).cursor()
    gid_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="GUIDMERGE",charset='utf8', use_unicode=True).cursor() 
    
    query = 'select id, gid, title from sports_participants where title like "%2016 summe%"'
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        _id = event[0]
        gid = event[1]
        title = event[2]
        part_del = 'delete from sports_participants where id = %s' %(_id )
        cursor.execute(part_del)
        tm_del = 'delete from sports_teams where participant_id=%s' %(_id)
        cursor.execute(tm_del)
        sk_del = 'delete from sports_source_keys where entity_id=%s and entity_type="participant"' %(_id)
        cursor.execute(sk_del)
        tou_del = 'delete from  sports_tournaments_results where participant_id=%s' %(_id)
        cursor.execute(tou_del)
        toup_del = 'delete from sports_tournaments_participants where participant_id=%s' %(_id)
        cursor.execute(toup_del)
        ro_del = 'delete from sports_roster where team_id=%s' %(_id)
        cursor.execute(ro_del)
        gid_del = 'delete from sports_wiki_merge where child_gid=%s limit 1'
        values = (gid)
        gid_cur.execute(gid_del, values)
    cursor.close()
    gid_cur.close()

if __name__ == '__main__':
    main()
