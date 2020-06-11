import MySQLdb

def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB",charset='utf8', use_unicode=True).cursor()
    query = 'select SP.id, SP.title from sports_participants SP, sports_groups_participants GP, sports_tournaments_groups TG where GP.participant_id=SP.id and TG.id=GP.group_id and TG.group_name like "%Olympics -%"'
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        _id = event[0]
        title = event[1]
        up_title = ''
        if " men's"  in title:
            up_title = title.split(" men's")[0].strip()
        if " women's" in title:
            up_title = title.split(" women's")[0].strip()
        if "men's" not in title and 'national' in title:
            up_title = title.split(' national')[0].strip()
        if "Olympic" in title:
            up_title = title.split('Olympic')[0].strip()
        if up_title:
            up_title = up_title.replace('United States', 'USA')
            up_qry = 'update sports_teams set short_title=%s, display_title=%s where participant_id=%s limit 1'
            values = (up_title, up_title, _id)
            cursor.execute(up_qry, values)
        loc_qry = 'select iso from sports_locations where country=%s and iso !="" limit 1'
        up_title = up_title.replace('United States', 'USA')
        values = (up_title)
        cursor.execute(loc_qry, values)
        data =  cursor.fetchone()
        if data:
            iso = data[0]
            up_qry = 'update sports_teams set callsign=%s where participant_id=%s limit 1'
            values = (iso, _id)
            cursor.execute(up_qry, values)
    '''query = 'select id, participant_id, short_title, display_title, callsign from sports_teams where participant_id in (select id from sports_participants where title like "%2016 summer%")'
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        _id = event[0]
        par_id = event[1]
        up_title = event[2]
        loc_qry = 'select iso from sports_locations where country=%s and iso !="" limit 1'
        up_title = up_title.replace('United States', 'USA')
        values = (up_title)
        cursor.execute(loc_qry, values)
        data =  cursor.fetchone()
        if data:
            iso = data[0]
            up_qry = 'update sports_teams set callsign=%s where participant_id=%s limit 1'
            values = (iso, par_id)
            cursor.execute(up_qry, values)'''

    cursor.close()

if __name__ == '__main__':
    main()
