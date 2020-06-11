import MySQLdb

IN_QRY = 'insert into sports_groups_participants(group_id, participant_id, season, created_at, modified_at) values(%s, %s, %s, now(), now()) on duplicate key update modified_at=now()'
def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB",charset='utf8', use_unicode=True).cursor()
    query = 'select SP.id, SP.title from sports_participants SP, sports_groups_participants GP, sports_tournaments_groups TG where GP.participant_id=SP.id and TG.id=GP.group_id and TG.group_name like "%Olympics -%"'
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        _id = event[0]
        title = event[1].replace('China PR', 'China')
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
            grou_name = up_title + " at the Olympics"
            sel_qry = 'select id from sports_tournaments_groups where group_name=%s'
            values = (grou_name)
            cursor.execute(sel_qry, values)
            data =cursor.fetchone()
            if data:
                gp_id =data[0]
                values = (gp_id, _id, '2016')
                cursor.execute(IN_QRY, values)
            else:
                print _id


    cursor.close()

if __name__ == '__main__':
    main()

