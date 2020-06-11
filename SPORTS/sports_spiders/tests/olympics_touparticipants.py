import MySQLdb


def main():
    conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
    cursor = conn.cursor()
    sel_qry = 'select participant_id from sports_groups_participants where group_id in (select id from \
                        sports_tournaments_groups where group_name  like "%Olympics -%");'
    cursor.execute(sel_qry)
    data = cursor.fetchall()
    for data_ in data:
        id_ = data_[0]
        insert_qry = 'insert into sports_tournaments_participants (participant_id, tournament_id, status, \
        seed, season, status_remarks, standing, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, \
        now(), now()) on duplicate key update modified_at = now()'
        values = (id_, "461", "active", "", "2016", "", "")
        cursor.execute(insert_qry, values)
    cursor.close()
if __name__ == '__main__':
    main()


