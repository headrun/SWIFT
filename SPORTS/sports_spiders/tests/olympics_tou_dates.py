import sys
import MySQLdb


def main():
    conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
    cursor = conn.cursor()

    sel_qry = "select id, title from sports_tournaments where subtype ='summer' and game in ('basketball', 'soccer', 'field hockey', 'water polo', 'handball', 'volleyball')"
    cursor.execute(sel_qry)
    data = cursor.fetchall()
    for data_ in data:
        id_ = data_[0]
	title = data_[1]
	if " Match" not in title and "final" not in title.lower() and "Round" not in title:
		continue
	max_id_qry = 'select max(game_datetime)	from sports_games where event_id=%s'
	values = (id_)
	cursor.execute(max_id_qry, values)
	max_data = cursor.fetchall()
	max_date = max_data[0][0]

	if not max_date:
		continue

	max_date = str(max_date).split(' ')[0]
	min_id_qry = 'select min(game_datetime) from sports_games where event_id=%s'
        values = (id_)
        cursor.execute(min_id_qry, values)
        min_data = cursor.fetchall()
        min_date = min_data[0][0]
	min_date = str(min_date).split(' ')[0]
	up_qry = 'update sports_tournaments set season_start=%s, season_end=%s where id=%s limit 1'
	up_values = (min_date, max_date, id_)
	cursor.execute(up_qry, up_values)	





    cursor.close()
if __name__ == '__main__':
    main()

