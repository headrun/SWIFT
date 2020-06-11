import MySQLdb


def main():
    #conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
    conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd='veveo123', db="SPORTSDB", charset='utf8', use_unicode=True)
    cursor = conn.cursor()
    pl_query = "select id, birth_place from sports_players where birth_place REGEXP '^[0-9]+$'"
    cursor.execute(pl_query) 
    data = cursor.fetchall()
    for data_ in data:
        pl_id = str(data_[0])
        loc_id = str(data_[1])
        loc_query = 'select continent, city, state, country from sports_locations where id=%s' %(loc_id)
        cursor.execute(loc_query)
        data = cursor.fetchone()
        if data:
            continent = data[0]
            city = data[1]
            state = data[2]
            country = data[3]
            if continent and not city and not country:
                up_qry = 'update sports_players set birth_place="" where id =%s limit 1' %(pl_id)
                cursor.execute(up_qry)
            elif city and country:
                loc = city + ", " +country
                up_qry = 'update sports_players set birth_place="%s" where id =%s limit 1' %(loc, pl_id)
                cursor.execute(up_qry)
            elif city:
                up_qry = 'update sports_players set birth_place="%s" where id =%s limit 1' %(city, pl_id)
                cursor.execute(up_qry)
            elif country:
                up_qry = 'update sports_players set birth_place="%s" where id =%s limit 1' %(country, pl_id)
                cursor.execute(up_qry)
            else:
                up_qry = 'update sports_players set birth_place="" where id =%s limit 1' %(pl_id)
                cursor.execute(up_qry)


    cursor.close()

if __name__ == '__main__':
    main()

