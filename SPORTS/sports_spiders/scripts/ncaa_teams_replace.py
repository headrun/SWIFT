import MySQLdb

def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB").cursor()
    #query  = 'select id, group_name, aka from sports_tournaments_groups where group_name like "%Mountain West Conference%"'
    query = 'select id, title from sports_tournaments where title like "%Rogers cup%"'
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        _id = event[0]
        aka = event[1]
        title = event[1].replace("Rogers Cup", "Canadian Open")
        '''title = event[1].replace("American Athletic Conference", "AAC").replace('Atlantic Coast Conference', 'ACC').replace('Southeastern Conference', 'SEC').replace('Pac-12 Conference', 'Pac-12').replace('Big 12 Conference', 'Big 12').replace('Big Ten Conference', 'Big Ten').replace('Conference USA', 'C-USA').replace('Big East Conference', 'Big East').replace('Atlantic 10 Conference', 'Atlantic 10').replace('Mountain West Conference', 'MWC')'''
        if title: 
            up_qry = 'update sports_tournaments set title=%s, aka=%s where id = %s  limit 1'
            values = (title, aka, _id)
            cursor.execute(up_qry, values)

    '''cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="DATATESTDB").cursor()
    #query = 'select id, record from test_cases where record like "% Conference%" and last_modified like "%2016-07%"'
    query = 'select id, record from test_cases where suite_id =117 and record like "%#TOU313#%"'
    cursor.execute(query)
    data = cursor.fetchall()
    for event in data:
        _id = event[0]
        title = event[1].replace("American Athletic Conference", "AAC").replace('Atlantic Coast Conference', 'ACC').replace('Southeastern Conference', 'SEC').replace('Pac-12 Conference', 'Pac-12').replace('Big 12 Conference', 'Big 12').replace('Big Ten Conference', 'Big Ten').replace('Conference USA', 'C-USA').replace('Big East Conference', 'Big East').replace('Atlantic 10 Conference', 'Atlantic 10').replace('Mountain West Conference', 'MWC').replace('American Athletic', 'AAC').replace('Mountain West', 'MWC')
        if title:
            up_qry = 'update test_cases set record = %s where id = %s limit 1'
            values = (title, _id)
            cursor.execute(up_qry, values) '''

    cursor.close()

if __name__ == '__main__':
    main()

