import MySQLdb
def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB",charset='utf8', use_unicode=True).cursor()
    #cursor = MySQLdb.connect(host='10.28.216.45', user="veveo", passwd="veveo123", db="SPORTSDB_DEV",charset='utf8', use_unicode=True).cursor()
    aka_list = open('akas.txt','r+')
    for aka in aka_list:
        aka_id = aka.split(',')[0].strip()
        v =list(set(aka.strip().split(',')[2:]))
        val = "###".join(v)
        sel_qry = 'update sports_participants set aka= %s where id = %s'
        values = (val,aka_id)
        cursor.execute(sel_qry, values)
        data = cursor.fetchone()
    cursor.close()

if __name__ == '__main__':
    main()

