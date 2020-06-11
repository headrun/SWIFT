import MySQLdb

def main():

    cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB",charset='utf8', use_unicode=True).cursor()
    data = open('souce_mapping', 'r+')
    for data_ in data:
        sk_    = data_.split(' ')[0].strip()
        new_sk = data_.split(' ')[1].strip()
        up_qry = 'update sports_source_keys set source_key=%s where source_key=%s and entity_type = "game" limit 1'
        values = (new_sk, sk_)
        cursor.execute(up_qry, values)
    cursor.close()

if __name__ == '__main__':
    main()


