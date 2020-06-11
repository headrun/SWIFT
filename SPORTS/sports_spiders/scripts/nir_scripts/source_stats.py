import MySQLdb


cursor = MySQLdb.connect(host="10.4.18.108", db="MVP_COMMONDB").cursor()
cursor.execute("select id, source_name, country from mvp_source_stats")

records = cursor.fetchall()


for record in records:
    _id, source_name, country = record
    file("MVP", "ab+").write("%s\t%s\t%s\n" %(_id, source_name, country))
