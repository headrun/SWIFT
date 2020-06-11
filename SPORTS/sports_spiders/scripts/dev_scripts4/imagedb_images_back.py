import MySQLdb
import requests

insert = 'insert into wiki_urlqueue (sk, url, crawl_type, crawl_status, meta_data, created_at, modified_at) values (%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'

class PopulateUrlqueue:
    def __init__(self):
        self.conn = MySQLdb.connect(db='IMAGEDB', host='10.4.2.207', user='root')
        self.cursor = self.conn.cursor()


    def populate_urlqueue(self):
        query = "select image_meta_id, image_url from image_meta meta, source_image_map map where meta.id= map.image_meta_id and source='wiki'"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            sk, url = row
            values = (sk, url, 'keepup', '0', '')
            self.cursor.execute(insert, values)

    def check_urls(self):
        query = 'select count(*) from wiki_urlqueue where crawl_status=0 limit 100'
        self.cursor.execute(query)
        count = self.cursor.fetchone()
        if count and str(count[0]) != '0':
            status = True
        else:
            status = False

        return status

    def dead_check(self):
        query = 'select url from wiki_urlqueue where crawl_status=0 limit 100'
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        for image in records:
            image = image[0]
            try:
                data = requests.get(image)
            except:
                import pdb; pdb.set_trace()
            if data.status_code == 200:
                print 'status is 200'
            else:
                print image

    def main(self):
        #self.populate_urlqueue()
        status = self.check_urls()
        if status ==  True:
            self.dead_check()


if __name__ == '__main__':
    OBJ = PopulateUrlqueue()
    OBJ.main()
