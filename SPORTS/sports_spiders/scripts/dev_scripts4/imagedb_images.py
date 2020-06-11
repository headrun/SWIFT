import MySQLdb
import requests
import urllib3

from vtv_utils import VTV_CONTENT_VDB_DIR, copy_file, execute_shell_cmd
from vtv_task import VtvTask, vtv_task_main
from vtv_db import get_mysql_connection

insert = 'insert into wiki_urlqueue (sk, url, crawl_type, crawl_status, meta_data, created_at, modified_at) values (%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()'



class DeadImageCheck(VtvTask):
    
    def __init__(self):
        VtvTask.__init__(self)
        self.init_config(self.options.config_file)
        self.db_ip                = self.get_default_config_value('IMAGEDB_IP')
        self.db_name              = self.get_default_config_value('IMAGEDB_NAME')
        self.deleted_images_count = 0

    def populate_urlqueue(self):
        query = "select image_meta_id, image_url from image_meta meta, source_image_map map where meta.id= map.image_meta_id and source='wiki' limit 50"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        for row in rows:
            sk, url = row
            values = (sk, url, 'keepup', '0', '')
            self.cursor.execute(self.insert, values)

    def check_urls(self):
        query = 'select count(*) from wiki_urlqueue where crawl_status=0'
        self.cursor.execute(query)
        count = self.cursor.fetchone()

        if count and str(count[0]) != '0':
            status = True
        else:
            status = False

        return status

    def dead_check(self):
        stats = {}
        deleted_images_count = 0
        query = 'select url, sk from wiki_urlqueue where crawl_status=0 limit 10'

        self.cursor.execute(query)
        records = self.cursor.fetchall()
        for record in records:
            image = record[0]
            sk    = record[1]
            try:
                data = requests.get(image)
            except:
                urllib3.disable_warnings()

            if data.status_code == 200:
                print 'status is 200'
            else:   
                #delete image from image_meta
                delete_from_image_meta_query = 'delete from image_meta where id = "%s" limit 1' % sk
                #self.cursor.execute(delete_from_image_meta_query)
                
                #delete image_meta_id from source_image_map
                delete_from_source_image_map = 'delete from source_image_map where image_meta_id = "%s" limit 1' % sk
                #self.cursor.execute(delete_from_source_image_map)
                self.deleted_images_count += 1
            #update image status urlqueue
            update_crawl_status = 'update wiki_urlqueue set crawl_status=9 where sk = "%s" limit 1' % sk
            #self.cursor.execute(update_crawl_status)

        stats['deleted_images'] = self.deleted_images_count
        print stats



    def run_main(self):
        self.open_cursor(self.db_ip, self.db_name)
        status = self.check_urls()
        if status ==  True:
            self.populate_urlqueue()
            self.dead_check()

        self.cursor.close()


if __name__ == '__main__':
    vtv_task_main(DeadImageCheck)
