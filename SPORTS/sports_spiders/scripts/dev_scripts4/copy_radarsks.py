import MySQLdb

INSERT_SK = 'insert ignore into sports_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'

INSERT_RECORD = 'insert into sports_radar_images_mapping (entity_id, entity_type, image_id, created_at, modified_at) values(%s, %s, %s, now(), now())'


UPDATE_Q = 'update sports_radar_images_mapping set image_id=%s where entity_type="player" and image_id=%s'

class PopulateSks:

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB_BKP")
        self.cursor = self.conn.cursor()

        self.dev_conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        self.dev_cursor = self.dev_conn.cursor()

    def main(self):
        query = 'select id, url_sk from sports_radar_images'
        self.dev_cursor.execute(query)
        records = self.dev_cursor.fetchall()
        for record in records:
            id_, image_sk = record
            values = (id_, image_sk)
            self.cursor.execute(UPDATE_Q, values)


if __name__ == '__main__':
    OBJ = PopulateSks()
    OBJ.main()
