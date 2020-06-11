import MySQLdb

cycling_source = 'select id, entity_id, source_key from sports_source_keys where source="uciworldtour_cycling" and entity_type="participant" and entity_id in (select id from sports_participants where participant_type="player")'


#update_q = 'update sports_source_keys set source_key="%s" where id= %s limit 1'
INSERT = 'insert into sports_source_keys (entity_id, entity_type, \
            source, source_key, created_at, modified_at) \
        values(%s, "%s", "%s", "%s", now(), now()) on duplicate key update modified_at = now()'

class UpdateSourceKey:

    def mysql_connection(self):
        conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
        cursor = conn.cursor()
        return conn, cursor

    def main(self):
        conn, cursor = self.mysql_connection()
        cursor.execute(cycling_source)
        data = cursor.fetchall()
        for sk_data in data:
            id_, entity_id, sk = sk_data
            entity_type  = "participant"
            source = "tourdefrance_cycling"
            if sk:
                cursor.execute(INSERT% (entity_id, entity_type, source, sk))

        conn.close()
if __name__ == '__main__':
    OBJ = UpdateSourceKey()
    OBJ.main()
