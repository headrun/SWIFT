import MySQLdb


class RadarImagesCheck():
    def __init__(self):
        #self.conn   = MySQLdb.connect(host="10.28.216.45", user="veveo", db="SPORTSDB_DEV", charset='utf8', use_unicode=True)
        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo",
                                    passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def main(self):
        self.cursor.execute(
            'select count(*) from sports_source_keys where entity_type="participant" and source="radar" and entity_id not in (select id from sports_participants)')
        data = self.cursor.fetchone()
        print "Number of playes present in radar source_keys, but not present in sposrts_participants : %s" % (
            data[0])

        self.cursor.execute(
            'select count(*) from sports_radar_images_mapping where entity_type = "player" and entity_id  not in (select id from sports_participants)')
        data = self.cursor.fetchone()
        print "Number of players present in sports_radar_images_mapping and not present in sports_participats : %s" % (
            data[0])
        self.cursor.execute(
            'select count(*) from sports_radar_images_mapping where entity_type = "team" and entity_id  not in (select id from sports_participants)')
        data = self.cursor.fetchone()
        print "Number of teams present in sports_radar_images_mapping and not present in sports_participats : %s" % (
            data[0])
        self.cursor.execute(
            'select count(*) from sports_radar_images_mapping where entity_type = "game" and entity_id  not in (select id from sports_games)')
        data = self.cursor.fetchone()
        print "Number of games present in sports_radar_images_mapping and not present in sports_games : %s" % (
            data[0])

        self.cursor.execute(
            'select count(*) from sports_radar_tags where entity_type = "player" and entity_id  not in (select id from sports_participants)')
        data = self.cursor.fetchone()
        print "Number of players present in sports_radar_tags and not present in sports_participats : %s" % (
            data[0])

        self.cursor.execute(
            'select count(*) from sports_radar_images_mapping where image_id not in (select id from sports_radar_images)')
        data = self.cursor.fetchone()
        print "Number of image_id present in sports_radar_images_mapping and not present in sports_radar_images :%s" % (
            data[0])

        self.cursor.execute(
            'select count(*) from sports_radar_tags where entity_type = "team" and entity_id  not in (select id from sports_participants)')
        data = self.cursor.fetchone()
        print "Number of teams present in sports_radar_tags and not present in sports_participats :%s" % (
            data[0])

        self.cursor.execute(
            'select count(*) from sports_radar_tags where image_id not in (select id from sports_radar_images)')
        data = self.cursor.fetchone()
        print "Number of image_id present in sports_radar_tags and not present in sports_radar_images :%s" % (
            data[0])

        self.cursor.execute(
            'select count(*) from (select distinct(entity_id), count(*) from sports_source_keys where source="radar" and entity_type ="participant" group by entity_id having count(*) > 1 order by count(*) desc) a')
        data = self.cursor.fetchone()
        print "Number of playes having more than one radar source_key in sports_source_keys: %s" % (
            data[0])

        self.cursor.execute('select count(*) from sports_radar_images_mapping Imm, sports_radar_images Im, sports_participants P where P.id=Imm.entity_id and Im.id=Imm.image_id and P.game="football" and Imm.entity_type="player" and Im.image_url not like "%nfl%" and Im.image_url not like "%ncaafb%"')
        data = self.cursor.fetchone()
        print "Number of football players having wrong image mapping :%s" % (
            data[0])

        self.cursor.execute('select count(*) from sports_radar_images_mapping Imm, sports_radar_images Im, sports_participants P where P.id=Imm.entity_id and Im.id=Imm.image_id and P.game="baseball" and Imm.entity_type="player" and Im.image_url not like "%mlb%"')
        data = self.cursor.fetchone()
        print "Number of baseball players having wrong image mapping :%s" % (
            data[0])

        self.cursor.execute('select count(*) from sports_radar_images_mapping Imm, sports_radar_images Im, sports_participants P where P.id=Imm.entity_id and Im.id=Imm.image_id and P.game="soccer" and Imm.entity_type="player" and Im.image_url not like "%soccer%" and Im.image_url not like "%mls-%"')
        data = self.cursor.fetchone()
        print "Number of soccer players having wrong image mapping :%s" % (
            data[0])

        self.cursor.execute('select count(*) from sports_radar_images_mapping Imm, sports_radar_images Im, sports_participants P where P.id=Imm.entity_id and Im.id=Imm.image_id and P.game="basketball" and Imm.entity_type="player" and Im.image_url not like "%nba%" and Im.image_url not like "%ncaamb%"')
        data = self.cursor.fetchone()
        print "Number of basketball players having wrong image mapping :%s" % (
            data[0])

        self.cursor.execute('select count(*) from sports_radar_images_mapping Imm, sports_radar_images Im, sports_participants P where P.id=Imm.entity_id and Im.id=Imm.image_id and P.game="hockey" and Imm.entity_type="player" and Im.image_url not like "%nhl%"')
        data = self.cursor.fetchone()
        print "Number of hockey players having wrong image mapping :%s" % (
            data[0])

        self.cursor.execute('select count(*) from sports_radar_images_mapping Imm, sports_radar_images Im, sports_participants P where P.id=Imm.entity_id and Im.id=Imm.image_id and P.game="golf" and Imm.entity_type="player" and Im.image_url not like "%golf%"')
        data = self.cursor.fetchone()
        print "Number of golf players having wrong image mapping :%s" % (
            data[0])

        self.cursor.execute('select count(*) from sports_radar_images_mapping Imm, sports_radar_images Im, sports_participants P where P.id=Imm.entity_id and Im.id=Imm.image_id and P.game="cricket" and Imm.entity_type="player" and Im.image_url not like "%cricket%"')
        data = self.cursor.fetchone()
        print "Number of cricket players having wrong image mapping :%s" % (
            data[0])

        self.cursor.execute('select count(*) from sports_radar_images_mapping Imm, sports_radar_images Im, sports_participants P where P.id=Imm.entity_id and Im.id=Imm.image_id and P.game="auto racing" and Imm.entity_type="player" and Im.image_url not like "%nascar%"')
        data = self.cursor.fetchone()
        print "Number of auto racing players having wrong image mapping :%s" % (
            data[0])


if __name__ == '__main__':
    OBJ = RadarImagesCheck()
    OBJ.main()
