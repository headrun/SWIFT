import MySQLdb
import random
import optparse
parser = optparse.OptionParser()
parser.add_option("--league", dest="league", help="enter league")
(options, arguments) = parser.parse_args()
league = options.league


class GettyImagesFlag():

    def __init__(self):

        self.conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.getty_players = "select M.entity_id from GETTY_IMAGES.getty_players_mapping M, GETTY_IMAGES.getty_players P where M.getty_player_id=P.getty_player_id and P.league='%s' and P.is_crawled='1'"%(league)
        self.image_max_weight = "select max(I.weight) from sports_images I, sports_images_mapping M   where I.image_type='%s' and I.id=M.image_id and M.entity_id='%s' and I.id not in (select image_id from sports_getty_images_status where status = 9) "
        self.image_types = ['headshots', 'actionshots']

    def main(self):
        self.cursor.execute(self.getty_players)
        participant_ids = self.cursor.fetchall()
        for part_id in participant_ids:
            participant_id = part_id[0]
            try:
                up_al_flag = 'update sports_images_status set flag="%s" where user_id="%s" and flag="%s"'
                values= (0, participant_id, 1)
                self.cursor.execute(up_al_flag, values)
                self.conn.commit()
            except:
                print participant_id
            for type_ in self.image_types:
                values = (type_, participant_id)
                self.cursor.execute(self.image_max_weight % values)
                map_wei = self.cursor.fetchall()
                if map_wei:
                    for map_we in map_wei:
                        image_we_id = map_we[0]
                        ima_id = "select I.id from sports_images I, sports_images_mapping M where I.weight='%s' and I.image_type='%s' and I.id=M.image_id and M.entity_id='%s' and I.id not in (select image_id from sports_getty_images_status where status = 9) "
                        values = (image_we_id, type_, participant_id)
                        self.cursor.execute(ima_id % values)
                        image_id = self.cursor.fetchall()
                        try:
                            up_img_id = image_id[0][0]
                            up_qry = 'update sports_images_status set flag="1" where image_id="%s" and user_id="%s"'
                            val = (up_img_id, participant_id)
                            self.cursor.execute(up_qry%val)
                            self.conn.commit()
                        except:
                            continue
        self.cursor.close()
        self.conn.close()

if __name__ == '__main__':
    GettyImagesFlag().main()

