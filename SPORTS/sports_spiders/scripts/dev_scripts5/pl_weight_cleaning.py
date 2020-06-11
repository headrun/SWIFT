import MySQLdb
import datetime

PL_LBS_QRY = 'select id, participant_id, weight from sports_players where weight like "%lb%" and weight not like "%/%"'
PL_DOT_QRY = 'select id, participant_id, weight from sports_players where weight like "%.%" and weight not like "%/%"'
UPDATE_WEIGHT = 'update sports_players set weight = "%s" where id = %s limit 1'
UPDATE_KGS_QRY = 'select id, participant_id, weight from sports_players where weight like "%kgs%" and weight not like "%/%"'
UP_KG_LNTH_QRY = 'select id, participant_id, weight from sports_players where length(weight) =2'
UP_KG_QRY = 'select id, participant_id, weight from sports_players where weight like "%kg%" and weight not like "% kg%"'

Auto_Qry = 'select  id from sports_participants where game = "football" and participant_type="player" and reference_url like "%nfl.com%"'
FL_QRY = 'select  id from sports_participants where game = "football" and participant_type="player"'
HL_QRY = 'select id from sports_participants where game = "hockey" and participant_type="player" and reference_url like "%nhl.com%"'
Auto_up_qry = 'select id, weight from sports_players where  weight != "" and participant_id = %s'


class PlayersWeightCleaning:

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        #self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()


    def main(self):

        #self.cursor.execute(PL_LBS_QRY)
        #self.cursor.execute(PL_DOT_QRY)
        #self.cursor.execute(UPDATE_KGS_QRY)
        #self.cursor.execute(UP_KG_QRY)
        #self.cursor.execute(UP_KG_LNTH_QRY)
        #self.cursor.execute(Auto_Qry)
        #self.cursor.execute(FL_QRY)
        self.cursor.execute(HL_QRY)

        data = self.cursor.fetchall()
        for data_ in data:
            id_ = data_[0]
            values = (id_)
            self.cursor.execute(Auto_up_qry %(values))
            data_ = self.cursor.fetchall()
            if data_:
                id_ = data_[0][0]
                weight = data_[0][1]
                if "kg" not in weight:
                    up_weight = float(weight.strip()) * float(0.453592)
                    up_weight = round(float(up_weight))
                    up_weight = str(up_weight).replace('.0', '') + " kg"
                    values = (up_weight, id_)
                    print up_weight
                    self.cursor.execute(UPDATE_WEIGHT %(values))

        data = self.cursor.fetchall()
        for data_ in data:
            id_ = data_[0]
            pl_id = data_[1]
            weight = data_[2]
            if "        " in weight:
                continue
            print weight
            up_weight = weight.strip().replace('kgs', '').replace('kg', '').strip()
            #up_weight = float(weight.strip().replace('lbs', '').replace('.', '').replace('lb', '')) * float(0.453592)
            up_weight = round(float(up_weight))
            up_weight = str(up_weight).replace('.0', '') + " kg"
            print up_weight
            values = (up_weight, id_)
            self.cursor.execute(UPDATE_WEIGHT %(values))





if __name__ == '__main__':
    OBJ = PlayersWeightCleaning()
    OBJ.main()

