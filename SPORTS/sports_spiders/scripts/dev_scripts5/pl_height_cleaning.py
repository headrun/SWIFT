import MySQLdb
import datetime


PL_METERS_QRY = 'select id, participant_id, height from sports_players where height like "%meter%"'
PL_MET_QRY = 'select id, participant_id, height from sports_players where height like "% m%";'
PL_M_QRY = 'select id, participant_id, height from sports_players where height like "%m%" and height not like "%cm%" and height not like "%.00%"'
PL_CM_QRY = 'select id, participant_id, height from sports_players where height like "%cm%" and height not like "% cm%" and height not like "%.00%"'

PL_FT_QRY = 'select id, participant_id, height from sports_players where height like "%ft%"'
PL_INCH_QRY = '''select id, participant_id, height from sports_players where height like "%'%" and height not like "%/ %"'''
PL_DOT_QRY = 'select id, participant_id, height from sports_players where height like "%.%" and height not like "%m%"'
PL_LENTH_QRY = 'select id, participant_id, height from sports_players where length(height) = 3'

UPDATE_HEIGHT = 'update sports_players set height = "%s" where id = %s limit 1'

class PlayersHeightCleaning:

    def __init__(self):
        #self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()


    def main(self):

        self.cursor.execute(PL_LENTH_QRY)
        data = self.cursor.fetchall()
        for data_ in data:
            id_ = data_[0]
            pl_id = data_[1]
            height = data_[2]
            up_height = height.strip()
            up_height = str(up_height).replace('.0', '') + " cm"
            values = (up_height, id_)
            self.cursor.execute(UPDATE_HEIGHT %(values))


        #self.cursor.execute(PL_FT_QRY)
        self.cursor.execute(PL_INCH_QRY)
        data = self.cursor.fetchall()
        for data_ in data:
            id_ = data_[0]
            pl_id = data_[1]
            height = data_[2].replace("''", "'").replace(' 1/2', '')
            if "      " in height:
                continue
            #update_ft = float(height.split('ft')[0].strip()) * float(30.48)
            if height == "6'":
                update_ft = float(height.split("'")[0].strip()) * float(30.48)
                up_height = round(float(update_ft))
            else:
                update_ft = float(height.split("'")[0].strip()) * float(30.48)
                update_in = float(height.split("'")[1].replace("'", '').replace('"', '').strip()) *float(2.54)
                up_height = round(float(update_ft) + float(update_in))
            #update_in = float(height.split('ft')[-1].strip().replace(',', '').replace('in', '').strip()) * float(2.54)
            up_height = str(up_height).replace('.0', '') + " cm"
            values = (up_height, id_)
            self.cursor.execute(UPDATE_HEIGHT %(values))

        self.cursor.execute(PL_DOT_QRY)
        data = self.cursor.fetchall()
        for data_ in data:
            id_ = data_[0]
            pl_id = data_[1]
            height = data_[2]
            update_ft = float(height.split(".")[0].strip()) * float(30.48)
            update_in = float(height.split(".")[1].strip()) * float(2.54)
            up_height = round(float(update_ft) + float(update_in))
            up_height = str(up_height).replace('.0', '') + " cm"
            values = (up_height, id_)
            self.cursor.execute(UPDATE_HEIGHT %(values))

        self.cursor.execute(PL_CM_QRY)
        data = self.cursor.fetchall()
        for data_ in data:
            id_ = data_[0]
            pl_id = data_[1]
            height = data_[2]
            up_height = height.replace('cm', ' cm')
            values = (up_height, id_)
            self.cursor.execute(UPDATE_HEIGHT %(values))

        self.cursor.execute(PL_METERS_QRY)
        data = self.cursor.fetchall()
        for data_ in data:
            id_ = data_[0]
            pl_id = data_[1]
            height = data_[2]
            up_height = float(height.replace('meters', '').strip()) *100
            up_height = str(up_height).replace('.0', '') + " cm"
            values = (up_height, id_)
            self.cursor.execute(UPDATE_HEIGHT %(values))

        #self.cursor.execute(PL_MET_QRY)
        self.cursor.execute(PL_M_QRY)
        m_data = self.cursor.fetchall()

        for data_ in m_data:
            id_ = data_[0]
            pl_id = data_[1]
            height = data_[2]

            if "/" in height:
                height_ = height.split('/')[0]
                if "m" in height_:
                    height = height_
                else:
                    height_ = height.split('/')[-1]
                    height = height_
            if "." in height:
                up_height = float(height.replace('m', '').strip()) *100
            else:
                up_height = height.replace('m', '').strip()
            up_height = str(up_height).replace('.0', '') + " cm"
            values = (up_height, id_)
            self.cursor.execute(UPDATE_HEIGHT %(values))



if __name__ == '__main__':
    OBJ = PlayersHeightCleaning()
    OBJ.main()

