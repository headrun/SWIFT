import MySQLdb
import sys
from optparse import OptionParser
import datetime
from datetime import date, timedelta


class CheckDuplicates:

    def mysql_connection(self):
        conn = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB")
        cursor = conn.cursor()
        return conn, cursor

    def main(self):
        conn, cursor = self.mysql_connection()
	today_datetime = datetime.date.today()
	t_dt = today_datetime.strftime('%Y-%m-%d')
        query = 'select entity_id from sports_source_keys where source_key like "'+t_dt+'%tbd%" and source="MLS" '
        game_ids = cursor.execute(query)
        game_id = cursor.fetchall()
        game_sk =[]
        [game_sk.append(str(da[0])) for da in game_id]
        game_query = 'select id from sports_games where id in %s and status!="Hole"'%(str(tuple(game_sk)).replace(',)',')'))
        values = cursor.execute(game_query)
        g_id= cursor.fetchall()
        game_keys  = []
        [game_keys.append(str(da[0])) for da in g_id]
        if g_id != ():
            update_status = 'update sports_games set status="Hole" where id in %s'%(str(tuple(game_keys)).replace(',)',')'))
            cursor.execute(update_status)    

        conn.close()
        cursor.close()



if __name__ == '__main__':
    OBJ = CheckDuplicates()
    OBJ.main()
