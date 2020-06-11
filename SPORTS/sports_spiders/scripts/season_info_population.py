import genericFileInterfaces
import foldFileInterface
from datetime import datetime
import requests
import json
import time
import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def main():

    sp_cur = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True).cursor()

    tou_dict = {}
    sea_qry = 'select tournament_id, season, season_start, season_end from sports_tournaments_seasons_test'
    sp_cur.execute(sea_qry)
    sp_data = sp_cur.fetchall()
    for data_ in sp_data:
        tou_id, season, season_start, season_end = data_
        pse_qry = 'select max(game_datetime) from sports_games where status != "Hole" and game_datetime < %s and tournament_id = %s'
        pse_vals = (season_start, tou_id)
        sp_cur.execute(pse_qry, pse_vals)
        ps_end_date_datetime = sp_cur.fetchone()[0] or ''
        if ps_end_date_datetime == '': 
            continue
        ps_end_date = ps_end_date_datetime.date()
        if season_start.year == season_end.year:
            date_temp = str(season_start.year - 1)
            _str= '"%'+date_temp+ '%"'
            date_qry = 'select max(game_datetime) from sports_games where tournament_id = %s and game_datetime like %s' %(tou_id, _str)
            sp_cur.execute(date_qry)
            ps_end_datetime = sp_cur.fetchone()[0] or ''
            if ps_end_datetime == '': continue
            ps_end_date = ps_end_datetime.date()
        ps_temp_1 = ''
        pss_date = ''
        temp_cou = 0
        while True:
            if temp_cou == 0:
                check_date = ps_end_date
            ps_vals = (check_date, tou_id)
            sp_cur.execute(pse_qry, ps_vals)
            ps_start_date_time = sp_cur.fetchone()[0] or ''
            if ps_start_date_time == '':
                pss_date = check_date
                break
        
            ps_temp_1 = ps_start_date_time.date()
            check_dats = check_date - ps_temp_1
            if check_dats.days > 30:
                pss_date = check_date
                break
            check_date = ps_temp_1
            temp_cou += 1
        if pss_date.year == ps_end_date.year:
            pre_season = str(pss_date.year)
        else:
            pre_season = str(pss_date.year) + '-' + str(ps_end_date.year - 2000)
        print tou_id, pre_season, pss_date, ps_end_date

        qry = 'insert into sports_tournaments_seasons_test (tournament_id, season, season_start, season_end, created_at, modified_at) values \
              (%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
        vals = (tou_id, pre_season, pss_date, ps_end_date)
        sp_cur.execute(qry, vals)

    sp_cur.close()

if __name__ == '__main__':
    main()

