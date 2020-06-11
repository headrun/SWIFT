#!/usr/bin/env python
import sys
import optparse
import re
import time
import datetime
import MySQLdb

UP_QRY = 'update sports_games set sport_id=%s, game=%s where id=%s limit 1'
#IST_QRY = 'insert into sports_teams(id,participant_id, short_title, callsign, category, keywords, tournament_id, division,  gender,  formed, timezone, logo_url, vtap_logo_url, youtube_channel, stadium_id, status, created_at, modified_at, display_title) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
IST_QRY = 'insert into sports_groups_results(id, group_id, participant_id, result_type, result_value, season, created_at, modified_at)values(%s, %s, %s, %s, %s, %s, %s, %s)'

def main():

    cursor = MySQLdb.connect(host='10.28.216.45', user="veveo", passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True).cursor()
    isp_cursor = MySQLdb.connect(host='10.28.218.81', user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True).cursor()
    query  = "select * from sports_groups_results where group_id=25 and season='2014'"
    cursor.execute(query)
    data = cursor.fetchall()

    for event in data:
        id_, group_id, participant_id, result_type, result_value, season, created_at, modified_at = event
        values = (id_, group_id, participant_id, result_type, result_value, season, created_at, modified_at)
        isp_cursor.execute(IST_QRY, values)

    cursor.close()

if __name__ == '__main__':
    main()

