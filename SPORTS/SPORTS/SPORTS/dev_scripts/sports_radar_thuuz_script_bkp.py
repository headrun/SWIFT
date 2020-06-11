import datetime
import MySQLdb.cursors
import os
import json

class SportsRadarTestCST():
    def __init__(self):
        self.conn, self.cursor = self.create_cursor()

    def get_radar_stats(self, tou_id):
        qry = 'select radar_id from SPORTSRADARDB.sports_radar_merge where type = "tournament" and sportsdb_id = %s' 
        self.cursor.execute(qry, (tou_id))
        try:
            radar_ids = [i['radar_id'] for i in self.cursor.fetchall() if 'sr:' not in i['radar_id']]
            final_id = radar_ids[0]
            print radar_ids
            count_qry = 'select count(*) as count from SPORTSRADARDB.sports_games where tournament_id = %s'
            self.cursor.execute(count_qry, (final_id))
            radar_total =  self.cursor.fetchoneDict()['count']
            merged_qry = 'select count(*) as count from SPORTSRADARDB.sports_games where tournament_id = %s and id collate utf8_unicode_ci in (select radar_id from SPORTSRADARDB.sports_radar_merge where type ="game")'
            self.cursor.execute(merged_qry, (final_id))
            merged_total = self.cursor.fetchoneDict()['count']
            return {'total': radar_total, 'merged': merged_total}
            
        except Exception as e:
            return {'total': 0, 'merged': 0}

    def get_thuuz_stats(self, thuuz_id):
        try:
            count_qry = 'select count(*) as count from SPORTSRADARDB.sports_thuuz_games where Ti = %s'
            self.cursor.execute(count_qry, (thuuz_id))
            _total =  self.cursor.fetchoneDict()['count']
            merged_qry = 'select count(*) as count from SPORTSRADARDB.sports_thuuz_games where Ti = %s and Gi collate utf8_unicode_ci in (select thuuz_id from SPORTSRADARDB.sports_thuuz_merge where type ="game")'
            self.cursor.execute(merged_qry, (thuuz_id))
            merged_total = self.cursor.fetchoneDict()['count']
            return {'total': _total, 'merged': merged_total}
        except:
            return {'total': 0, 'merged': 0}

    def get_rovi_stats(self, tou_id):
        try:
            tou_sea_qry = 'select season_start, season_end, sport_id title from sports_tournaments where id = %s'
            self.cursor.execute(tou_sea_qry, (tou_id))
            result_dict = self.cursor.fetchoneDict()
            season_start, season_end, _title = result_dict['season_start'] , result_dict['season_end'], result_dict['title']
            sport_id = result_dict['sport_id']
            tou_qry = 'select count(*) as count from sports_games where tournament_id = %s and status != "Hole" and game_datetime between %s and %s'
            self.cursor.execute(tou_qry, (tou_id, season_start, season_end))
            _total = self.cursor.fetchoneDict()['count']
            tou_merge_qry = 'select count(*) as count from sports_games where tournament_id = %s and status != "Hole" and game_datetime between %s and %s and id in (select game_id from sports_rovi_games_merge)'
            self.cursor.execute(tou_qry, (tou_id, season_start, season_end))
            merge_total = self.cursor.fetchoneDict()['count']
            return {'total': _total, 'merged': merge_total, 'title': _title, 'sport_id': sport_id}
        except Exception as e:
            print e
            return {'total': 0, 'merged': 0, 'title': '', 'sport_id': ''}
        
            
        

    def run_queries(self):
        sports_merge_stats = {}
        query = 'select thuuz_id, sportsdb_id from SPORTSRADARDB.sports_thuuz_merge where type = "tournament"'
        self.cursor.execute(query)
        sports_thuuz_games_merge = self.cursor.fetchallDict()

        for record in sports_thuuz_games_merge:
            tournament_id = record['sportsdb_id']

            if tournament_id not in sports_merge_stats.keys():
                sports_merge_stats.setdefault(tournament_id, {})
                sports_merge_stats[tournament_id].setdefault('radar', {})
                sports_merge_stats[tournament_id].setdefault('thuuz', {})
                sports_merge_stats[tournament_id].setdefault('total', 0)
                sports_merge_stats[tournament_id].setdefault('rovi', {})

            radar_info = self.get_radar_stats(tournament_id)
            thuuz_info = self.get_thuuz_stats(record['thuuz_id'])
            rovi_info = self.get_rovi_stats(tournament_id) 

            sports_merge_stats[tournament_id]['radar'] = radar_info
            sports_merge_stats[tournament_id]['thuuz'] = thuuz_info
            sports_merge_stats[tournament_id]['total'] = rovi_info['total']
            sports_merge_stats[tournament_id]['rovi']  = rovi_info


        for tournament_id, record in sports_merge_stats.iteritems():
            radar_percentage = round(((float(record['radar']['merged'])/float(record['radar']['total']))*100), 2)
            thuuz_percentage = round(((float(record['thuuz']['merged'])/float(record['thuuz']['total']))*100), 2)
            rovi_percentage = round(((float(record['rovi']['merged'])/float(record['rovi']['total']))*100), 2)

            tournament_name = record['rovi']['title']

                

            aux_info = dict(radar_percentage=radar_percentage, thuuz_percentage=thuuz_percentage,
        tournament_name=tournament_name, tournament_id=tournament_id, radar_count=record['radar']['merged'],radar_total_count=record['radar']['total'], thuuz_count=record['thuuz']['merged'],total_count=record['total'], sport_id=record['rovi']['sport_id'], game_status='', sport_name=record['rovi']['sport_id'], rovi_percentage=rovi_percentage, rovi_count=record['rovi']['merged'])

            query = 'insert into WEBSOURCEDB.SportsRadarThuzzMerge(tournament_id, aux_info, run_date, created_at, modified_at)'
            query += ' values(%s, %s, curdate(), now(), now())on duplicate key update modified_at=now(), aux_info=%s'
            values = (tournament_id, json.dumps(aux_info), json.dumps(aux_info))
            self.cursor.execute(query, values)
            

        print sports_games_status
        self.cursor.close()
        self.conn.close()

    def create_cursor(self):
        conn = MySQLdb.connect(db='SPORTSDB', host='10.28.218.81', user="veveo",
            passwd="veveo123", cursorclass=MySQLdb.cursors.DictCursor)
        cursor = conn.cursor()
        return conn, cursor


if __name__ == '__main__': 
    obj = SportsRadarTestCST()
    obj.run_queries()
