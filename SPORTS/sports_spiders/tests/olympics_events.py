# -*- coding: utf-8 -*-
from vtv_task import VtvTask, vtv_task_main
import sys 
import MySQLdb

class OlympicsEvents(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
	self.evetdict = {"Olympics - Men's Rugby Sevens": ['Quarterfinals', 'Semifinals', 'Bronze Medal Match', 'Gold Medal Match'], \
			"Olympics - Women's Rugby Sevens": ['Quarterfinals', 'Semifinals', 'Bronze Medal Match', 'Gold Medal Match']}

    def eventsinformation(self):
       for key, values in self.evetdict.iteritems():
            tou_name = key
            event_list = values
            tou_details = 'select game, gender, season_start, season_end, image_link, reference_url, id, sport_id from sports_tournaments where title=%s and subtype="summer"'
            tou_det = (tou_name)
            self.cursor.execute(tou_details, tou_det)
            tou_data = self.cursor.fetchall()
            game = tou_data[0][0]
            gender = tou_data[0][1]
            start = str(tou_data[0][2])
            end = str(tou_data[0][3])
            ref = tou_data[0][5] 
            image = tou_data[0][4]
            tou_id = tou_data[0][6]
	    sport_id = tou_data[0][7]
            for event in event_list:
                event = tou_name + " - " +event
                query = "select id from sports_tournaments where title=%s"
                values = (event)
                self.cursor.execute(query, values)
                db_name = self.cursor.fetchall()
                print db_name
                if not db_name:
                    self.cursor.execute('select id, gid from sports_tournaments where id in (select max(id) from sports_tournaments)')
                    tou_data = self.cursor.fetchall()
                    max_id, max_gid = tou_data[0]

                    next_id = max_id+1
                    next_gid = 'TOU'+str(int(max_gid.replace('TOU', ''))+1)
                    type_ = "event"
                    aff = "IOC"
                    if " men" in tou_name.lower():
                        gender = "male"
                    else:
                        gender = "female"
                    base_popularity = "200"
                    genre = "olympics" 
                    subtype = "summer"
                    keywords = sched=std=loc=stadium=aka=score=''
                    query = "insert into sports_tournaments (id, gid, title, type, subtype, aka, game, sport_id, affiliation, gender, genre, season_start, season_end, base_popularity, keywords, reference_url, schedule_url, scores_url, standings_url, location_ids, stadium_ids, created_at, modified_at, image_link) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now(), %s)"

                    values = (next_id, next_gid, event, type_, subtype, aka, game, sport_id, aff, gender, genre, start, end, base_popularity, keywords, ref, sched, score, std, loc, stadium, image)
                    self.cursor.execute(query, values)
                    if tou_id and next_id:
                        status = '' 
                        sequence_num = ''
                        query = "insert into sports_tournaments_events (tournament_id, event_id, sequence_num, status, created_at, modified_at) values (%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()"
                        values = (tou_id, next_id, sequence_num, status)
                        self.cursor.execute(query, values)


    def run_main(self):
        self.eventsinformation()




if __name__ == '__main__':
    vtv_task_main(OlympicsEvents)

