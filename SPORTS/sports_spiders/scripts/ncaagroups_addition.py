# -*- coding: utf-8 -*-
from vtv_task import VtvTask, vtv_task_main
import sys 
import MySQLdb

'''TOU_DICT = {"Women's Basketball": "421", "Women's Soccer": "3207", "Women's Volleyball": "3208", \
		"Women's Softball": "3205", "Men's Soccer": "3206", "Baseball": "489", "Football" : "9", \
        "Men's Hockey": "221", "Women's Hockey": "3212", "Men's Volleyball": "3213"}'''
TOU_DICT = {"Women's Basketball": "3333", "Women's Soccer": "3341", "Women's Volleyball": "3345", \
        "Women's Softball": "3337", "Men's Soccer": "3339", "Baseball": "3331", "Football" : "1072", \
        "Men's Basketball": "3153", "Men's Volleyball": "3420", "Men's Hockey":"3421"}
TOU_DICT = {"Women's Basketball": "3334", "Women's Soccer": "3342", "Women's Volleyball": "3344", \
        "Women's Softball": "3338", "Men's Soccer": "3340", "Baseball": "3332", "Football" : "1073", \
        "Men's Basketball": "3154", "Men's Volleyball": "3343", "Men's Hockey":"3335", "Women's Hockey": "3336"}

class NCAAGroups(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
	'''self.evetdict = {"Big Ten Conference": ["Big Ten Conference Women's Basketball",
						"Big Ten Conference Baseball",
						"Big Ten Conference Men's Soccer",
						"Big Ten Conference Women's Soccer",
						"Big Ten Conference Women's Softball",
						"Big Ten Conference Women's Volleyball",
						"Big Ten Conference Ice Hockey"]}'''
        #self.evetdict = {"Washington Adventist": ['Washington Adventist Women', "Washington Adventist Men"]}
        self.evetdict = {"Washington Adventist Women": ["Washington Adventist Women's Basketball", "Washington Adventist Women's Volleyball", "Washington Adventist Women's Softball", "Washington Adventist Women's Soccer"],
	    "Washington Adventist Men": ["Washington Adventist Men's Soccer", "Washington Adventist Men's Basketball", "Washington Adventist Baseball", "Washington Adventist Men's Volleyball"]}

    def eventsinformation(self):
       for key, values in self.evetdict.iteritems():
            tou_name = key.replace('Washington Adventist', "Washington Adventist Conference")
            event_list = values
            tou_details = 'select id, group_name, group_type, base_popularity, image_link from sports_tournaments_groups where aka=%s'
            tou_det = (tou_name)
	    replace_name = tou_name.replace(' Men', '').replace(' Women', '')
	    
            self.cursor.execute(tou_details, tou_det)
            tou_data = self.cursor.fetchall()
            id_ = tou_data[0][0]
            title = tou_data[0][1]
            type_ = str(tou_data[0][2])
            bop = str(tou_data[0][3])
            image = tou_data[0][4] 
            for event in event_list:
                event = event
                aka = event.replace('Washington Adventist', "Washington Adventist Conference")
                query = "select id from sports_tournaments_groups where aka=%s"
                values = (event)
                self.cursor.execute(query, values)
                db_name = self.cursor.fetchall()
                print db_name
		tou_id_name = event.replace(replace_name, '').strip()
		tou_id = TOU_DICT.get(tou_id_name, '') 
                if not db_name:
                    self.cursor.execute('select id, gid from sports_tournaments_groups where id in (select max(id) from sports_tournaments_groups)')
                    tou_data = self.cursor.fetchall()
                    max_id, max_gid = tou_data[0]

                    next_id = max_id+1
                    next_gid = 'GR'+str(int(max_gid.replace('GR', ''))+1)
		    query = "insert into sports_tournaments_groups(id, gid, group_name, sport_id, keywords, aka, tournament_id, group_type, info, base_popularity, image_link, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()"

                    values = (next_id, next_gid, event, '96', '', aka, tou_id, type_, '', bop, image)
                    self.cursor.execute(query, values)
                    if id_ and next_id:
                        query = "insert into sports_subgroups(group_id, subgroup_id, created_at, modified_at) values (%s, %s, now(), now()) on duplicate key update modified_at=now()"
                        values = (id_, next_id)
                        self.cursor.execute(query, values)


    def run_main(self):
        self.eventsinformation()




if __name__ == '__main__':
    vtv_task_main(NCAAGroups)

