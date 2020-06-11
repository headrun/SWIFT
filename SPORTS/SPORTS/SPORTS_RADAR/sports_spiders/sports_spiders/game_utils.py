#!/usr/bin/python

import logging
import configUtils
import os
from redis_utils import get_redis_data
from vtv_utils import vtv_pickle, vtv_unpickle, VTV_SERVER_DIR

from datetime import datetime, timedelta

STEP_BACK = '..'
CWD       = os.getcwd()
UTILS_CFG = 'game_utils.cfg'
STATS_DIR = 'SPORTS_STATS_DIR'
LOGGER_OUT = 'scrapy_spiders.log'
SKS_DIR = 'SKS_DIR'
VTV_SERVER_DIR = "/home/veveo/SPORTS_RADAR/sports_spiders/sports_spiders"
CONFIG = os.path.join(VTV_SERVER_DIR, UTILS_CFG)
STATS_DIR = os.path.join(CWD, STEP_BACK, STEP_BACK, STATS_DIR)
FILE_PATH = os.path.join(CWD, STEP_BACK, STEP_BACK, SKS_DIR)

def clean_data(data):
    try:
        return ''.join([chr(ord(x)) for x in data]).decode('utf8', 'ignore').encode('utf8')
    except ValueError:
        return data.encode('utf8')

def init_logger(log_file, debug_mode = False):
    global log
    log = logging.getLogger(log_file)
    handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=104857600, backupCount=3)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.INFO)

def release_logger():
    handlers = log.handlers[:]
    for handler in handlers:
        handler.close()
        log.removeHandler(handler)

def check_dir_access(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

def check_file_access(file_name):
    if not os.access(file_name, os.F_OK|os.R_OK):
        vtv_pickle(file_name, {}, log)


class SportsdbSetup:

    def __init__(self, item, cursor, spider_class='', gids_file='', hash_conf=None):

        if hash_conf:
            self.hash_conf = hash_conf
        else:
            self.hash_conf = configUtils.readConfFile(CONFIG)
        self.cursor        = cursor
        self.date          = datetime.now()
        self.cur_date      = self.date.date()
        self.item          = item
        self.spider_class  = spider_class
        self.gids_file     = gids_file
        self.rich_data     = None
        self.source        = None
        self.entity_id     = None
        self.source_key    = None
        self.participants  = None
        self.game          = None
        self.game_datetime = None
        self.result        = None
        self.affiliation   = None
        self.game_status   = None
        self.tournament    = None
        self.event         = None
        self.location_info = None
        self.reference_url = None
        self.date_str      = None
        self.stats_file    = None
        self.season        = None
        self.result_type   = None
        self.id_type       = None
        self.time_unknown  = None
        self.tz_info       = None
        self.participant_type = None
        self.result_sub_type = None
        self.sport_name = None
        init_logger(LOGGER_OUT, 'True')
        self.sport_id       = None
        #self.get_sport_ids()
        self.out_file      = open('%s/missed_participants_sks_%s' % (FILE_PATH, self.cur_date), 'a+')

    def get_redis_entity(self, source_key='', entity_type='', source='', entity_id=''):
        query = ''
        if source_key and entity_type and source:
            query = '%s:%s:%s' % (source_key, entity_type, source)
            entity_id = get_redis_data(query, strict=True)
            return entity_id

        for entry in [source_key, entity_type, source]:
            if not entry and query:
                query += '*'
                continue
            if entry == entity_id:
                query += '%s' % entry
                continue
            query += '%s:' % entry
        entity_id = get_redis_data(query)
        return entity_id

    def clean(self):
        release_logger()

    def store_pickle_data(self, data, file_obj):
        if not data:
            data = {}
        pickle_table = vtv_unpickle(file_obj, log)
        if pickle_table is None or not pickle_table:
            pickle_table = {}
            vtv_pickle(file_obj, {}, log)
        for key, value in data.iteritems():
            if key not in pickle_table:
                pickle_table.setdefault(key, set())
            pickle_table[key].add(value)
        vtv_pickle(file_obj, pickle_table, log)

    def check_source_keys(self, source_key, participant_type = ''): # check for sk existance in source_keys table
        try:
            participant = self.participant_type
            if participant_type:
                participant = participant_type
            res = [self.get_redis_entity(source_key=source_key, entity_type=participant, source=self.source)]
            if not res or None in res:
                self.cursor.execute(self.hash_conf['SK_CHECK_QUERY'] %(participant, source_key, self.source)) # check entity_id
                res = [str(i[0]) for i in self.cursor.fetchall()]
        except Exception, error:
            print error

        res_ = '<>'.join(res)

        if len(res)==1:
            return 1, res_
        elif len(res)==0:
            return 0, res_
        else:
            return 2, res_

    def populate_stats(self, stats):
        for result_type, values in stats.iteritems():
            for team, result_values in values.iteritems():
                participant_id = self.get_participant_id(team, 'participant')
                if not participant_id:
                    continue
                counter = 1
                for result_value in result_values:
                    player_id = self.get_participant_id(result_value[0], 'participant')
                    try:
                        if not player_id or int(result_value[1]) == 0:
                            continue
                    except:
                        pass
                    if result_type.endswith('_'):
                        result_id = result_type + 'id_%s' % counter
                        res_value = result_type + 'value_%s' % counter
                        values_1 = (self.entity_id, participant_id, result_id, player_id, player_id)
                        values_2 = (self.entity_id, participant_id, res_value, result_value[1], result_value[1])
                        self.cursor.execute(self.hash_conf['INSERT_SCORES'], values_1)
                        self.cursor.execute(self.hash_conf['INSERT_SCORES'], values_2)
                        counter += 1

    def get_sport_ids(self):
        self.sports_id_dict = {}
        self.cursor.execute('select id, title from sports_types')
        data = self.cursor.fetchall()
        for data_ in data:
            sport_id, sport_title = data_
            self.sports_id_dict[sport_title] = sport_id

    def get_pl_game(self, sport_id):
        query = 'select title from sports_types where id=%s'
        values = (sport_id)
        self.cursor.execute(query, values)
        data = self.cursor.fetchone()
        if data:
            game = data[0]
        else:
            game = ''
        return game

    def get_participant_id(self, participant, participant_type = ''):
        existance, result = self.check_source_keys(participant, participant_type)
        if existance == 1:
            participant_id = result
        else:
            participant_id = 0
        return participant_id

    def get_other_links(self, video_dict):
        links = []

        for k in video_dict.keys():
            linkd = video_dict.get(k, '')
            link = ''
            if linkd and isinstance(linkd, dict):
                link = linkd.get('url', '')
            if link:
                links.append(link)
        return '<>'.join(links)

    def create_game_id(self):
        query = "select auto_increment from information_schema.TABLES where TABLE_NAME='sports_games' and TABLE_SCHEMA='%s'" % self.hash_conf['DB_NAME']
        self.cursor.execute(query)
        count = self.cursor.fetchone()
        u_id =  count[0]
        game_id = 'EV'+str(u_id)
        return u_id, game_id

    def get_tournament_id(self):
        if self.game:
            self.sport_id = self.sport_id
            if not self.sport_id:
                self.sport_id = self.sports_id_dict.get(self.game, '')
            self.cursor.execute('SELECT id FROM sports_tournaments WHERE title=\"%s\" and sport_id=\"%s\"' %(self.tournament, self.sport_id))
        else:
            self.cursor.execute('SELECT id FROM sports_tournaments WHERE title=\"%s\"' %(self.tournament))
	
        data = self.cursor.fetchone()
	if not data:
		self.cursor.execute('SELECT id FROM sports_tournaments WHERE title=\"%s\"' %(self.tournament))
		data = self.cursor.fetchone()	
        if data:
            tid = data[0]
        else:
            tid = 0
        return tid

    def get_group_id(self, title):
        self.cursor.execute('select id from sports_tournaments_groups where group_name = \"%s\"' %(title))
        data = self.cursor.fetchone()
        if data:
            tid = data[0]
        else:
	    self.cursor.execute('select id from sports_tournaments_groups where aka = \"%s\"' %(title))
	    data = self.cursor.fetchone()
	    if data:
		tid = data[0]
	    else:
                tid = data
        return tid

    def update_game_locations(self, loc, stadium):
        tbd_list = ['tbd', 'tba']
        if stadium.lower() in tbd_list:
            return 0, 0
        std = city = state = country = continent = street = latlong = zipcode = ''
        if isinstance(loc, dict):
            std = loc.get('stadium', '').strip()
            city = loc.get('city', '').strip()
            state = loc.get('state', '').strip()
            country = loc.get('country', '').strip()
            continent = loc.get('continent', '').strip()
            street = loc.get('street', '').strip()
            latlong = loc.get('latlong', '').strip()
            zipcode = loc.get('zipcode', '').strip()

        elif loc and isinstance(loc, str):
            if len(loc.split(','))==3:
                std = loc.split(',')[0].strip()
                city = loc.split(',')[1].strip()
                state = loc.split(',')[2].strip()
                country = continent = street = zipcode = latlong = ''
            elif len(loc.split(','))==2:
                std = loc.split(',')[0].strip()
                city = loc.split(',')[1].strip()
                state = country = continent = street = zipcode = latlong = ''
            elif len(loc.split(','))==1:
                city = loc.split(',')[0].strip()
                std = state = country = continent = street = zipcode = latlong = ''
            elif len(loc.split(','))==4:
                country = continent = street = zipcode = latlong = ''
                std = loc.split(',')[0].strip() + ',' + loc.split(',')[1].strip()
                city = loc.split(',')[2].strip()
                state = loc.split(',')[3].strip()

        std_id = 0
        loc_id = 0
        if (city.lower() in tbd_list) or (state.lower() in tbd_list) or (country.lower() in tbd_list):
            return 0, 0

        if (not stadium) and std:
            stadium = std

        if stadium:
            stadium_query = 'select id, location_id from sports_stadiums where title=%s'
            self.cursor.execute(stadium_query, stadium)
            std_data = self.cursor.fetchone()
            if not std_data:
                stadium_title = '%' + stadium + '%'
                query = 'select id, location_id from sports_stadiums where title like %s'
                self.cursor.execute(query, stadium_title)
                std_data = self.cursor.fetchone()
            if std_data:
                std_id, loc_id = [str(std_dt) for std_dt in std_data]


        if (not loc_id or loc_id == 0) and (state or city or country):
            if city:
                query = 'select id from sports_locations where city=%s'
                self.cursor.execute(query, city)
                loc_id = [str(i[0]) for i in self.cursor.fetchall()]
                if len(loc_id) > 1:
                    if state and country:
                        query = 'select id from sports_locations where city=%s and country=%s and state=%s'
                        values = (city, country, state)
                    elif state:
                        query = 'select id from sports_locations where city=%s and state=%s'
                        values = (city, state)
                    elif country:
                        query = 'select id from sports_locations where city=%s and country=%s'
                        values = (city, country)
                    else:
                        query = 'select id from sports_locations where city=%s and country!=""'
                        values = (city)

                    self.cursor.execute(query, values)
                    loc_id = [str(i[0]) for i in self.cursor.fetchall()]
                    if loc_id:
                        loc_id = loc_id[0]
                elif len(loc_id) == 1:
                    loc_id = loc_id[0]
            elif country:
                query = 'select id from sports_locations where country=%s and city="" and state=""'
                self.cursor.execute(query, country)
                result = self.cursor.fetchone()
                if result:
                    loc_id = result[0]
            elif state:
                query = 'select id from sports_locations where state=%s'
                self.cursor.execute(query, state)
                result = self.cursor.fetchone()
                if result:
                    loc_id = result[0]

            '''if (not loc_id or loc_id == 0):
                self.cursor.execute(self.hash_conf['LOC_INSERTION'], (continent, country, state, city, street, zipcode, latlong))
                query = 'select id from sports_locations where continent=%s and country=%s and state=%s and city=%s and street=%s and zipcode=%s and      latlong=%s'
                values = (continent, country, state, city, street, zipcode, latlong)
                self.cursor.execute(query, values)
                loc_id = self.cursor.fetchone()[0]'''

        '''if (not std_id or std_id == 0) and stadium and stadium.lower() !="null":
            self.cursor.execute("select auto_increment from information_schema.TABLES where TABLE_NAME='sports_stadiums' and TABLE_SCHEMA='SPORTSRADARDB'")
            count = self.cursor.fetchone()
            stadium_gid = 'STAD%s' % str(count[0])
            self.cursor.execute(self.hash_conf['STD_INSERTION'], (stadium_gid, stadium, loc_id, loc_id))
            query = 'select id, location_id from sports_stadiums where title=%s and location_id=%s'
            values = (stadium, loc_id)
            self.cursor.execute(query, values)
            std_id = self.cursor.fetchone()
            if std_id:
                std_id, loc_id = std_id'''

        return loc_id, std_id

    def get_eventid(self):
        if not self.event and len(self.event) < 2:
            return 0

        event_name = self.event
        if self.game == 'golf': event_name = self.event + '%'
        self.sport_id = self.sport_id

        if not self.sport_id:
            self.sport_id = self.sports_id_dict.get(self.game, '') 

        if self.affiliation:
            query  = self.hash_conf['TOU_ID_FROM_TITLE_AFF']
            values = (self.sport_id, self.affiliation, event_name)

        else:
            query  = self.hash_conf['TOU_ID_FROM_TITLE']
            values = (self.sport_id, event_name)

        self.cursor.execute(query, values)
        event_id   = self.cursor.fetchall()
        if event_id:
            event_id   = event_id[0][0]
        else:
            if self.affiliation:
                query  = self.hash_conf['TOU_ID_FROM_AKA_AFF']
                values = (self.sport_id, self.affiliation, event_name)
            else:
                query  = self.hash_conf['TOU_ID_FROM_AKA']
                values = (self.sport_id, event_name)
            self.cursor.execute(query, values)
            event_id   = self.cursor.fetchall()
            if event_id:
                event_id =  event_id[0][0]
            else:
                event_id = 0

        return event_id


    def participant_title_check(self, title, callsign):
        self.sport_id = self.sport_id
        if not self.sport_id:
            self.sport_id = self.sports_id_dict.get(self.game, '')
        self.cursor.execute(self.hash_conf['PARTICIPANT_TITLE_AKA_CHECK'] %(title, callsign, self.sport_id, self.affiliation))
        pid = self.cursor.fetchone()
        if not pid:
            self.cursor.execute(self.hash_conf['PARTICIPANT_TITLE_CHECK'] %(title, self.sport_id, self.affiliation))
            pid = self.cursor.fetchone()
        return pid


    def update_participants(self):
        pts = {0: '', 1: ''}
        i = 1
        if isinstance(self.participants, dict):
            pts = set()

        for pt in self.participants:
            if isinstance(self.participants, list):
                callsign = pt.get('callsign', '')
                ptitle = pt.get('name', '')
            else:
                callsign = str(pt)
                ptitle = self.participants[pt][1]

            pid = [callsign]
            if pid and len(pid)==1:
                if isinstance(self.participants, dict):
                    pts.add((self.participants[pt][0], pid[0]))
                else:
                    pts[i] = pid[0]
            else:
                pid = self.participant_title_check(ptitle, callsign)
                if pid and len(pid)==1:
                    pid = str(pid[0])
                    self.cursor.execute(self.hash_conf['PID_EXISTANCE_IN_SKS'], ('participant', pid))
                    pres = self.cursor.fetchone()
                    if not pres:
                        self.cursor.execute(self.hash_conf['INSERT_SKS'], (pid, 'participant', self.affiliation.upper(), callsign))
                    if isinstance(self.participants, dict):
                        pts.add((self.participants[pt][0], pid))
                    else:
                        pts[i] = pid
                elif not pid:
                    pass
                else:
                    print 'Got multiple matches. Need to check manually.!!'

            i = i-1
        return pts

    def update_game_scores(self, pt_ids):
        scores = {0: '', 1: ''}
        quarters = {0: '', 1: ''}
        ot_scores = {0: '', 1: ''}
        winner = {0: '', 1: ''}
        shoot_out = {0: '', 1: ''}
        innings = {0: '', 1: ''}
        errors = {0: '', 1: ''}
        hits = {0: '', 1: ''}
        goal_details = {0: '', 1: ''}

        if isinstance(self.result, dict):
            for pt_sk, values in self.result.iteritems():
                if pt_sk == '0':
                    pt_id = '0'
                else:
                    pt_id = self.check_participants(str(pt_sk))
                    if pt_id:
                        pt_id = pt_id[0]
                if pt_id or pt_sk == '0':
                    for key, value in values.iteritems():
                        if 'winner' in key and isinstance(value, list):
                            for v in value:
                                winner_id = self.check_participants(str(v))
                                if winner_id:
                                    v = winner_id[0]
                                    values = (self.entity_id, v, key, v, v)
                                    self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)
                        elif value != '':
                            if 'jersey' in key:
                                par_id = self.check_participants(value)
                                if par_id:
                                    value = par_id[0]
                            if 'winner' in key or 'innings' in key:
                                if isinstance(value, unicode):
                                    value = value
                                else:
                                    value = str(value)
                                winner_id = self.check_participants(value)
                                if winner_id:
                                    value = winner_id[0]
                                    if self.game in eval(self.hash_conf['TOU_WINNER_LIST']) and self.season is not None:
                                        if self.event and not self.result_sub_type:
                                            tou_id = self.get_eventid()
                                        else:
                                            tou_id = self.get_tournament_id()
                                        if tou_id != 0 and not 'pole_winner' in key:
                                            values = (tou_id, self.season, key, '', value)
                                            self.cursor.execute(self.hash_conf['INSERT_TOU_RESULTS'], values)
                            values = (self.entity_id, str(pt_id), key, value, value)
                            self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)
            return

        for r in self.result:
            if r[0][0] == 0:
                scores[1] = r[1][0]
                quarters[1] = r[2].get('quarters', [])
                ot_scores[1] = r[2]['ot']
                winner[1] = r[2].get('winner', '')
                shoot_out[1] = r[2].get('so', [])
                innings[1] = r[2].get('innings_scores', [])
                errors[1] = r[2].get('errors', '')
                hits[1] = r[2].get('hits', '')
                goal_details[1] = r[2].get('goals', '')
            else:
                scores[0] = r[1][0]
                quarters[0] = r[2].get('quarters', [])
                ot_scores[0] = r[2]['ot']
                winner[0] = r[2].get('winner', '')
                shoot_out[0] = r[2].get('so', [])
                innings[0] = r[2].get('innings_scores', [])
                errors[0] = r[2].get('errors', '')
                hits[0] = r[2].get('hits', '')
                goal_details[0] = r[2].get('goals', '')

            if r[2]['ot']:
                is_ot = True
            else:
                is_ot = False

            if r[2].get('so', []):
                is_so = True
            else:
                is_so = False

        for pt in pt_ids:
            pid, is_home = pt
            pscore = scores[is_home]
            qts = quarters[is_home]
            win = winner[is_home]
            so = shoot_out[is_home]
            ings = innings[is_home]
            ots = ot_scores[is_home]
            er = errors[is_home]
            ht = hits[is_home]
            goals = goal_details[is_home]


            if ings and ots:
                ings.extend(ots)
                ots = ''

            if pscore:
                values = (self.entity_id, str(pid), 'final', pscore, pscore)
                self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)

                for q in range(len(qts)):
                    if 'hockey' in self.game:
                        rt = 'P%s' % (q+1)
                    elif 'football' in self.game or 'basketball' in self.game:
                        rt = 'Q%s' % (q+1)
                    values = (self.entity_id, str(pid), rt, qts[q], qts[q])
                    self.cursor.execute(self.hash_conf['INSERT_SCORES'], values) #UPDATE GAME SCORES

                for i in range(len(goals)):
                    goal = goals[i]
                    goaler =  goal.get('goaler', '')

                    gt = goal.get('goal_time', '')
                    gp = goal.get('quarter', '')
                    if 'OT' not in gp:
                        t = '(P'+gp.split(' ')[0].strip()+')'
                    else: t = '(OT)'
                    goal_time = gt+t

                    if not goaler or not goal_time: continue
                    rt = 'G%s' % (str(i+1))
                    values = (self.entity_id, str(pid), rt, goaler, goaler)
                    self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)

                    rt = 'GT%s' % (str(i+1))
                    values = (self.entity_id, str(pid), rt, goal_time, goal_time)
                    self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)

                if ht:
                    values = (self.entity_id, str(pid), 'H', ht, ht)
                    self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)
                if er:
                    values = (self.entity_id, str(pid), 'E', er, er)
                    self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)

                for i in range(len(ings)):
                    rt = 'I%s' % (i+1)
                    values = (self.entity_id, str(pid), rt, ings[i], ings[i])
                    self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)

                if win == 1 and self.game_status != 'ongoing':
                    values = (self.entity_id, '', 'winner', str(pid), str(pid))
                    self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)

                if so:
                    rt = 'SO'
                    values = (self.entity_id, str(pid), rt, so[0], so[0])
                    self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)

                if 'hockey' in self.game and ots:
                    rt = 'OT'
                    values = (self.entity_id, str(pid), rt, ots[0], ots[0])
                    self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)

                elif 'hockey' not in self.game and ots:
                    for i in range(len(ots)):
                        rt = 'OT%s' % str((i+1))
                        rv = ots[i]
                        values = (self.entity_id, str(pid), rt, rv, rv)
                        self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)

        if is_so:
            rv = '%s-%s(SO)' % (scores[1], scores[0])
            values = (self.entity_id, '', 'score', rv, rv)
            self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)
        else:
            if is_ot:
                rv = '%s-%s(OT)' % (scores[1], scores[0])
                values = (self.entity_id, '', 'score', rv, rv)
                self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)
            else:
                rv = '%s-%s' % (scores[1], scores[0])
                values = (self.entity_id, '', 'score', rv, rv)
                self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)

        return

    def check_participants(self, callsign):
        sk = callsign.strip().lower()
        srcs = eval(self.hash_conf['SOURCE_MAPPING']).get(self.affiliation, [])
        if srcs:
            srcs = srcs.split('<>')
        else:
            values = ('participant', self.source, callsign)
            res = [self.get_redis_entity(source_key=callsign, entity_type='participant', source=self.source)]
            if None in res:
                res = ''
            if not res:
                self.cursor.execute(self.hash_conf['PID_CHECK_QUERY'], values)
                res = [str(i[0]) for i in self.cursor.fetchall()]
            if len(res) > 1:
                for res_ in res:
                    values = (res_, self.participant_type)
                    self.cursor.execute(self.hash_conf['PID_CHECK_TYPE_QUERY'], values)
                    result = [str(i[0]) for i in self.cursor.fetchall()]
                    if result:
                        return result
            else:
                if res:
                    return res

        for src in srcs:
            values = ('participant', src, sk)
            res = [self.get_redis_entity(source_key=sk, entity_type='participant', source=src)]
            if None in res:
                res = ''
            if not res or None in res:
                self.cursor.execute(self.hash_conf['PID_CHECK_QUERY'], values)
                res = [str(i[0]) for i in self.cursor.fetchall()]
            if res:
                return res

        if not res:
            if self.reference_url is None:
                record = '<>'.join([self.spider_class, self.source, callsign])
            else:
                record = '<>'.join([self.game, self.source, callsign, self.reference_url])
            self.out_file.write('%s\n' % record)

        return res

    def handle_richdata_videos(self):
        videos = self.rich_data.get('video_links', [])
        for video in videos:
            sk = video.get('sk', '')
            title = video.get('title', '')
            desc = video.get('desc', '')
            img = video.get('img_link', '')
            ref = video.get('reference', '')
            if 'high' in video.keys() and 'low' in video.keys():
                if video.get('high', ''):
                    high_link = video.get('high', '').get('url', '')
                    high_mime = video.get('high', '').get('mimetype', '')
                if video.get('low', ''):
                    low_link = video.get('low', '').get('url', '')
                    low_mime = video.get('low', '').get('mimetype', '')
            else:
                high_link = high_mime = low_link = low_mime = ''

            other_links = ''
            if not high_link and not low_link:
                other_links = self.get_other_links(video)
            if sk and title:
                game_val = (self.entity_id, sk)
                self.cursor.execute(self.hash_conf['GAME_VIDEOS'], game_val)

                video_val = (sk, ref, high_link, low_link, high_mime, low_mime, clean_data(title), clean_data(desc), img, other_links)
                self.cursor.execute(self.hash_conf['INSERT_VIDEOS'], video_val)

    def handle_richdata(self):
        channels = ''
        game_note = self.rich_data.get('game_note', '')
        _channels = self.rich_data.get('channels', '')
        if isinstance(_channels, str):
            channels = _channels
        elif isinstance(_channels, list):
            channels = '<>'.join(_channels)
        radio = self.rich_data.get('Radio', '')
        online = self.rich_data.get('Online', '')
        location = self.rich_data.get('location', '')
        stadium = self.rich_data.get('Stadium', '').strip()
        if not stadium:
            stadium = self.rich_data.get('stadium', '').strip()
        game_type = self.rich_data.get('game_type', '')
        return (game_note, channels, radio, online, location, stadium, game_type)

    def get_game_participants(self):
        self.cursor.execute(self.hash_conf['PARTICIPANT_IDS'], (self.entity_id,))
        res = list(set([i for i in self.cursor.fetchall()]))
        return res


    def update_unknown_source(self):
        query = "select source from sports_source_keys where entity_id=%s and entity_type='game'"
        values = (self.entity_id)

        for k, v in eval(self.hash_conf['AFF_SRC']).iteritems():
            if k == self.affiliation:
                src = v
                break
        self.cursor.execute(query, values)
        source = self.cursor.fetchone()
        if source:
            source = source[0]
            if 'unknown' in source:
                query = "UPDATE sports_source_keys SET source=%s WHERE entity_id=%s and entity_type='game' and source_key=%s"
                values = (src, self.entity_id, self.source_key)

    def populate_series_name(self):
        loc_ids = []
        series = self.series_name.get('series_name', '')
        series_season_start = self.series_name.get('season_start', '')
        series_season_end   = self.series_name.get('season_end', '')
        countries  = self.series_name.get('country', '')

        if countries:
            query = 'select id from sports_locations where country=%s and city="" and state= ""'
            for country in countries.split('<>'):
                self.cursor.execute(query , country)
                loc_id = self.cursor.fetchone()
                if loc_id:
                    loc_ids.append(str(loc_id[0]))

        location_id = '<>'.join(loc_ids)

        self.cursor.execute(self.hash_conf['SERIES_EXISTENCE'] , (series))
        series_id = self.cursor.fetchone()
        if not series_id:
            values = (series, location_id, series_season_start, series_season_end)
            self.cursor.execute(self.hash_conf['INSERT_SERIES_NAME'] , values)
            series_id = self.cursor.rowcount

        if series_id and self.entity_id:
            if isinstance(series_id, tuple):
                series_id = series_id[0]
            values = (series_id, self.entity_id)
            self.cursor.execute(self.hash_conf['INSERT_SERIES_GAMES'] , values)

    def populate_tou_participants(self, pt_ids, tid):
        for value, key in pt_ids:
            if int(key) != 0:
                query = 'select title from sports_participants where id=%s'
                self.cursor.execute(query , value)
                pt_name = self.cursor.fetchone()[0]
                if not 'tbd' in pt_name.lower():
                    self.cursor.execute(self.hash_conf['INSERT_TOU_PARTICIPANTS'], (value, tid))

    def create_new_record(self):
        pts = self.update_participants()
        pt_ids = []
        rd = self.handle_richdata()

        tid = self.tournament

        event_id = self.event

        loc_id, std_id = self.update_game_locations(rd[4], rd[5])
        std_id = rd[4].get('stadium', '')
        if isinstance(pts, set):
            for pt in pts:
                pt_ids.append((pt[1], pt[0]))

        else:
            for k, v in pts.iteritems():
                if v: pt_ids.append((v, k))

        if tid and (len(pt_ids) == 2 or self.participant_type == 'player') or \
            (len(pt_ids) == 4 and self.participant_type == 'player' and self.game == 'tennis') or \
                   self.game in eval(self.hash_conf['NON_PARTICIPANT_GAMES']):
            ref_url = ''
            if self.reference_url:
                ref_url = self.reference_url

            if loc_id == []:
                loc_id = ''
            game_values = (self.source_key, self.game_datetime, self.sport_id, rd[0], tid, self.game_status, rd[1], rd[2], rd[3], ref_url, event_id, loc_id, std_id, self.time_unknown, self.tz_info, self.season_type, self.week_id, self.week_number)

            self.cursor.execute(self.hash_conf['GAME_INSERTION'], game_values)

            en_values = (self.source_key, 'game', 'reference_id', self.reference_id)
            self.cursor.execute(self.hash_conf['INT_ENT'], en_values)
 
            for v, k in pt_ids:
                if self.game in eval(self.hash_conf['UPDATE_GROUP_PARTICIPANTS_GAMES_LIST']):
                    self.cursor.execute(self.hash_conf['GAME_GROUP_PTS'], (self.source_key, v, k))
                else:
                    self.cursor.execute(self.hash_conf['GAME_PTS'], (self.source_key, v, k))



    def update_record(self):

        if self.source in eval(self.hash_conf['GRAND_SLAM']) and 'constructed' in self.game_status:
            return

        if self.event:
            event_id = self.event
        else:
            tid = self.get_tournament_id()
            event_id = tid
        pt_ids = self.get_game_participants()
        if self.game in eval(self.hash_conf['UPDATE_PARTICIPANTS_GAMES_LIST']):
            db_ids = set([tuple(sorted((str(i[0]), str(i[1])))) for i in pt_ids])
            pts = self.update_participants()
            id_diff = pts - db_ids
            if id_diff:
                remove_pids = tuple(pts)
                for remove_pid in remove_pids:
                    #self.cursor.execute(self.hash_conf['REMOVE_GAME_RES_PAT'], (remove_pid[1], self.entity_id))
                    self.cursor.execute(self.hash_conf['REMOVE_GAME_PAT'], (remove_pid[1], self.entity_id))
                pids = tuple(id_diff)
                for pid in pids:
                    if self.game in eval(self.hash_conf['UPDATE_GROUP_PARTICIPANTS_GAMES_LIST']):
                        self.cursor.execute(self.hash_conf['GAME_GROUP_PTS'], (self.entity_id, pid[1], pid[0]))
                    else:
                        self.cursor.execute(self.hash_conf['GAME_PTS'], (self.entity_id, pid[1], pid[0])) 

        if len(pt_ids) <= 1:
            pts = self.update_participants()

            if isinstance(pts, set):
                for pt in pts:
                    self.cursor.execute(self.hash_conf['GAME_PTS'], (self.entity_id, pt[1], pt[0]))
                    pt_ids.append((pt[1], pt[0]))
            else:
                for k, v in pts.iteritems():
                    self.cursor.execute(self.hash_conf['GAME_PTS'], (self.entity_id, v, k))
                    if v: pt_ids.append((v, k))

        pt_ids = self.get_game_participants()
        tid = self.get_tournament_id()

        '''if tid:
            self.populate_tou_participants(pt_ids, tid)'''

        if self.game_status in ['completed', 'ongoing'] \
                         or self.game in eval(self.hash_conf['PUSH_SCORES']):
            self.update_game_scores(pt_ids)

        if self.affiliation.lower() in eval(self.hash_conf['ALLOWED_AFFILIATION']):
            if 'tbd_title' in self.rich_data and self.rich_data.get('tbd_title', ''):
                source_key, value = self.rich_data.get('tbd_title', '')
                pt_id = self.check_participants(source_key)
                if pt_id:
                    values = (self.entity_id, str(pt_id[0]), 'tbd_title', value, value)
                    self.cursor.execute(self.hash_conf['INSERT_SCORES'], values)

        channel_info = ''
        loc_id = std_id = 0
        game_note = ''
        radio_info = ''
        if self.rich_data:
            self.handle_richdata_videos() #UPDATE GAME VIDEOS DATA
            rd = self.handle_richdata()
            loc_id, std_id = self.update_game_locations(rd[4], rd[5])
            channel_info = rd[1]
            game_note = rd[0]
            radio_info = rd[2]

        if self.series_name and isinstance(self.series_name, dict):
            self.populate_series_name()

        if 'stats' in self.rich_data:
            stats = self.rich_data.get('stats', {})
            self.populate_stats(stats)


        if not event_id or str(event_id) == '197' or str(event_id) == '977':
            values = (self.game_status, self.time_unknown, self.entity_id)
            self.cursor.execute(self.hash_conf['UPDATE_GAME_RECORD'], values)
        else:
            values = (self.game_status, event_id, self.time_unknown, self.entity_id)
            self.cursor.execute(self.hash_conf['UPDATE_RECORD'], values) #UPDATE GAME STATUS

        sport_id = self.sport_id

        if sport_id:
            values = (sport_id, self.entity_id)
            self.cursor.execute(self.hash_conf['UPDATE_SPORTID'], values)

        if loc_id:
            values = (loc_id, self.entity_id)
            self.cursor.execute(self.hash_conf['UPDATE_LOCATION'], values)

        if std_id:
            values = (std_id, self.entity_id)
            self.cursor.execute(self.hash_conf['UPDATE_STADIUM'], values)

        if self.tournament:
            values = (self.tournament, self.entity_id)
            self.cursor.execute(self.hash_conf['UPDATE_TOU'], values)


        if game_note and game_note is not None:
            self.cursor.execute(self.hash_conf['UPDATE_GAME_NOTE'], (game_note, self.entity_id))
        #if self.game != "cricket" and  " 05:00" not in self.game_datetime \
                      #and " 04:00" not in self.game_datetime and " 00:00" not in self.game_datetime:
        if radio_info is not None:
            self.cursor.execute(self.hash_conf['UPDATE_RADIO'], (radio_info, self.entity_id))
        if channel_info is not None and channel_info:
            self.cursor.execute(self.hash_conf['UPDATE_CHANNEL'], (channel_info, self.entity_id))

        if self.game_status == "scheduled" or self.game_status == "closed" or self.game_status == "not_started":
            if self.game_datetime:
                values = (self.game_datetime, self.time_unknown, self.entity_id)
                self.cursor.execute(self.hash_conf['UPDATE_GAMETIME'], values)

        if self.game_status == 'completed' and self.affiliation == 'iaaf':
             if self.game_datetime:
                 values = (self.game_datetime, self.time_unknown, self.entity_id)
                 self.cursor.execute(self.hash_conf['UPDATE_GAMETIME'], values)

        '''if self.game_status == 'completed' and self.affiliation == 'club-football':
             if self.game_datetime:
                 values = (self.game_datetime, self.time_unknown, self.entity_id)
                 self.cursor.execute(self.hash_conf['UPDATE_GAMETIME'], values)'''

        if self.game_status == 'completed' and self.affiliation == 'ufc':
             if self.game_datetime:
                 values = (self.game_datetime, self.time_unknown, self.entity_id)
                 self.cursor.execute(self.hash_conf['UPDATE_GAMETIME'], values)

        if self.game_status == 'scheduled' or self.game_status == 'completed':
            if self.reference_url:
                values = (self.reference_url, self.entity_id)
                self.cursor.execute(self.hash_conf['UPDATE_REF_URL'], values)

        if self.tz_info:
            values = (self.tz_info, self.entity_id)
            self.cursor.execute(self.hash_conf['UPDATE_TZINFO'], values)
        if self.season_type:
            values = (self.season_type, self.entity_id)
            self.cursor.execute(self.hash_conf['UPDATE_SEASON_TYPE'], values)

        if self.week_id:
            values = (self.week_id, self.entity_id)
            self.cursor.execute(self.hash_conf['UPDATE_WEEK_ID'], values)
    
        if self.week_number:
            values = (self.week_number, self.entity_id)
            self.cursor.execute(self.hash_conf['UPDATE_WEEK_NO'], values)


    def populate_tournament_standings(self):
        if not self.tournament:
            return
        tournament_id = self.get_tournament_id()
        if not tournament_id:
            return
        for team_sk, values in self.result.iteritems():
            team_id = self.get_participant_id(team_sk, 'participant')
            if not team_id:
                team_id = self.get_group_id(team_sk)
                if not team_id:
                    continue
            for key, value in values.iteritems():
                values = (tournament_id, team_id, self.season, self.id_type, 'standings', key, value, value)
                self.cursor.execute(self.hash_conf['TOU_STANDINGS_RESULTS'], values)

    def populate_group_standings(self):
        if not self.tournament:
            return
        group_id = self.get_group_id(self.tournament)
        if not group_id:
            return
        for team_sk, values in self.result.iteritems():
            team_id = self.get_participant_id(team_sk, 'participant')
            if not team_id:
                return
            for key, value in values.iteritems():
                values = (group_id, team_id, key, value, self.season, value)
                self.cursor.execute(self.hash_conf['GROUP_RESULTS'], values)

                values = (group_id, team_id, self.season)
                self.cursor.execute(self.hash_conf['INSERT_GRP_PARTICIPANTS'], values)

    def update_rosters(self):
        for team_sk, participants_data in self.participants.iteritems():
            team_id = self.check_participants(str(team_sk))
            if not team_id or len(team_id) > 1:
                continue
            self.cursor.execute(self.hash_conf['INACTIVE_ROSTER'], str(team_id[0]))
            for player_sk, player_data in participants_data.iteritems():
                player_id = self.check_participants(str(player_sk))
                if not player_id:
                    print player_sk
                    continue
                player_role = player_data.get('player_role', '')
                player_number = player_data.get('player_number', '')
                status = player_data.get('status', '')
                field_type = player_data.get('field_type', '')
                language = player_data.get('language', '')

                source_template = eval(self.hash_conf['SOURCE_TEMPLATE'])
                if status == 'active' and self.source.lower() in source_template:
                    default_template = self.hash_conf[source_template[self.source.lower()][1]]
                    self.cursor.execute(self.hash_conf['PARTICIPANT_TITLE'] % player_id[0])
                    title, sport_id, reference_url = self.cursor.fetchone()
                    game = self.get_pl_game(sport_id)
                    self.cursor.execute(self.hash_conf['PARTICIPANT_TITLE'] % team_id[0])
                    team_details = self.cursor.fetchone()
                    field_text = default_template % (title, game, player_role, source_template[self.source.lower()][0], team_details[0], reference_url)
                    values = (language, player_id[0], 'participant', field_type, field_text, field_text.strip())
                    self.cursor.execute(self.hash_conf['INSERT_DESCRIPTION'], values)

                if not player_role:
                    values = (team_id[0], player_id[0], player_role, player_number, status, '',
                              self.season, player_number, status, self.season)
                    self.cursor.execute(self.hash_conf['INSERT_ROSTER_WITHOUT_ROLE'], values)
                else:
                    values = (team_id[0], player_id[0], player_role, player_number, status, '', self.season,
                             player_role, player_number, status, self.season)
                    self.cursor.execute(self.hash_conf['INSERT_ROSTER'], values)


    def process_record(self):

        self.source        = self.item.get('source', '')
        self.source_key    = self.item.get('source_key', '')
        self.participants  = self.item.get('participants', '')
        self.game          = self.item.get('game', '')
        self.game_datetime = self.item.get('game_datetime', '')
        self.result        = self.item.get('result', '')
        self.affiliation   = self.item.get('affiliation', '')
        self.rich_data     = self.item.get('rich_data', {})
        self.game_status   = self.item.get('game_status', '')
        self.tournament    = self.item.get('tournament', '')
        self.event         = self.item.get('event', 0)
        self.time_unknown  = self.item.get('time_unknown', 0)
        self.tz_info       = self.item.get('tz_info', '')
        self.reference_url = self.item.get('reference_url', '')
        self.participant_type = self.item.get('participant_type', '')
        self.date_str      = self.date.strftime('%Y-%m-%d')
        self.season        = self.item.get('season', '')
        self.result_sub_type = self.item.get('result_sub_type', '')
        self.series_name   = self.item.get('series_name', '')
        self.sport_name    = self.item.get('sport_name', '')
        self.sport_id      = self.item.get('sport_id', '')
        self.reference_id  = self.item.get('reference_id', '')
        self.week_id       = self.item.get('week_id', '')
        self.week_number   = self.item.get('week_number', '')
        self.season_type   = self.item.get('season_type', '')
        
        if self.spider_class:
            self.stats_file    = os.path.join(STATS_DIR, self.spider_class + '.pickle')
        else:
            self.stats_file    = ''

        #check_file_access(self.stats_file)
        #check_file_access(self.gids_file)
    
        source_key = self.item.get('source_key', '')
        source = self.item.get('source', '')
        entity_id = 0
        if source_key:
            entity_id = self.get_redis_entity(source_key=source_key, source=source, entity_type='game')
            if not entity_id:
                query = self.hash_conf['GAME_EXISTANCE_CHECK'] % (source_key)
                self.cursor.execute(query)
                entity_id = self.cursor.fetchone()
                if entity_id:
                    entity_id = entity_id[0]
                    en_values = (entity_id, 'game', 'reference_id', self.reference_id)
                    self.cursor.execute(self.hash_conf['INT_ENT'], en_values) 

        if source_key and not entity_id:
            log.info('Started creating new record: %s' %source_key)
            self.create_new_record()
            if self.entity_id == None and self.stats_file:
                self.store_pickle_data({'skipped_%s' %self.date_str: source_key}, self.stats_file)
            elif self.stats_file:
                self.store_pickle_data({'inserted_%s' %self.date_str: self.entity_id}, self.stats_file)
                self.store_pickle_data({'gids': self.entity_id}, self.gids_file)
            log.info('Completed creating new record: %s' %source_key)

        elif source_key and entity_id:
            self.entity_id = entity_id
            log.info('Started updating record: %s' %self.entity_id)
            self.update_record()
            if self.stats_file:
                self.store_pickle_data({'updated_%s' %self.date_str: self.entity_id}, self.stats_file)
                self.store_pickle_data({'gids': self.entity_id}, self.gids_file)
            log.info('Completed updating record: %s' %self.entity_id)

    def populate_standings(self):
        self.season        = self.item.get('season', '')
        self.tournament    = self.item.get('tournament', '')
        self.source        = self.item.get('source', '')
        self.result        = self.item.get('result', '')
        self.result_type   = self.item.get('result_type', '')
        self.id_type       = self.item.get('id_type', '')
        self.affiliation   = self.item.get('affiliation', '')
        self.participant_type = self.item.get('participant_type', '')

        if self.result_type == 'tournament_standings':
            self.populate_tournament_standings()
        elif self.result_type == 'group_standings':
            self.populate_group_standings()

    def populate_rosters(self):
        self.source       = self.item.get('source', '')
        self.season       = self.item.get('season', '')
        self.participants = self.item.get('participants', '')
        self.affiliation  = self.item.get('affiliation', '')

        self.update_rosters()
