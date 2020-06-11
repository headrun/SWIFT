#!/usr/bin/env python
import datetime
import os
import datetime
from collections import Counter, OrderedDict
from vtv_task import VtvTask, vtv_task_main
from data_report import VtvHtml, HtmlObj
from vtv_utils import copy_file
from vtv_db import get_mysql_connection
from data_report import VtvHtml, HtmlObj


PAR_COUNT = "select count(*) from sports_participants where created_at BETWEEN NOW() - INTERVAL 30 DAY AND NOW() and participant_type='player'"
TEAM_COUNT = "select count(*) from sports_participants where created_at BETWEEN NOW() - INTERVAL 30 DAY AND NOW() and participant_type='team'"
TOU_COUNT = "select count(*) from sports_tournaments where created_at BETWEEN NOW() - INTERVAL 30 DAY AND NOW() and type='tournament'"
EVENT_COUNT = "select count(*) from sports_tournaments where created_at BETWEEN NOW() - INTERVAL 30 DAY AND NOW() and type='event'"
CON_COUNT = "select count(*) from sports_tournaments where created_at BETWEEN NOW() - INTERVAL 30 DAY AND NOW() and type='sportsconcept'"
GROUP_COUNT = "select count(*) from sports_tournaments_groups where group_type !='Rivalry' and created_at BETWEEN NOW() - INTERVAL 30 DAY AND NOW()"
sport_count = 'select distinct(sport_id) from sports_participants where created_at like %s'
sport_to_count = 'select distinct(sport_id) from sports_tournaments where created_at like %s'
sport_gp_count = 'select distinct(sport_id) from sports_tournaments_groups where created_at like %s'

PAR_DB_COUNT = "select count(*) from sports_participants where participant_type='player'"
TEAM_DB_COUNT = "select count(*) from sports_participants where participant_type='team'"
TOU_DB_COUNT = "select count(*) from sports_tournaments where type='tournament'"
EVENT_DB_COUNT = "select count(*) from sports_tournaments where type='event'"
CON_DB_COUNT = "select count(*) from sports_tournaments where type='sportsconcept'"
GROUP_DB_COUNT = "select count(*) from sports_tournaments_groups where group_type !='Rivalry'"


DB_COUNT_DICT = [('Sportsconcept', CON_DB_COUNT), ('Tournament', TOU_DB_COUNT), \
                ('Event', EVENT_DB_COUNT), ('Group', GROUP_DB_COUNT), \
                ('Team', TEAM_DB_COUNT), ('Player', PAR_DB_COUNT)]

QUERY_DICT = [('Sportsconcept', CON_COUNT), ('Tournament', TOU_COUNT), \
            ('Event', EVENT_COUNT), ('Group', GROUP_COUNT), \
            ('Team', TEAM_COUNT), ('Player', PAR_COUNT)]

SPORTS_GROUP_QRY = "select DATE(created_at), count(*) from sports_tournaments_groups where created_at BETWEEN NOW() - INTERVAL 30 DAY AND NOW() and group_type!='Rivalry' group by DATE(created_at) order by YEAR(created_at) desc, MONTH(created_at) desc"
SPORTS_GROUP_PAR = "select SP.sport_id, T.title, count(*) from sports_tournaments_groups SP, sports_types T where SP.created_at like %s and T.id=SP.sport_id and group_type!='Rivalry' group by SP.sport_id order by T.title"

SPORTS_TOU_QRY = "select DATE(created_at), count(*) from sports_tournaments where created_at BETWEEN NOW() - INTERVAL 30 DAY AND NOW() and type= %s group by DATE(created_at) order by YEAR(created_at) desc, MONTH(created_at) desc"

SPORTS_TOU_PAR = 'select SP.sport_id, T.title, count(*) from sports_tournaments SP, sports_types T where SP.created_at like %s and T.id=SP.sport_id and SP.type = %s group by SP.sport_id order by T.title'

PAR_QRY = 'select SP.sport_id, T.title, count(*) from sports_participants SP, sports_types T where SP.created_at like %s and T.id=SP.sport_id and SP.participant_type = %s group by SP.sport_id order by T.title'

SEL_QRY = "select DATE(created_at), count(*) from sports_participants where created_at BETWEEN NOW() - INTERVAL 30 DAY AND NOW() and participant_type=%s group by DATE(created_at) order by YEAR(created_at) desc, MONTH(created_at) desc"

TYPE_SEL_DICT = { 'Group': SPORTS_GROUP_QRY, 'Tournament': SPORTS_TOU_QRY, \
                'Event': SPORTS_TOU_QRY, 'Sportsconcept': SPORTS_TOU_QRY, \
                'Player': SEL_QRY, 'Team': SEL_QRY}
TYPE_PAR_DICT = {'Group': SPORTS_GROUP_PAR, 'Tournament': SPORTS_TOU_PAR, \
                'Event': SPORTS_TOU_PAR, 'Sportsconcept': SPORTS_TOU_PAR, \
                'Player': PAR_QRY, 'Team': PAR_QRY}

DB_SEL_QRY = 'SELECT count(distinct(sport_id)) as sport, YEAR(created_at) as year_val, MONTH(created_at) as month_val ,COUNT(*) as total FROM sports_participants where participant_type=%s GROUP BY YEAR(created_at), MONTH(created_at)'

DB_TOU_QRY = 'SELECT count(distinct(sport_id)) as sport, YEAR(created_at) as year_val, MONTH(created_at) as month_val ,COUNT(*) as total FROM sports_tournaments where type=%s GROUP BY YEAR(created_at), MONTH(created_at)'

DB_GROUP_QRY = 'SELECT count(distinct(sport_id)) as sport, YEAR(created_at) as year_val, MONTH(created_at) as month_val ,COUNT(*) as total FROM sports_tournaments_groups where group_type!="Rivalry" GROUP BY YEAR(created_at), MONTH(created_at)'

DB_SEL_DICT = {'Group': DB_GROUP_QRY, 'Tournament': DB_TOU_QRY, \
                'Event': DB_TOU_QRY, 'Sportsconcept': DB_TOU_QRY, \
                'Player': DB_SEL_QRY, 'Team': DB_SEL_QRY}

DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'
class NewEntriesReport(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name = "SPORTSDB"
        self.db_ip   = "10.28.218.81"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                self.db_name, cursorclass ='',
                                user = 'veveo', passwd ='veveo123')
        self.today         = str(datetime.datetime.now().date())
        self.text = ''
        self.tou_dict = {}
        self.tou_text = ''
        self.teams_text = ''
        self.teams_dict = {}
        self.all_teams = []
        self.obj_dict = OrderedDict()
        self.obj_map_dict = OrderedDict()
        self.tour_details = {}
        self.teams_dict = {}
        self.type_list = ['Sportsconcept', 'Tournament', 'Group', 'Event', 'Team', 'Player']
        self.teams_dict = OrderedDict()
        self.teams_sport_dict = OrderedDict()
        self.db_sport_dict = OrderedDict()
        self.db_teams_dict = {}
        self.db_data_dict = OrderedDict()
        self.teams_list = []
        self.dates_list = []
        self.REPORTS_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR, 'SPORTS')
        #self.REPORTS_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR, 'SPORTS')

    def get_month(self):
        for type_ in self.type_list:
            sel_qry = DB_SEL_DICT[type_]
            if type_ != "Group":
                values = (type_)
                self.cursor.execute(sel_qry, values)
            else:
                self.cursor.execute(sel_qry)
            data = self.cursor.fetchall()
            for data_ in data:
                sp_count = data_[0]
                year = data_[1]
                month = data_[2]
                month = '0'+str(month)
                month = month.replace('011', '11').replace('012', '12').replace('010', '10')
                count = str(data_[3])
                date_ = str(year)+"-"+str(month)
                PAR_QRY = TYPE_PAR_DICT[type_]
                dat_ = '%' + date_ + '%'
                if type_ != "Group":
                    values = (dat_, type_)
                else:
                    values = (dat_)
                s_count = t_count = g_count = 0
                sport_count_list = []
                sp_values = (dat_)
                self.cursor.execute(sport_count, sp_values)
                sp_data = self.cursor.fetchall()
                for s_data in sp_data:
                    s_count = s_data[0]
                    sport_count_list.append(s_count)

                self.cursor.execute(sport_to_count, sp_values)
                to_data = self.cursor.fetchall()
                for t_data in to_data:
                    t_count = t_data[0]
                    sport_count_list.append(t_count)
                self.cursor.execute(sport_gp_count, sp_values)
                gp_data = self.cursor.fetchall()
                for g_data in gp_data:
                    g_count = g_data[0]
                    sport_count_list.append(g_count)

                self.cursor.execute(PAR_QRY, values)
                p_data = self.cursor.fetchall()
                total_count = 0
                sp_tl_title = []
                for p_dat in p_data:
                    sp_id = p_dat[0]
                    sp_title = p_dat[1]
                    p_count = p_dat[2]
                    p_tl_count  = total_count + int(p_count)
                    self.teams_list.append(sp_title)
                    self.dates_list.append(date_)
                    self.db_teams_dict.setdefault((date_, sp_title), []).append((type_, p_count))
                self.db_sport_dict.setdefault((date_), []).append((len(list(set(sport_count_list))), type_, count))

        self.dates_list = list(set(self.dates_list))
        self.teams_list.sort()
        self.dates_list.sort(reverse=True)

        for date in self.dates_list:
            for sport in self.teams_list:
                date = date
                sport = sport
                data = (date, sport)
                tou_data = self.db_teams_dict.get(data, '')
                if tou_data:
                    self.db_data_dict.setdefault((date, sport), []).append(tou_data)
                 
        headers = ['Date', 'Sport', 'Sportsconcept', 'Tournament', 'Event', 'Group', 'Team', 'Player']
        final_sp_list = []
        db_sport_dict = sorted(self.db_sport_dict.items(), key=lambda kv: kv[0], reverse=True)

        for key, values in db_sport_dict:
            date = key
            row = [''] * 8
            row[0] = date
            for value in values:
                sport, type, count = value
                row[1] = sport
                row[headers.index(type)] = str(count)
            final_sp_list.append(row)

        final_team_list = []
        for key, values in self.db_data_dict.iteritems():
            date, sport = key
            row = [''] * 8
            row[0], row[1] = date, sport
            for value in values[0]:
                type, count = value
                row[headers.index(type)] = str(count)
            final_team_list.append(row)

        return final_sp_list, final_team_list


    def get_details(self):

        for type_ in self.type_list:
            sel_qry = TYPE_SEL_DICT[type_]
            if type_ != "Group":
                values = (type_)
                self.cursor.execute(sel_qry, values)
            else:
                self.cursor.execute(sel_qry)
            data = self.cursor.fetchall()
            for data_ in data:
                date_ = str(data_[0])
                count = data_[1]
                PAR_QRY = TYPE_PAR_DICT[type_]
                dat_ = '%' + date_ + '%'
                if type_ != "Group":
                    values = (dat_, type_)
                else:
                    values = (dat_)
                s_count = t_count = g_count = 0
                sport_count_list = []
                sp_values = (dat_)
                self.cursor.execute(sport_count, sp_values)
                sp_data = self.cursor.fetchall()
                for s_data in sp_data:
                    s_count = s_data[0]
                    sport_count_list.append(s_count)

                self.cursor.execute(sport_to_count, sp_values)
                to_data = self.cursor.fetchall()
                for t_data in to_data:
                    t_count = t_data[0]
                    sport_count_list.append(t_count)
                self.cursor.execute(sport_gp_count, sp_values)
                gp_data = self.cursor.fetchall()
                for g_data in gp_data:
                    g_count = g_data[0]
                    sport_count_list.append(g_count)

                self.cursor.execute(PAR_QRY, values)
                p_data = self.cursor.fetchall()
                total_count = 0
                sp_tl_title = []
                for p_dat in p_data:
                    sp_id = p_dat[0]
                    sp_title = p_dat[1]
                    p_count = p_dat[2]
                    p_tl_count  = total_count + int(p_count)
                    sp_tl_title.append(sp_title)
                    self.teams_dict.setdefault((date_, sp_title), []).append((type_, p_count))
                sp_title = ",".join(sp_tl_title)
                self.teams_sport_dict.setdefault((date_), []).append((len(list(set(sport_count_list))), type_, count))

        headers = ['Date', 'Sport', 'Sportsconcept', 'Tournament', 'Event', 'Group', 'Team', 'Player']

        final_team_list = []
        for key, values in self.teams_dict.iteritems():
            date, sport = key
            row = [''] * 8
            row[0], row[1] = date, sport
            for value in values:
                type, count = value
                row[headers.index(type)] = str(count)
            final_team_list.append(row)

        final_sp_list = []
        for key, values in self.teams_sport_dict.iteritems():
            date = key
            row = [''] * 8
            row[0] = date
            for value in values:
                sport, type, count = value
                row[1] = sport
                row[headers.index(type)] = str(count)
            final_sp_list.append(row)


        main_html_obj = VtvHtml()
        main_page = main_html_obj.create_page('Sports New Entries Report: %s' % self.today)
        main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
        main_html_obj.container_open()
        report_file_format = 'sports_new_entries_%s.html'

        summary_header = []
        summary_list, final_list = [], []

        db_summary_header = []
        dbsummary_list, db_final_list = [], []

        main_page.h2('Summary')
        records = ['Month Wise Stats', 'Month Wise Sport Stats', 'Date Wise Stats', 'Date Wise Sport Stats']
        for record in records:
            file_name = '#%s' %(record)
            report_file = "<a href='%s'><b> %s </b></a>" % (file_name, record)
            main_html_obj.page.li(report_file)
            page_str = str(main_page)  

        db_date = '2013-01-04' " : " + self.today
        db_summary_header.append('Date')
        db_summary_header.append('Sport')
        dbsummary_list.append(db_date)
        sport = ''
        dbsummary_list.append(sport)
        main_page.h2('DB Total Count')
        for values in DB_COUNT_DICT:
            key, query = values
            self.cursor.execute(query)
            count = self.cursor.fetchone()
            db_summary_header.append(key)
            dbsummary_list.append(str(count[0]))
        db_final_list.append(tuple(dbsummary_list))

        t_str = main_html_obj.get_html_table_with_rows(db_summary_header, db_final_list)
        main_page.add(t_str)


        date = '2017-03-10'+ " : " + self.today
        summary_header.append('Date')
        summary_header.append('Sport')
        summary_list.append(date)
        sport = ''
        summary_list.append(sport)
        #main_page.h2('Total Count')
        for values in QUERY_DICT:
            key, query = values
            self.cursor.execute(query)
            count = self.cursor.fetchone()
            summary_header.append(key)
            summary_list.append(str(count[0]))
        final_list.append(tuple(summary_list))

        #t_str = main_html_obj.get_html_table_with_rows(summary_header, final_list)
        #main_page.add(t_str)

        db_final_list,db_sport_list = self.get_month()
        report_file = "<a name='%s'>%s</a>" % ('Month Wise Stats', 'Month Wise Stats')
        main_page.h2(report_file)
        db_final_list.sort(key=lambda x: x[0], reverse=True)
        t_str = main_html_obj.get_html_table_with_rows(headers, db_final_list)
        main_page.add(t_str)

        report_file = "<a name='%s'>%s</a>" % ('Month Wise Sport Stats', 'Month Wise Sport Stats')
        main_page.h2(report_file)

        db_sport_list.sort(key=lambda x: x[0], reverse=True)
        t_str = main_html_obj.get_html_table_with_rows(headers, db_sport_list)
        main_page.add(t_str)

        report_file = "<a name='%s'>%s</a>" % ('Date Wise Stats', 'Date Wise Stats')
        main_page.h2(report_file)

        final_sp_list.sort(key=lambda x: x[0], reverse=True)
        t_str = main_html_obj.get_html_table_with_rows(headers, final_sp_list)
        main_page.add(t_str)

        report_file = "<a name='%s'>%s</a>" % ('Date Wise Sport Stats', 'Date Wise Sport Stats')
        main_page.h2(report_file)

        final_team_list.sort(key=lambda x: x[0], reverse=True)
        t_str = main_html_obj.get_html_table_with_rows(headers, final_team_list)
        main_page.add(t_str)


        report_file_name = report_file_format % self.today_str
        report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)
        page_str = str(main_page)
        open(report_file_name,'w').write(page_str)
        copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % 'latest'), self.logger)

    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.', 'newentry_count*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

    def run_main(self):
        self.get_details()


if __name__ == '__main__':
    vtv_task_main(NewEntriesReport)

