import os
import MySQLdb
import pytz
import datetime
from vtv_task import VtvTask, vtv_task_main
from vtv_utils import VTV_REPORTS_DIR

DIR        = os.path.dirname(os.path.realpath(__file__))
HTML_FILE  = '%s_%s.html'
STATS_DIR  = 'SPORTS_STATS_DIR'

class CollectGames(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.source_dict   = {}
        self.stats_dir     = 'SPORTS_STATS_DIR'
        self.today         = datetime.datetime.now().date().strftime("%Y-%m-%d")
        self.conn          = MySQLdb.connect(host="10.4.18.183", db="SPORTSDB", user="root")
        self.cursor        = self.conn.cursor()

    def check_dir_access(self, dir_name):
        if not os.path.exists(dir_name):
            return False
        return True

    def get_query_result(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_et_time(self, date):
        gmt = pytz.timezone('GMT')
        eastern = pytz.timezone('US/Eastern')
        dategmt = gmt.localize(date)
        dateeastern = dategmt.astimezone(eastern)
        return dateeastern.date()

    def collect_sources(self):
        logs = [ os.path.join(DIR, STATS_DIR, f) \
                 for f in os.listdir(STATS_DIR) \
                 if '.pickle' not in f and self.today in f \
                 and 'standing' not in f
               ]
        for log in logs:
            log_obj = open(log).readline()
            if log_obj:
                line = eval(log_obj)
                if line.get('source', ''):
                    self.source_dict[line['source']] = line['spider_class']

    def run_main(self):
        import pdb; pdb.set_trace()
        self.collect_sources()
        for source, source_class in self.source_dict.iteritems():
            self.stats_list = []
            spiders_report  = os.path.join(VTV_REPORTS_DIR, source_class)
            dir_access      = self.check_dir_access(spiders_report)
            if not dir_access:
                source_class   = source_class + 'Scores'
                spiders_report = os.path.join(VTV_REPORTS_DIR, source_class)
            self.options.report_file_name = os.path.join(spiders_report, HTML_FILE % (source_class, self.today_str))
            date_today = {}
            stats_dict = {}
            query      = "select entity_id from sports_source_keys where source = '%s'" % source
            data       = self.get_query_result(query)
            entity_ids = [d[0] for d in data]
            query      = "select id, game_datetime, status from sports_games where id in (%s)"
            game_values = self.get_query_result(query % ','.join(map(str, entity_ids)))
            for game_value in game_values:
                if not game_value:
                    continue
                game_id, game_date, game_status = game_value
                et_date = self.get_et_time(game_date)
                if et_date == datetime.datetime.now().date():
                    date_today[game_id] = game_status

            winners_count = 0
            if date_today:
                winners = 'select count(distinct(game_id)) from sports_games_results where game_id in (%s) and result_type = "winner"'
                winners_count = self.get_query_result(winners % ','.join(map(str, date_today.keys())))
                winners_count = winners_count[0][0]

            stats_dict['games']     = len(date_today)
            stats_dict['completed'] = date_today.values().count('completed')
            stats_dict['winners']   = int(winners_count)
            self.add_stats('stats', stats_dict)
            self.print_stats()
            self.pickle_state()
            self.cleanup()

if __name__ == "__main__":
    vtv_task_main(CollectGames)
