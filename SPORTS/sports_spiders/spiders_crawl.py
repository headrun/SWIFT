import os, sys
import time
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy.utils.project import get_project_settings
from scrapy.xlib.pydispatch import dispatcher
from scrapy import log, signals
from vtv_task import VtvTask, vtv_task_main
from datetime import datetime
from vtv_utils import vtv_pickle, vtv_unpickle, get_compact_traceback, \
                    execute_shell_cmd, VTV_REPORTS_DIR


DIR = os.path.dirname(os.path.realpath(__file__))
PATH, CUR_DIR = os.path.split(DIR)
DOT = '.'
PICKLE_FILE_NAME = 'runs_count.pickle'
HTML_FILE        = '%s_%s.html'
MODULE_DOMAIN = DOT.join([CUR_DIR, 'spiders'])
PATH = os.path.join(DIR, CUR_DIR, 'spiders')
SPIDER_MODULES = [f_name[:-3] for f_name in os.listdir(PATH) \
                  if f_name.endswith('.py') and f_name != '__init__.py']
NOW = time.time()

def get_module(f_name):

    """
    Returns the object of the required spider
    """
    for module in SPIDER_MODULES:
        module_path = MODULE_DOMAIN + DOT + module
        __import__(module_path)
        req_module = sys.modules[module_path]
        if f_name in dir(req_module):
            return req_module

def check_dir_access(dir_name):

    """
    Checks the existance of directories
    """
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


class SpidersCrawl(VtvTask):

    """
    Crawls the spider by using the reactor
    """
    def __init__(self):
        VtvTask.__init__(self)
        self.cwd         = os.getcwd()
        self.scrapy_dir  = os.path.join(self.cwd, self.cwd.rsplit('/', 1)[-1])
        self.spiders_dir = os.path.join(self.scrapy_dir, 'spiders')
        self.pickle_file_name = PICKLE_FILE_NAME
        self.date_now    = datetime.now()
        self.today       = self.date_now.strftime("%Y-%m-%d")
        self.stats_dir   = 'SPORTS_STATS_DIR'
        self.check_file_access(self.pickle_file_name)


    def check_file_access(self, file_name):

        """
        Checks the existance of files
        """
        if not os.access(file_name, os.F_OK|os.R_OK):
            vtv_pickle(file_name, {}, self.logger)


    def load_pickle_data(self, data):

        """
        Loads the runs count to pickle file
        """
        pickle_table = vtv_unpickle(self.pickle_file_name, self.logger)
        spider_stats = data.get_stats()
        spider_name  = spider_stats['spider_name']
        if not pickle_table:
            pickle_table = {}
        pickle_count = pickle_table.get(spider_name, '')
        if not pickle_count:
            pickle_table[spider_name] = 1
        else:
            pickle_table[spider_name] += 1
        vtv_pickle(self.pickle_file_name, pickle_table, self.logger)
        return pickle_table[spider_name]

    def load_stats(self):

        """
        Collects the pickle file data
        """
        file_name = os.path.join(self.stats_dir, '%s.pickle' %self.options.spider_name)
        if not os.access(file_name, os.F_OK|os.R_OK):
            return set(), set(), set()

        pickle_table = vtv_unpickle(file_name, self.logger)
        updated  = pickle_table.get('updated_%s' %self.today, set())
        inserted = pickle_table.get('inserted_%s' %self.today, set())
        skipped  = pickle_table.get('skipped_%s' %self.today, set())
        return inserted, updated, skipped

    def execute_command(self, cmd):

        """
        Executes the given command
        """
        cmd_status, output = execute_shell_cmd(cmd, self.logger)
        self.logger.info('CMD: %s Status: %s' % (cmd, cmd_status))

    def process(self):

        """
        Runs the spider and collects the stats
        """
        self.logger.info('Started processing spider')
        os.chdir(self.spiders_dir)
        dispatcher.connect(self.stop_reactor, signal=signals.spider_closed) # Signal to stop the reactor after completing the spider
        module = get_module(self.options.spider_name)
        spider  = eval("module.%s(spider_type='%s')" % (self.options.spider_name, self.options.spider_type))
        settings = get_project_settings()
        if self.options.skip_robots:
            settings.overrides['ROBOTSTXT_OBEY'] = False
        crawler = Crawler(settings)
        crawler.configure()
        crawler.crawl(spider)
        crawler.start()
        log.start()
        reactor.run() # the script will block here if no signal

        os.chdir(self.cwd)
        crawler.stats.set_value('spider_name', self.script_name)
        downloader_count = crawler.stats.get_stats()['downloader/response_count']
        runs_count = self.load_pickle_data(crawler.stats)
        inserted, updated, skipped = self.load_stats()

        temp_dict = {}
        temp_dict['responses'] = downloader_count
        temp_dict['runs'] = runs_count
        temp_dict['inserted'] = len(inserted)
        temp_dict['updated'] = len(updated)
        temp_dict['skipped'] = len(skipped)
        self.add_stats(self.script_name, temp_dict)

        cmd = "python sports_validations.py %s" % self.options.spider_name
        execute_shell_cmd(cmd, self.logger)
        self.logger.info(crawler.stats.get_stats())
        self.logger.info('Completed processing spider')

    def stop_reactor(self):

        """
        Signal to stop the reactor
        """
        reactor.stop()

    def run_main(self):

        """
        Common function for vtv_task to run the code
        """
        self.script_name = self.options.spider_name
        if self.options.spider_type:
            self.script_name += '%s' % self.options.spider_type.capitalize()
        spiders_report = os.path.join(VTV_REPORTS_DIR, self.script_name)
        check_dir_access(spiders_report)
        self.options.report_file_name = os.path.join(spiders_report, HTML_FILE % (self.script_name, self.today_str))
        try:
            self.process()
        except (KeyboardInterrupt,  SystemExit):
            raise
        except:
            e = sys.exc_info()[2]
            self.logger.error('spider: %s: exception: %s' % (self.script_name, get_compact_traceback(e)))

    '''def cleanup(self):

        """
        Removes the old logs
        """
        for files in os.listdir(self.stats_dir):
            if 'pickle' not in files and os.stat(os.path.join(self.stats_dir, files)).st_mtime < NOW - 5 * 86400:
                os.remove(os.path.join(self.stats_dir, files))'''

    def set_options(self):

        """
        Crawl options
        """
        self.parser.add_option("",   "--spider-name",  default='', help="Specify name of the Spider")
        self.parser.add_option("",   "--spider-type",  default='', help="Specify type of Spider crawl")
        self.parser.add_option("",   "--skip-robots",  default='', help="Skip robots.txt logic")

if __name__ == '__main__':
    vtv_task_main(SpidersCrawl)
