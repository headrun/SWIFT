# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import MySQLdb
import os
import copy
from scrapy import signals
from sports_spiders import configUtils
from sports_spiders import game_utils as gu
from sports_spiders.vtv_utils import VTV_SERVER_DIR
from datetime import datetime

DELEM = '_'
STEPS = '..'
UTILS_CFG = 'game_utils.cfg'
CONFIG = os.path.join(VTV_SERVER_DIR, UTILS_CFG)
STATS_DIR = os.path.join(STEPS, STEPS, 'SPORTS_STATS_DIR')


class SportsGames(object):

    def __init__(self):
        self.gids_file = None
        self.spider_class = None
        self.conn = None
        self.cursor = None
        self.hash_conf = None
        self.spider_name = None
        self.items_log = None

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        today = datetime.now()
        today = today.strftime("%Y-%m-%d")
        if hasattr(spider, 'spider_type'):
            self.spider_name = spider.name + DELEM \
                + spider.spider_type \
                + DELEM + today
        else:
            self.spider_name = spider.name + DELEM + today

        self.hash_conf    = configUtils.readConfFile(CONFIG)
        self.conn = MySQLdb.connect(db=self.hash_conf['DB_NAME'],
                                    host=self.hash_conf['HOST'],
                                    user=self.hash_conf['USER'],
                                    charset="utf8", use_unicode=True,
                                    passwd="root")
        self.cursor = self.conn.cursor()
        self.spider_class = spider.__class__.__name__
        self.gids_file = os.path.join(
            STATS_DIR, self.spider_class + '_gids.pickle')
        log_name = os.path.join(STATS_DIR, self.spider_name)
        self.items_log = open(log_name, 'a+')

    def spider_closed(self, spider):
        self.conn.close()

    def write_log(self, item):
        items_dict = copy.deepcopy(item._values)
        items_dict['spider_class'] = self.spider_class
        self.items_log.write(str(datetime.now()) + ': ' +
                             str(items_dict) + '\n\n')

    def process_item(self, item, spider):
        self.write_log(item)
        sports_item = gu.SportsdbSetup(
            item, self.cursor, self.spider_class, self.gids_file, self.hash_conf)
        if item.get('result_type', '') and \
                'standings' in item.get('result_type', ''):
            sports_item.populate_standings()
            sports_item.clean()
            return

        if item.get('result_type', '') and \
                'roster' in item.get('result_type', ''):
            sports_item.populate_rosters()
            sports_item.clean()
            return
        sports_item.process_record()
        sports_item.clean()
        return item


class CheckDB(object):
    def __init__(self):
        pass

    def process_item(self, item, spider):
        pass
