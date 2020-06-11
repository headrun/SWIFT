# Scrapy settings for sports_spiders project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#
import os
BOT_NAME = 'sports_spiders'

SPIDER_MODULES      = ['sports_spiders.spiders']
NEWSPIDER_MODULE    = 'sports_spiders.spiders'

ITEM_PIPELINES      = { 'sports_spiders.pipelines.SportsGames': 100,
                        'sports_spiders.pipelines.CheckDB': 200 }
#ROBOTSTXT_OBEY      = True
DOWNLOAD_DELAY      = 1
LOG_LEVEL           = 'INFO'
DOWNLOADER_STATS    = True
STATS_DUMP          = True
CONCURRENT_REQUESTS = 1
CONCURRENT_REQUESTS_PER_DOMAIN = 1

PROJECT_DIR = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

#HTTPCACHE_ENABLED = True                                    # Note: Disable Cache Option in Prod setup.
HTTPCACHE_DIR = '%s/cache/' % PROJECT_DIR



DEPTH_PRIORITY = 1
SCHEDULER_DISK_QUEUE = 'scrapy.squeue.PickleFifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeue.FifoMemoryQueue'
