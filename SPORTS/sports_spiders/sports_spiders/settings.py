# Scrapy settings for sports_spiders project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'sports_spiders'

SPIDER_MODULES = ['sports_spiders.spiders']
NEWSPIDER_MODULE = 'sports_spiders.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'Mozilla/5.0 (Windows NT 6.2; rv:22.0) Gecko/20130405 Firefox/23.0'

# HTTPCACHE_ENABLED   = False
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR       = '/home/veveo/sports_spiders/sports_spiders/cache'
ITEM_PIPELINES      = { 'sports_spiders.pipelines.SportsGames': 100,
                        'sports_spiders.pipelines.CheckDB': 200 }
#ROBOTSTXT_OBEY      = True
ROBOTSTXT_OBEY = False
DOWNLOAD_DELAY      = 1
LOG_LEVEL           = 'INFO'
DOWNLOADER_STATS    = True
STATS_DUMP          = True
CONCURRENT_REQUESTS = 1
CONCURRENT_REQUESTS_PER_DOMAIN = 1

DEPTH_PRIORITY = 1
#SCHEDULER_DISK_QUEUE = 'scrapy.squeue.PickleFifoDiskQueue'
#SCHEDULER_MEMORY_QUEUE = 'scrapy.squeue.FifoMemoryQueue'

#DOWNLOADER_MIDDLEWARES = {
#      'sports_spiders.middlewares.InterfaceRoundRobinMiddleware' : 1
#}
#     'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware': 110,
#     'sports_spiders.middlewares.ProxyMiddleware': 100,
# }
