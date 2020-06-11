# -*- coding: utf-8 -*-
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
from scrapy.http import Request
from scrapy.selector import Selector
import unicodedata
from StringUtil import cleanString
from difflib import SequenceMatcher
import MySQLdb
import datetime
import urllib
import time
from vtv_db import get_mysql_connection
from vtv_utils import VTV_CONTENT_VDB_DIR, copy_file, execute_shell_cmd, make_dir_list
from vtv_task import VtvTask, vtv_task_main
from redis_utils import get_redis_data


PLAYERS_PERMUTATIONS = {}
PLAYERS_IN_DB        = {}
SPACE = ' '
THRESHOLD   = 0.70

def permutations(iterable, r=None):
    # permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
    # permutations(range(3)) --> 012 021 102 120 201 210
    pool = tuple(iterable)
    n = len(pool)
    if r is None:
        r = n

    if r > n:
        return
    indices = range(n)
    cycles = range(n, n-r, -1)
    yield tuple(pool[i] for i in indices[:r])
    while n:
        for i in reversed(range(r)):
            cycles[i] -= 1
            if cycles[i] == 0:
                indices[i:] = indices[i+1:] + indices[i:i+1]
                cycles[i] = n - i
            else:
                j = cycles[i]
                indices[i], indices[-j] = indices[-j], indices[i]
                yield tuple(pool[i] for i in indices[:r])
                break
        else:
            return

def get_player_permutations(existing_players):
    for existing_player in existing_players:
        if SPACE in existing_player:
            existing_player = cleanString(existing_player)
            existing_player_list = existing_player.split(SPACE)
            if len(existing_player_list) <= 5:
                existing_player_set = [SPACE.join(i) for i in \
                                      permutations(existing_player_list)]
            else:
                existing_player_set = [existing_player]

        else:
            existing_player_set = [existing_player]

        PLAYERS_PERMUTATIONS[existing_player] = existing_player_set

def find_players_to_add(db_players, tou_players):
    titles_present = {}
    mstly_matched_titles = {}

    for player in tou_players:
        player = player
        if player in db_players:
            titles_present[player] = (player, 'EXACTLY PRESENT')
            continue
        check_flag = False
        seqmatcher = SequenceMatcher(None, player)

        for existing_player in db_players:
            if SPACE in player and SPACE in existing_player:
                existing_player_set = PLAYERS_PERMUTATIONS.\
                                      get(existing_player, [existing_player])
            else:
                existing_player_set = [existing_player]

            for ex_player in existing_player_set:
                seqmatcher.set_seq2(ex_player)
                if seqmatcher.ratio() >= THRESHOLD:
                    mstly_matched_titles[player] = existing_player
                    titles_present[player] = (player, 'MOSTLY PRESENT')
                    check_flag = True
                    break

            if check_flag: break
        else:
            titles_present[player] = (player, 'NOT PRESENT')

    mostly_present = [player for player, details in titles_present.items() \
                     if details[1] == 'MOSTLY PRESENT']
    exactly_present = [player for player, details in titles_present.items() \
                     if details[1] == 'EXACTLY PRESENT']
    not_present = [player for player, details in titles_present.items() \
                     if details[1] == 'NOT PRESENT']
    return exactly_present, mostly_present, not_present, mstly_matched_titles


WIKI_LINK = "https://en.wikipedia.org/?curid="
class PlayersMerge(VTVSpider):
    name = 'player_merge'
    start_urls = []
    def __init__(self):
                #self.conn   = MySQLdb.connect(host="10.28.216.45", user="veveo", passwd="veveo123", db="SPORTSDB_DEV", charset='utf8', use_unicode=True)
                self.conn   = MySQLdb.connect(host="10.28.218.81", user="veveo", passwd="veveo123", db="SPORTSDB", charset='utf8', use_unicode=True)
                self.cursor = self.conn.cursor()

    def start_requests(self):
        f = open('player_check_20161121.txt', 'r+')
        for record in f:
            if "In Old not in New" not in record:
                continue
            data = record.strip().split('#')
            merge = data[0].replace('In Old not in New: ', '')
            wiki_merge = merge.split(' ')[0]
            pl_gid = merge.split(' ')[1].strip()
            wiki_title = data[1]
            wiki_title = wiki_title.replace(" '", "").replace("' ", "").split('(')[0].strip().replace(' cyclist', '').replace(' english footballer', '').strip().split('footballer')[0].strip().split('boxer')[0].strip().split(' runner')[0].strip().replace(' basketball', '').replace(' athlete', '').replace(' wrestler', '').replace(' racing driver', '')
            db_title = data[2].replace(" '", "").replace("' ", "")
            source = record.split('#')[3].split(',')[0].split(':')[-1].replace(" (u'", '').replace("'", '')
            try:
                seed = record.split('#')[3].split(',')[3].split(':')[-1].replace(" (u'", '').replace("'", '')
            except:
                seed = ''
            wiki_title_ = ''
            exactly_present, mostly_present, not_present, mstly_matched_titles = find_players_to_add(wiki_title, [db_title])

            if mostly_present or mstly_matched_titles or not_present:
                if wiki_title.split(' ')[0] in db_title and wiki_title.split(' ')[-1] in db_title:
                    if wiki_title and "WIKI" in wiki_merge:
                        wiki_link = WIKI_LINK + wiki_merge.replace('WIKI', '')
                        #yield Request(wiki_link, callback=self.parse, meta= {'pl_gid': pl_gid, 'db_title': db_title})
                elif wiki_title.split(' ')[0] in db_title and wiki_title.split(' ')[1] in db_title:
                    if wiki_title and "WIKI" in wiki_merge:
                        wiki_link = WIKI_LINK + wiki_merge.replace('WIKI', '')
                        #yield Request(wiki_link, callback=self.parse, meta= {'pl_gid': pl_gid, 'db_title': db_title})
                elif wiki_title.split(' ')[0] in db_title and seed == source and db_title.split(' ')[-1] in wiki_title:
                    if wiki_title and "WIKI" in wiki_merge:
                        wiki_link = WIKI_LINK + wiki_merge.replace('WIKI', '')
                        #yield Request(wiki_link, callback=self.parse, meta= {'pl_gid': pl_gid, 'db_title': db_title})
                elif db_title.split(' ')[0] in wiki_title and db_title.split(' ')[1] in wiki_title:
                    if wiki_title and "WIKI" in wiki_merge:
                        wiki_link = WIKI_LINK + wiki_merge.replace('WIKI', '')
                        #yield Request(wiki_link, callback=self.parse, meta= {'pl_gid': pl_gid, 'db_title': db_title})
                elif seed and record.split('#')[-1].split(',')[1] == record.split('#')[-1].split(',')[4]:
                    if wiki_title and "WIKI" in wiki_merge:
                        wiki_link = WIKI_LINK + wiki_merge.replace('WIKI', '')
                        #yield Request(wiki_link, callback=self.parse, meta= {'pl_gid': pl_gid, 'db_title': db_title})
                elif pl_gid in ['PL46444', 'PL65675', 'PL31507', 'PL60811', 'PL139917', 'PL11116', 'PL153690', 'PL33639', 'PL1901', 'PL146837', 'PL146644', 'PL32394', 'PL35406', 'PL59535', 'PL65929', 'PL111161', 'PL31579', 'PL45353', 'PL153691', 'PL153700', 'PL23470', 'PL31138', 'PL30222', 'PL66472', 'PL150545', 'PL44517', 'PL43441', 'PL49010', 'PL64905', 'PL147591', 'PL15166', 'PL147553', 'PL111591', 'PL126345', 'PL30532', 'PL146790', 'PL154073', 'PL46063', 'PL57170', 'PL28797', 'PL61629', 'PL154980', 'PL147993', 'PL110421', 'PL336', 'PL59590', 'PL12515', 'PL5044', 'PL149984', 'PL110333', 'PL49471', 'PL106219', 'PL57881', 'PL61489', 'PL154690', 'PL51479', 'PL138667', 'PL141116', 'PL45601', 'PL24659', 'PL44770', 'PL103420']:
                    if wiki_title and "WIKI" in wiki_merge:
                        wiki_link = WIKI_LINK + wiki_merge.replace('WIKI', '')
                        #yield Request(wiki_link, callback=self.parse, meta= {'pl_gid': pl_gid, 'db_title': db_title})
                elif not seed:
                
                    if wiki_title and "WIKI" in wiki_merge and 'PL137141' not in pl_gid:
                        wiki_link = WIKI_LINK + wiki_merge.replace('WIKI', '')
                        yield Request(wiki_link, callback=self.parse, meta= {'pl_gid': pl_gid, 'db_title': db_title, 'record': record})

            if exactly_present:
                wiki_link = WIKI_LINK + wiki_merge.replace('WIKI', '')
                yield Request(wiki_link, callback=self.parse, meta= {'pl_gid': pl_gid, 'db_title': db_title})


            if wiki_title != db_title:
                if len(wiki_title.split(' ')) == 2:
                    wiki_title_ = wiki_title.split(' ')[-1] + " " + wiki_title.split(' ')[0].strip()
                if len(wiki_title.split(' ')) == 3 and len(db_title.split(' ')) == 3:
                    wiki_title_ = wiki_title.split(' ')[1] + " " + wiki_title.split(' ')[-1].strip() + " " + wiki_title.split(' ')[0].strip()
                if len(wiki_title.split(' ')) == 3 and len(db_title.split(' ')) == 2:
                    wiki_title_ = wiki_title.split(' ')[1] + " " + wiki_title.split(' ')[0].strip()
                    if wiki_title_ != db_title:
                       wiki_title_ = wiki_title.split(' ')[0].strip() + " " + wiki_title.split(' ')[-1] 
                    if wiki_title_ != db_title:
                        wiki_title_ = wiki_title.split(' ')[1].strip() + " " + wiki_title.split(' ')[-1]
                    if wiki_title_ != db_title:
                        wiki_title_ = wiki_title.split(' ')[-1].strip() + " " + wiki_title.split(' ')[0]

                if len(wiki_title.split(' ')) == 2 and len(db_title.split(' ')) == 3:
                    db_title_ = ''
                    db_title_ = db_title.split(' ')[1] + " " + db_title.split(' ')[0].strip()
                    if db_title_ != wiki_title:
                       db_title_ = db_title.split(' ')[0].strip() + " " + db_title.split(' ')[-1]
                    if db_title_ != wiki_title:
                        db_title_ = db_title.split(' ')[1].strip() + " " + db_title.split(' ')[-1]
                    if db_title_ != wiki_title:
                        db_title_ = db_title.split(' ')[-1].strip() + " " + db_title.split(' ')[0]
                    if db_title_:
                        db_title = db_title_
                if wiki_title_:
                    wiki_title = wiki_title_
                if wiki_title == db_title or wiki_title.replace('-', ' ') == db_title:
                    wiki_link = WIKI_LINK + wiki_merge.replace('WIKI', '') 
                    yield Request(wiki_link, callback=self.parse, meta= {'pl_gid': pl_gid, 'db_title': db_title})

    def parse(self, response):
        sel = Selector(response)
        pl_gid = response.meta['pl_gid'] 
        db_title = response.meta['db_title'].title()
        pl_name = extract_data(sel, '//h1[@id="firstHeading"]//text()').split('(')[0].strip().split('footballer')[0].strip().split('boxer')[0].strip().split(' runner')[0].strip()
        up_qry = 'update sports_participants set title=%s, aka=%s where gid=%s limit 1'
        values = (pl_name, db_title, pl_gid)
        self.cursor.execute(up_qry, values)


