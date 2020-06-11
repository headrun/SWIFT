#!/usr/bin/python

import xmlrpclib
import os, sys
import cgi
import math
import traceback
from sqlUtil import SqlUtil
import string
import re
import marshal
import socket

sys.path.insert(0, "/home/veveo/release/server")
import StringUtil
import urllib
from vtv_utils import *

html = """<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
    <title> Mashup Related Programs </title>
</head>
<body>
    <br />
    %s
</body>
</html>
"""

class RelatedProgram:
    def __init__(self, logger):
        self.logger = logger
        self.archival_max_score_hash = {}
        self.archival_similar_hash = {}
        self.df_hash = {}
        self.ip_address = socket.gethostbyname(socket.gethostname())
        self.readConfigFile("marshal_dump.conf")

        self.db_name = "RECO_DUMP"
        self.table_name = "vtv_seed"
        self.sql_obj = SqlUtil(self.db_name, self.logger)

    def readConfigFile(self, file_name):
        '''movie marshal dir, tvseries marshal dir'''
        self.dump_location = {}
        if not os.path.exists(file_name):
            return 
        f = open(file_name)
        for line in f:
            line = line.strip()
            if not line:
                continue            
            name, location = line.split('=')
            if not name.strip() or not location.strip():                
                continue
            self.dump_location[name.strip().lower()] = location.strip()
        
    def update_gid_marshal_map(self):
        self.gid_marshal_map = {}
        movie_key_file = os.path.join(self.dump_location['movie'], 'key_file_map_movie')
        tvseries_key_file = os.path.join(self.dump_location['tvseries'], 'key_file_map_tvseries')
        key_file_list = [movie_key_file, tvseries_key_file]
        for key_file in key_file_list:
            if os.path.exists(key_file):
                f = open(key_file)
                for line in f:
                    if line.strip():
                        gid, f_name = line.strip().split('#')
                        self.gid_marshal_map[gid.strip()] = f_name.strip()

    def search_keyword(self, keyword):
        suggestions = []
        keyword = StringUtil.cleanString(keyword)
        query = "Select * from %s where REC_TYPE='title' and VAL like %s"%(self.table_name, keyword)
        result = self.sql_obj.fetchAll(query)
        suggestions = [ (row[0], row[1]) for row in result ]
        return suggestions

    def getRelated(self, gid):
        marshal_file = self.gid_marshal_map.get(gid, '')
        if not os.path.exists(marshal_file):
            print "No related found :("
            return
        data = marshal.load(open(marshal_file))
        related = data[4][gid]
        return related
    
def get_attr_value(attr, schema, record):
    if attr not in schema:
        return ''
    elif len(record) >= schema[attr]+1:
        return record[schema[attr]]
    else:
        return ''

def print_suggestions(simprogram, search_string, suggestions):
    str="<div> Suggetion for search string: %s</div>\n"%search_string
    print html%str
    print '<table align=left bgcolor="wheat" width=70%>'
    print '<tr><td>'
    print '<table>'
    print '<b><tr bgcolor=dodgerblue><td>Gid</td> <td>Title</td></tr></b>'
    for gid, title in suggestions:
        title = title.encode('ascii', 'ignore')
        print '<tr bgcolor="wheat" ><td>%s</td><td><a href="related_client.py?action=related+%s">%s</a></td></tr>'%(gid, gid, title)
    print '</table>'

def print_related(simprogram, guid, related):
    cur_rec = simprogram.getGidInfo(guid, 'metaInfo')
    PROG_SCHEMA = simprogram.PROG_SCHEMA
    gid = get_attr_value('Gi', PROG_SCHEMA, cur_rec)
    title =get_attr_value('Ti', PROG_SCHEMA, cur_rec).encode('ascii', 'ignore')
    gkwds = get_attr_value('Gk', PROG_SCHEMA, cur_rec)
    gkwds_set = set(gkwds.split('<>'))
    genres = get_attr_value('Ge', PROG_SCHEMA, cur_rec)
    genres_set = set(genres.split('<>'))
    super_genres = get_attr_value('Su', PROG_SCHEMA, cur_rec)
    super_genre_set = set(super_genres.split('<>'))        
    country = get_attr_value('Cl', PROG_SCHEMA, cur_rec)
    country_set = set(country.split('<>'))
    ryear = get_attr_value('Ry', PROG_SCHEMA, cur_rec)
    rating = get_attr_value('Ra', PROG_SCHEMA, cur_rec)
    min_rating = get_attr_value('Rn', PROG_SCHEMA, cur_rec)
    max_rating = get_attr_value('Rx', PROG_SCHEMA, cur_rec)
    rating = '%s|%s|%s'%(min_rating.upper(), rating, max_rating.upper())
    language = get_attr_value('Ll', PROG_SCHEMA, cur_rec)
    language_set = set(language.split('<>'))
    good_genres = get_attr_value('Gg', PROG_SCHEMA, cur_rec)
    good_genres_set = set(good_genres.split('<>'))
    kwds = get_attr_value('Ke', PROG_SCHEMA, cur_rec)
    kwds_set = set(kwds.split('<>'))
    
    if not related:
        print "Could not find any related :("
        return        
    str="<div> Related Programs for: %s:<a href='related_client.py?action=%s'>%s</a><br><b>Gk:</b> %s<br><b>Ke:</b> %s<br><b>Gg:</b> %s<br><b>Ge:</b> %s<br><b>Su:</b> %s<br><b>Cl:</b> %s<br><b>Ry:</b> %s<br><b>Ll:</b>%s<br><b>Ra:</b> %s</div>\n"%(guid, "related+"+guid, title, gkwds, kwds, good_genres, genres, super_genres, country, ryear, language, rating)
    print html%str
    print '<table align=left bgcolor="wheat" border="1">'
    print '<tr><td>'
    print '<table border="1">'
    print '<b><tr bgcolor=dodgerblue><td>Sn.</td><td>Gid</td><td> Title <size = 80></td><td>Good Keywords</td><td>Keywords</td><td>Good Genres</td><td>Genres</td><td>Super Genres</td><td>Country</td><td>RYear</td><td>Language</td><td>Rating</td><td>Score</td><td>Debug Score</td></tr></b>'
    num = 0
    for cur_rec in related:
        num += 1
        final_score, score, gid, debug_string, uo_score = cur_rec
        #score, gid, debug_string = cur_rec
        meta_rec = simprogram.getGidInfo(gid, 'metaInfo')
        cur_title, cur_genres, cur_kwds, cur_country, cur_ryear = '', '', '', '', ''
        cur_title = get_attr_value('Ti', PROG_SCHEMA, meta_rec).encode('ascii', 'ignore')
        cur_ryear = get_attr_value('Ry', PROG_SCHEMA, meta_rec)
        cur_genres = get_attr_value('Ge', PROG_SCHEMA, meta_rec)
        cur_rating = get_attr_value('Ra', PROG_SCHEMA, meta_rec)
        cur_min_rating = get_attr_value('Rn', PROG_SCHEMA, meta_rec) 
        cur_max_rating = get_attr_value('Rx', PROG_SCHEMA, meta_rec)
        cur_rating = '%s|%s|%s'%(cur_min_rating.upper(), cur_rating, cur_max_rating.upper())
        if cur_genres:
            cur_genres_set = set(cur_genres.split('<>'))
            cur_genres = cur_genres_set.intersection(genres_set)
        good_cur_kwds = get_attr_value('Gk', PROG_SCHEMA, meta_rec)
        if good_cur_kwds:
            good_cur_kwds_set = set(good_cur_kwds.split('<>'))
            good_cur_kwds = good_cur_kwds_set.intersection(gkwds_set)
        cur_lang = get_attr_value('Ll', PROG_SCHEMA, meta_rec)
        if cur_lang:
            cur_lang_set = set(cur_lang.split('<>'))
            cur_lang = cur_lang_set.intersection(language_set)
        cur_country = get_attr_value('Cl', PROG_SCHEMA, meta_rec)
        if cur_country:
            cur_country_set = set(cur_country.split('<>'))
            cur_country = cur_country_set.intersection(country_set)
        cur_good_genres = get_attr_value('Gg', PROG_SCHEMA, meta_rec)
        if cur_good_genres:
            cur_good_genres_set = set(cur_good_genres.split('<>'))
            cur_good_genres = cur_good_genres_set.intersection(good_genres_set)
        cur_super_genres = get_attr_value('Su', PROG_SCHEMA, meta_rec)
        if cur_super_genres:
            cur_super_genres_set = set(cur_super_genres.split('<>'))
            cur_super_genres = cur_super_genres_set.intersection(super_genre_set)        
        cur_kwds = get_attr_value('Ke', PROG_SCHEMA, meta_rec)
        if cur_kwds:
            cur_kwds_set = set(cur_kwds.split('<>'))
            cur_kwds = cur_kwds_set.intersection(kwds_set)   

        print '<tr bgcolor="wheat" ><td>%s</td><td nowrap><a href="related_client.py?action=related+%s">%s</a></td><td><no_wrap="">%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'%(num, gid, gid, cur_title, good_cur_kwds, cur_kwds, cur_good_genres, cur_genres, cur_super_genres, cur_country, cur_ryear, cur_lang, cur_rating, final_score, debug_string)
    print '</table>'

def get_cgi_params(*args):
    params = {}
    f = cgi.FieldStorage()
    for eachparam in args:
        params[eachparam] = f.getvalue(eachparam, "")
    return params

def mashup_engine():
    print "Content-type: text/html"
    print
    logger = initialize_logger("/tmp/related_client.log")
    params = get_cgi_params('action')
    inp = params.get("action",None)
    arguments = None
    if ' ' in inp:
        action_id, arguments = inp.split(' ',1)
    else:
        action_id = inp
  
    relProgram = RelatedProgram(logger)
    if action_id == 'related':
        related = relProgram.getRelated(arguments)
        print_related(relProgram, arguments, related)
    elif action_id == 'search':
        suggestions = relProgram.search_keyword(arguments)
        print_suggestions(relProgram, arguments, suggestions) 
    else:
        print html%"Specify 'related-string-query' as http://%s/tools/related_client.py?action=related+gid\n"%(relProgram.ip_address)
        string = "i.e. To search Programs containing string 'lord' query should be\n"
        print html%string
        string = "<div><a href='related_client.py?instance=%s&action=search+lord'> http://%s/related_client.py?action=search+lord</a></div>\n"%(relProgram.ip_address)
        print html%string

if __name__ == '__main__':
    mashup_engine()

