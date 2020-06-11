#!/usr/bin/python

import xmlrpclib
import os, sys
import cgi
import math
import traceback
import MySQLdb
import string
import re
import marshal
import socket

sys.path.insert(0, "/home/veveo/release/server")
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

class SimilarProgram:
    def __init__(self, logger, port):
        self.logger = logger
        self.archival_max_score_hash = {}
        self.archival_similar_hash = {}
        self.df_hash = {}
        self.ip_address = socket.gethostbyname(socket.gethostname())
        self.port = int(port)
        self.gid_marshal_map = {}
        self.dump_location = {}
        self.readConfigFile("marshal_dump.conf")
        self.key_file_map = {'movie':'key_file_map_movie', 'tv':'key_file_map_tvseries'}
        self.PROG_SCHEMA = self.getGidInfo('dummy123', 'schema')

    def readConfigFile(self, file_name):
        '''movie marshal dir, tvseries marshal dir'''
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
            if name.strip() == 'TVSERIES':
                name = 'tv'    
            self.dump_location[name.strip().lower()] = location.strip()

    def update_gid_marshal_map(self):
        for ty in self.key_file_map:
            key_file = self.key_file_map[ty]
            dump_location = self.dump_location[ty]
            key_map_file = os.path.join(dump_location, key_file)
            if os.path.exists(key_map_file):
                f = open(key_map_file)
                for line in f:
                    if line.strip():
                        gid, f_name = line.strip().split('#')
                        self.gid_marshal_map[gid.strip()] = f_name.strip()+'.marshal'

    def getRelated(self, gid):
        marshal_file = self.gid_marshal_map.get(gid, '')
        if not os.path.exists(marshal_file):
            print "No related found :("
            return
        instance_hash = readConfigFile('ToolInstances.cfg')    
        port_set = instance_hash['vtv_latest']    
        for port, ty in port_set:
            if ty in marshal_file:
                self.port = int(port)
        data = marshal.load(open(marshal_file))
        related = data[gid]
        return related

    def getGidInfo(self, guid, query_type):       
        if query_type == "related" and self.gid_marshal_map:
            output_data = self.getRelated(guid) 
            self.PROG_SCHEMA = self.getGidInfo('dummy123', 'schema')
            return output_data
        s  = socket.socket()
        host = socket.gethostname()
        s.connect((host, self.port))
        s.send(guid+'|'+query_type)
        response_data = ''
        while True:
            data = s.recv(4096)
            if len(data) == 0:break
            response_data += data
        output_data = marshal.loads(response_data)
        s.close()
        return output_data

    def searchRecords(self, keyword, query_type):
        suggestions = self.getGidInfo(keyword, query_type)
        return suggestions

def get_attr_value(attr, schema, record):
    if attr not in schema:
        return ''
    elif len(record) >= schema[attr]+1:
        return record[schema[attr]]
    else:
        return ''

def print_suggestions(simprogram, search_string, suggestions, instance):
    str="<div> Suggetion for search string: %s</div>\n"%search_string
    print html%str
    print '<table align=left bgcolor="wheat" width=70%>'
    print '<tr><td>'
    print '<table>'
    print '<b><tr bgcolor=dodgerblue><td>Gid</td> <td>Title</td></tr></b>'
    for gid, title in suggestions:
        title = title.encode('ascii', 'ignore')
        print '<tr bgcolor="wheat" ><td>%s</td><td><a href="similar_client.py?instance=%s&action=related+%s">%s</a></td></tr>'%(gid, instance, gid, title)
    print '</table>'

def print_related(simprogram, guid, related, instance):
    cur_rec = simprogram.getGidInfo(guid, 'metaInfo')
    PROG_SCHEMA = simprogram.PROG_SCHEMA
    gid = get_attr_value('Gi', PROG_SCHEMA, cur_rec)
    title =get_attr_value('Ti', PROG_SCHEMA, cur_rec).encode('ascii', 'ignore')
    subgenre = get_attr_value('Sg', PROG_SCHEMA, cur_rec)
    subgenre_set = set(subgenre.split('<>'))
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
    str="<div> Related Programs for: %s:<a href='similar_client.py?instance=%s&action=%s'>%s</a><br><b>Ke:</b> %s<br><b>Gg:</b> %s<br><b>Ge:</b> %s<br><b>Su:</b> %s<br><b>Cl:</b> %s<br><b>Ry:</b> %s<br><b>Ll:</b>%s<br><b>Ra:</b> %s</div>\n"%(guid, instance, "related+"+guid, title, kwds, good_genres, genres, super_genres, country, ryear, language, rating)
    print html%str
    print '<table align=left bgcolor="wheat" border="1">'
    print '<tr><td>'
    print '<table border="1">'
    print '<b><tr bgcolor=dodgerblue><td>Sn.</td><td>Gid</td><td> Title <size = 80></td><td>Keywords</td><td>Good Genres</td><td>Genres</td><td>Super Genres</td><td>Country</td><td>RYear</td><td>Language</td><td>Rating</td><td>Score</td><td>Debug Score</td></tr></b>'
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
            cur_genres = '<>'.join(list(cur_genres_set.intersection(genres_set)))
        cur_subgenres = get_attr_value('Sg', PROG_SCHEMA, meta_rec)
        if cur_subgenres:
            cur_subgenres_set = set(cur_subgenres.split('<>'))
            cur_subgenres = '<>'.join(list(cur_subgenres_set.intersection(subgenre_set)))
        cur_lang = get_attr_value('Ll', PROG_SCHEMA, meta_rec)
        if cur_lang:
            cur_lang_set = set(cur_lang.split('<>'))
            cur_lang = '<>'.join(list(cur_lang_set.intersection(language_set)))
        cur_country = get_attr_value('Cl', PROG_SCHEMA, meta_rec)
        if cur_country:
            cur_country_set = set(cur_country.split('<>'))
            cur_country = '<>'.join(list(cur_country_set.intersection(country_set)))
        cur_good_genres = get_attr_value('Gg', PROG_SCHEMA, meta_rec)
        if cur_good_genres:
            cur_good_genres_set = set(cur_good_genres.split('<>'))
            cur_good_genres = '<>'.join(list(cur_good_genres_set.intersection(good_genres_set)))
        cur_super_genres = get_attr_value('Su', PROG_SCHEMA, meta_rec)
        if cur_super_genres:
            cur_super_genres_set = set(cur_super_genres.split('<>'))
            cur_super_genres = '<>'.join(list(cur_super_genres_set.intersection(super_genre_set)))
        cur_kwds = get_attr_value('Ke', PROG_SCHEMA, meta_rec)
        if cur_kwds:
            cur_kwds_set = set(cur_kwds.split('<>'))
            cur_kwds = '<>'.join(list(cur_kwds_set.intersection(kwds_set)))

        print '<tr bgcolor="wheat" ><td>%s</td><td nowrap><a href="similar_client.py?instance=%s&action=related+%s">%s</a></td><td><no_wrap="">%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'%(num,instance, gid, gid, cur_title, cur_kwds, cur_good_genres, cur_genres, cur_super_genres, cur_country, cur_ryear, cur_lang, cur_rating, final_score, debug_string)
    print '</table>'

def get_cgi_params(*args):
    params = {}
    f = cgi.FieldStorage()
    for eachparam in args:
        params[eachparam] = f.getvalue(eachparam, "")
    return params

def readConfigFile(file_name):
    instance_hash = {}
    if not os.path.exists(file_name):
        return instance_hash
    f = open(file_name)
    for line in f:
        line = line.strip()
        if not line:
            continue
        ty, name, port, marshal = line.split('\t')
        if not name.strip() or not port.strip():
            continue
        port_set = instance_hash.setdefault(name.strip(), set())
        port_set.add((port.strip(), ty.strip()))    
    return instance_hash    

def mashup_engine():
    print "Content-type: text/html"
    print
    logger = initialize_logger("/tmp/similar_client.log")
    server_port = get_cgi_params('instance')
    instance = server_port.get("instance", None)
    instance_hash = readConfigFile('ToolInstances.cfg')
    if not instance:
        print "Instance name not mentioned. Check instance name from below config or Configuration file"
        print "<br>Instances: %s"%(instance_hash.keys())
        print "<br>"
        return
    elif instance not in instance_hash:
        print "Invalid instance Name"   
        return 
    params = get_cgi_params('action')
    input = params.get("action",None)
    arguments = None
    if ' ' in input:
        action_id, arguments = input.split(' ',1)
    else:
        action_id = input
    port_set = instance_hash[instance]
    related, suggestions, simprogram = None, None, None
    for port, ty in port_set:
        simprogram = SimilarProgram(logger, port)
        if instance and instance == 'vtv_latest':
            simprogram.update_gid_marshal_map()
        if action_id == 'related':
            related = simprogram.getGidInfo(arguments, 'related')
            if related:
                break
        elif action_id == 'search':
            suggestions = simprogram.searchRecords(arguments, action_id)
            if suggestions:
                break
        elif action_id == 'filter':
            suggestions = simprogram.searchRecords(arguments, action_id)
            if suggestions:
                break
    if action_id == 'related':        
            print_related(simprogram, arguments, related, instance)
    elif action_id in ('search', 'filter'):
            print_suggestions(simprogram, arguments, suggestions, instance) 
    else:
        print html%"Specify 'related-string-query' as http://%s/tools/similar_client.py?instance=%s&action=related+gid\n"%(simprogram.ip_address, instance)
        str = "i.e. To search Programs containing string 'lord' query should be\n"
        print html%str
        str = "<div><a href='similar_client.py?instance=%s&action=search+lord'> http://%s/similar_client.py?instance=%s&action=search+lord</a></div>\n"%(instance,simprogram.ip_address, instance)
        print html%str
    
if __name__ == '__main__':
    mashup_engine()
