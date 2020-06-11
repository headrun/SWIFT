#!/usr/bin/python
import cgi
import xmlrpclib
import os, sys
import traceback
import string
import marshal

sys.path.insert(0, "/home/veveo/release/server")
sys.path.insert(0, "/home/veveo/release/server/merge_test")
from vtv_utils import *
from related.relatedRecordAnalyzer import relatedDebug

GID_TITLE_DICT = {}
RELATED_MOVIE_MARSHAL_FILE = "/data2/marshals/vtv_seed/related_movie.marshal"
RELATED_TVSERIES_MARSHAL_FILE = "/data2/marshals/vtv_seed/related_tvseries.marshal"
GID_TITLE_MARSHAL = "/data2/marshals/vtv_seed/gid_title.marshal"
DATA_TYPE = 'movie'

def print_form(response, src_gid):
    print '''<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">

<html>
<meta charset="utf-8"/>
<head><title>Related Run Time Analyzer Tool</title>
<style>
.span3 {  
    height: 250px !important;
    overflow: auto;
}
</style>
</head>

<!-- Bootstrap -->
<!-- link href="/bootstrap/css/bootstrap.min.css" rel="stylesheet" media="screen" -->
<link href="//netdna.bootstrapcdn.com/bootstrap/3.0.2/css/bootstrap.min.css" rel="stylesheet" media="screen" >
</head>
<body> 
<link rel="stylesheet" href="//code.jquery.com/ui/1.10.4/themes/smoothness/jquery-ui.css">
<script src="https://code.jquery.com/jquery.js"></script>
<div class="container">
<div class="navbar navbar-default">
<div class="navbar-inner">

<form class="navbar-form" action="" method="POST">
    <div class="form-group">    
    %s<input class="btn btn-primary" name="commit" type="submit" value="Execute" />
    </div>
</div>
</form>
</div>
</div>
<br>
<br>
<div class="row-fluid">
<div class="span12">
</div>
</div>
%s<br>
<!-- script src="/bootstrap/js/bootstrap.min.js"></script -->
<script src="//netdna.bootstrapcdn.com/bootstrap/3.0.2/js/bootstrap.min.js"></script>
<script>$(function (){ $("[data-toggle='popover']").popover();});</script>
</div>
</body>
</html>''' % (print_input(), print_response(response, src_gid))

def extract_data(debug_string):
    data = debug_string.rsplit(',', 2)
    ex_score = data[1].split(':')[1].strip()
    penalty = data[2].split(':')[1].strip()
    string = data[0]
    return string, ex_score, penalty

def add_info_cmd(src_info, cmd):
    if not src_info:
        return ''
    key_map = {'Good Genre':'Gg', 'Genre':'Ge', 'Director':'Di', 'Producer':'Pr', 'Writer':'Wr', 'Imp. Crew':'Ic','Sequel':'Sq', 'Language':'Ll', 'Keyword':'Ke', 'Wiki Links':'Wl', 'Title':'Ti', 'Gid':'Gi'}
    keys = ['Title', 'Gid', 'Good Genre', 'Genre', 'Director', 'Producer', 'Writer', 'Imp. Crew', 'Ry', 'Sequel', 'Language', 'Wiki Links', 'Keyword']
    cmd += '''<table class="table table-bordered ">\n<tbody>\n'''
    cmd += '''<tr bgcolor=088A68><td align="left"></td><th>Src Record</th></tr>\n'''
    for key in keys:
        cmd += '''<tr bgcolor=CBCBCB><th align="left">%s</th><td>%s</td></tr>\n'''%(key, src_info.get(key_map.get(key, key), ''))    
    cmd += '''</tbody></table>\n'''
    return cmd

def update_value_with_title(values):
    new_val = []
    if not values.strip():
        return ''
    for val in values.split('<>'):
        title = GID_TITLE_DICT.get(val, '').encode('ascii', 'ignore')
        if title:
            new_val.append('%s{%s}'%(title, val.upper()))
        else:
            new_val.append(val)    
    return '<>'.join(new_val) 

def update_records_info(records_info):
    for record, record_value in records_info.iteritems():
        for key in record_value:
            value = record_value.get(key, '')
            record_value[key] = update_value_with_title(value)
    return records_info

def print_records_info(records_info, src_gid):
    cmd = ''''''
    src_info = records_info.get(src_gid, {})
    cmd = add_info_cmd(src_info, cmd)
    return cmd

def get_common_vals(src_gid, target_gid, records_info):
    common_val_dict = {}
    src_record = records_info.get(src_gid, '')
    target_record = records_info.get(target_gid, '')
    if not src_record or not target_record:
        return common_val_dict
    for key in src_record:
        source_field_val = src_record.get(key, '')
        target_field_val = target_record.get(key, '')
        source_val_set = set(source_field_val.split('<>'))
        target_val_set = set(target_field_val.split('<>'))
        common_set = source_val_set.intersection(target_val_set)
        common_val_dict[key] = '<>'.join(list(common_set))
    return common_val_dict    

def get_debug_field(debug_string, src_gid, target_gid, records_info, key="value"):
    field_string = ""
    value_string = ""
    field_scores = debug_string.strip().split(',') 
    common_val_dict = {} 
    if key == "value": 
        common_val_dict = get_common_vals(src_gid, target_gid, records_info)
    for field_score in field_scores:
        field, score = field_score.strip().split(':')
        if field == "ExSc":
            field = "External Score"
        field_string += "<th>%s</th>"%(field)
        common_val = common_val_dict.get(field, '')
        if common_val:
            value_string += "<td><a href='#' data-toggle='tooltip' title='%s'>%s</a></td>"%(common_val, score)
        else:    
            value_string += "<td>%s</td>"%(score)
    if key == "field":
        return field_string
    return value_string    

def print_response(response, src_gid):
    resp_type = response[0]
    records_info = response[1]
    resp_data = response[2]
    cmd = ''''''
    cmd = print_records_info(records_info, src_gid)

    cmd += '''<table class="table table-bordered ">\n<tbody>\n'''

    sn = 1
    if resp_type == 0:
        cmd += '''<tr bgcolor=FA5858><td align="centre"><a href="#" data-toggle="tooltip" title="amit suthar">%s</a></td></tr>'''%(resp_data) 
        cmd += '''<tr bgcolor=FA5858><td align="centre">%s</td></tr>'''%(resp_data) 
    elif resp_type == 1: #Target gid mentioned
        debug_fields = get_debug_field(resp_data[0][3], '', '', '', key="field")
        cmd += '''<tr bgcolor=088A68><th align="right">Sn.</th><th>Rel. Gid</th><th>Title</th><th>Score</th>%s<th>External Score</th><th>Disp. Penalty</th></tr>\n'''%(debug_fields)
        related = resp_data
        gid = resp_data[0][2]
        title = GID_TITLE_DICT.get(gid, '').encode('ascii', 'ignore')
        debug_string = resp_data[0][3]
        pri_score = resp_data[2]
        penalty = resp_data[1]
        ex_score = resp_data[3]
        overall_score = resp_data[4]
        wiki_id = gid.lstrip('WIKI')
        cmd += '''<tr bgcolor=CBCBCB><td align="right">%s</td><td><a href="http://en.wikipedia.org/wiki/?curid=%s">%s</td><td>%s</td><td>%s</td>%s<td>%s</td><td>%s</td></tr>\n'''%(sn, wiki_id, gid, title, overall_score, get_debug_field(debug_string, src_gid, gid, records_info), ex_score, penalty)
    else: #Target Gid not mentioned
        debug_fields = get_debug_field(resp_data[0][0][3], '', '', '', key="field")
        cmd += '''<tr bgcolor=088A68><th align="right">Sn.</th><th>Rel. Gid</th><th>Title</th><th>Score</th>%s</tr>\n'''%(debug_fields)
        related = resp_data[0]
        for rel in related:
            gid = rel[2]
            title = GID_TITLE_DICT.get(gid, '').encode('ascii', 'ignore')
            overall_score = rel[0]
            debug_string = rel[3]
            wiki_id = gid.lstrip('WIKI')
            #debug_string, ex_score, penalty = extract_data(debug_string)
            cmd += '''<tr bgcolor=CBCBCB><td align="right">%s</td><td><a href="http://en.wikipedia.org/wiki/?curid=%s">%s</td><td>%s</td><td>%s</td>%s</tr>\n'''%(sn, wiki_id, gid, title, overall_score, get_debug_field(debug_string, src_gid, gid, records_info))
            sn += 1

    cmd += '''</tbody></table>\n'''
        
    return cmd

def print_input():
    cmd = ''''''    
    cmd += '''<table class="table table-bordered table-hover">\n<tbody><tr bgcolor=CBCBCB>\n'''
    cmd += '''<td align="right">Source Gid</td><td><input name=src_gid size="20" type="text" value=""/></td>\n '''
    cmd += '''<td align="right">Target Gid</td><td><input name=target_gid size="20" type="text" value=""/></td>\n '''
    cmd += '''<td align="right">Record Type</td><td><select name=rec_type><option value="movie">Movie</option><option value="tvseries">TvSeries</option></select></td>\n '''
    cmd += '''</tr>\n</tbody></table>\n'''
    return cmd

def get_response(src_gid, target_gid, rec_type):
    rec_type = "movie"
    if not src_gid:
        string = "Please enter gid to get related. Enter Src Gid only for all top 200 Related gids. Enter both to get relevance of two gids."
        return [0, {}, string]
    rd = relatedDebug()
    rd.options.data_type = rec_type
    rd.options.source_gid = src_gid
    rd.options.target_gid = target_gid
    if rec_type == "movie":
        rd.options.marshal_file = RELATED_MOVIE_MARSHAL_FILE
    else:    
        rd.options.marshal_file = RELATED_TVSERIES_MARSHAL_FILE
    rd.call_function(rd.getFieldParams)
    rd.call_function(rd.initializeSimilarEngine)
    rd.call_function(rd.loadMarshal)
    rd.call_function(rd.createDFhash)
    status = rd.call_function(rd.checkGenreConditions)
    gid_list = [src_gid]
    if status:
        gid_list.append(target_gid)
        code, result = 0, status
    else:
        related_gids, penalty, overall_score, ex_score, final_score = rd.call_function(rd.getRelatedDetails)
        gid_list.extend([rel[2] for rel in related_gids])
        if target_gid:
            code, result = 1, (related_gids[0], penalty, overall_score, ex_score, final_score)
        else:
            code, result = 2, (related_gids, penalty, overall_score, ex_score, final_score)
    records_info = rd.call_function(rd.getRecordsinfo, (gid_list,))
    records_info = update_records_info(records_info)
    return [code, records_info, result]        

    if status:
        return [0, records_info, status]
    related_gids, penalty, overall_score, ex_score, final_score = rd.call_function(rd.getRelatedDetails)
    if target_gid:
        return [1, records_info, (related_gids[0], penalty, overall_score, ex_score, final_score)]   #Target gid mentioned
    else:
        return [2, records_info, (related_gids, penalty, overall_score, ex_score, final_score)]   #Target gid not mentioned 

def update_gid_title_dict():
    data = marshal.load(open(GID_TITLE_MARSHAL))
    global GID_TITLE_DICT
    GID_TITLE_DICT = data

def mashup_engine():
    update_gid_title_dict()
    print "Content-type: text/html"
    form = cgi.FieldStorage()
    src_gid = form.getvalue("src_gid", '')
    target_gid = form.getvalue("target_gid", '')
    rec_type = form.getvalue("rec_type", '')
    response = get_response(src_gid, target_gid, rec_type)

    print_form(response, src_gid)
    sys.exit(0)

if __name__ == '__main__':
    mashup_engine()
