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
from relatedDiffTool import RelatedDiffTool

#sys.path.insert(0, "/home/veveo/release/server")
sys.path.insert(0, "/home/amit-kumar/related_20130905/pratham/build/vkc/release/server")
import urllib
from vtv_utils import *

html = """<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<html>
<head>
    <title> Related Diff/Stats Tool </title>
</head>
<script type="text/javascript">
function toggleMe(a){
    var e=document.getElementById(a);
    if(!e)return true;
    if(e.style.display=="none"){
        e.style.display="block"
    }
    else{
        e.style.display="none"
    }
    return true;
}
</script>
<body>
    <br />
    %s
</body>
</html>
"""

OP, DATA = '', ''

def get_cgi_params(*args):
    params = {}
    f = cgi.FieldStorage()
    for eachparam in args:
	params[eachparam] = f.getvalue(eachparam, "")
    return params

def print_diff_result(keyword, result):
    table_rel_dict, rel_group_map, gid_title_map = result
    color_list = ['00FFFF', 'FF99FF', 'FFFFcc', 'AAAAAA', 'BBBBBB', 'CCCCCC']
    groups = set(rel_group_map.values())
    group_color_map = {}
    index = 0
    for group in groups:
        if len(group.split('<>')) > 1:
            group_color_map[group] = color_list[index]
        index += 1
    tables = table_rel_dict.keys()
    print html%("<h4>Comapring related for : %s</h4>"%(keyword))
    print "<div>"
    for table in tables:        
        print "<table align=left border='1'>"
        print "<tr bgcolor='lightgreen'>"
        print "<th colspan='2'>%s</th>"%(table)
        for rel in table_rel_dict[table]:
            gid_link = '<a href="relatedDiffClient.py?op=%s&data=%s&action=search+%s">%s</a>'%(OP, DATA, rel, rel)
            print "<tr bgcolor='%s'><td>%s</td><td>%s</td></tr>"%(group_color_map.get(rel_group_map[rel], ''), gid_link, gid_title_map[rel])
    print "</table>"
    print "</div>"    

def print_diff_stats(results):
    toggle_str = '''<input type="button" onclick="return toggleMe('%s')" value="Show/Hide"><br><div id="%s" style="display:none">'''
    print html%("<h4>Related Diff Stats</h4>")
    print "<br>Common gids with related: %d"%len(results[0])
    print toggle_str%('common', 'common')
    print_set(results[0])
    print '</div><br>'
    diff_gids_tables = results[1].keys()
    for diff_table in diff_gids_tables:
        print "Different gids for %s : %d"%(diff_table, len(results[1][diff_table]))
        print toggle_str%(diff_table, diff_table)
        print_set(results[1][diff_table])
        print '</div><br>'
    #Common Gids with different set of related   
    common_set = set()    
    levels = results[2].keys()
    for level in levels:
        if level < 4:
            print "Gids having %d to %d different related: %d"%(5*level, 5*level+4, len(results[2][level]))       
        else:
            print "Gids having 20 and more different related: %d"%(len(results[2][level]))      
        print toggle_str%(level, level)
        print_set(results[2][level])
        print '</div><br>'
        common_set.update(results[2][level])

    # common Gids with same set but different order of realted    
    print "Gids having same related but different order: %d"%(len(results[3]))
    print toggle_str%('order', 'order')
    print_set(results[3])
    print '</div><br>'
    common_set.update(results[3])
    
    #Gids having exactly same related
    similar_set = results[0]-common_set
    print "Gids having exactly same related: %d"%(len(similar_set))
    print toggle_str%('same', 'same')
    print_set(similar_set)
    print '</div><br>'

def print_set(value_set):
    for gid in value_set:
        print '<a href="relatedDiffClient.py?op=%s&data=%s&action=search+%s">%s</a>'%(OP, DATA, gid, gid)
        
def related_engine():
    print "Content-type: text/html"
    print
    logger = initialize_logger("relatedDiff_client.log")
    params = get_cgi_params('op', 'data', 'action')
    operator = params.get("op", None)
    data = params.get("data", None)
    global OP
    OP = operator
    global DATA
    DATA = data
    action = params.get("action", None)
    if not operator or not data:
        print "Operator and data table name not mentioned. Don't know which data comparision you want :("
        return
    if ' ' in action:
        action_name, keyword = action.split(' ', 1)
    else:
        action_name = action

    if action_name not in ('update', 'search', 'get_stats') :
        print "Unknow action. I only knows about update and search"
        return
    # instantiate diff tool calculator
    rdt = RelatedDiffTool(operator, data, '', None, logger)    
    if action_name == 'update':
        rdt.init_tables()
        print "Tool data updated successfully. Ready for search"
        return

    if action_name == 'search':
        result = rdt.gererateRelatedComparision(keyword)
        print_diff_result(keyword, result)
        return

    if action_name == 'get_stats':
        if not len(rdt.tables[rdt.db_name]) == 2:
            print "comparision status only valid for 2 datas"
            return
        result = rdt.generateRelatedStats()
        print_diff_stats(result)
        return    
        
if __name__ == '__main__':
    related_engine()
