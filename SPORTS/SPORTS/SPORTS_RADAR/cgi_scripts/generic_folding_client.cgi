#!/usr/bin/python

import xmlrpclib
import os, sys
import cgi
import traceback
import MySQLdb
import string
import re

#from vtv_utils import initialize_logger, get_mysql_connection
#For this to work, we need to set the pythonpath. Do the following in the machine in which you are hosting this client.
#* import distutils.sysconfig
#* print distutils.sysconfig.get_python_lib()
# This will print the location of your "site-packages" directory. Visit that directory (from the command-line or Windows Explorer) and create a file in it called "MyPythonPath.pth" (or something else with the extension ".pth"). In it, include each value you wanted to add to your PYTHONPATH on separate lines:
#Ex: 
#/home/veveo/release/server/
#/home/data01/arun/sql_dbserver/pratham/make/vkc/release/server/

import urllib
from vtv_utils import *

#success header
def print_success_header():
    print '''status: 200 OK\r\nContent-Type: text/html\r\n\r\n'''

#start of html
def print_start_html():
    print '''<html>'''
    print
    print_css()
    print '''<body>'''
    print_header()

#page header
def print_header():
    print '''<div align="center">'''
    print '''<h4>Generic Folding Tool</h4>'''
    print '''</div><p></p>'''


#css description
def print_css():
    print '''
    <style type="text/css">
        .field_class {
        border-style:groove;
        background: lightBlue;
        -moz-border-radius:5px;
        border-radius: 5px;
        -webkit-border-radius: 5px;
        }
        .left_side{
        float:left;
        width:49.5%;
        }
        .right_side{
        float:right;
        width:49.5%;
        }
        legend {
        font: bold 330%/100% "Lucida Grande", Arial, sans-serif;
        padding: 0.2em 0.5em;
        border:1px solid black;
        color:black;
        font-size:90%;
        text-align:right;
        }

    </style>
    '''

#end of html
def print_end_html():
    print
    print '''</body>'''
    print 
    print '''</html>'''

#prints row of a table
def print_row(text_list, align='left', colspan=1, bgcolor=''):
    print '''<tr align="%s" bgcolor="%s">''' % (align, bgcolor)
    for text in text_list:
        if type(text) == unicode:
            text = text.encode('utf8')
        print '''<td colspan=%d>%s</td>''' % (colspan, text)
    print '''</tr>'''


#configuration rendering page
def render_configure_page(kwd='', port=''):
    print '''<div>'''
    print '''<form name="render_configuration">'''
    print '''<fieldset class="field_class"><legend>Configuration Information</legend>'''
    print '''<table align=center>'''
    print_row(['Keywords Extraction File', '<input name="kwd_file" type="text" size=40 value="%s"/>' % kwd])
    print_row(['Port Number', '<input name="port_no" type="text" size=40 value="%s"/>' % port])
    print_row(['', '<input name="submit_type" value="Configure Folding" type="submit"/>'])
    print '''</table>'''
    print '''</fieldset>'''
    print '''</form>'''
    print '''</div>'''

#folding page rendering
def render_folding_page(fold_parameters_list, kwd_file, port):
    print '''<div>'''
    print '''<form name="render_folding">'''
    print '''<fieldset class="field_class"><legend>Folding Information</legend>'''
    print '''<table align=center>'''
    for title, desc, value in fold_parameters_list:
        print_row(['%s :' % title, '<input name="%s" type="text" size=40 value="%s"/>' % (title, value)])
    print_row(['', '<input name="submit_type" value="fold" type="submit"/><input name="submit_type" value="Reconfigure" type="submit"/>'])
    print_row(['<input name="kwd_file" value="%s" type="hidden"/>' % kwd_file])
    print_row(['<input name="port_no" value="%s" type="hidden"/>' % port])
    print '''</table>'''
    print '''</fieldset>'''
    print '''</form>'''
    print '''</div>'''

#print alert text in a table
def alert_text(text):
    print '''<table bgcolor="red" style="position:relative" align=center width=70%>'''
    print_row(['%s' % text], align='center')
    print '''</table>'''

#get the folding parameters from the keyword extraction file
def get_folding_parameters(kwd_file, form):
    fold_parameters_list = []
    try:
        kwd = open(kwd_file, 'r')
        for line in kwd:
            if not line.startswith("FIELDS:="):
                continue
            (name, field_str) = line.split(':=', 1)
            name = name.strip()
            field_str = field_str.strip()
            field_list = field_str.split(',')
            for field in field_list:
                value = form.getvalue(field, '')
                value = value.strip()
                fold_parameters_list.append((field, field, value))
    except:
        render_configure_page(kwd=kwd_file,port=form.getvalue('port_no', ''))
        alert_text("Please enter a valid file..")
    return fold_parameters_list

#preprocess page for folding and fold results
def preprocess_page(form):
    #get the kwd_file, port_no
    kwd_file = form.getvalue('kwd_file', '')
    port_no = form.getvalue('port_no', '')
    return kwd_file, port_no

#apply gmrf and get results
def apply_gmrf_and_get_results(folding_server, fold_parameters_list):
    for name, desc, value in fold_parameters_list:
        folding_server.addFieldToInputRecord(desc, value)
    kwd_tuples = folding_server.applyGMRF()
    fold_tuples = folding_server.getFoldedConcepts()
    genres_string = folding_server.getUgenres()
    return kwd_tuples, fold_tuples, genres_string

#render fold_tuples
def render_folds(fold_tuples):
    print '''<table BORDER=0 CELLSPACING=1 CELLPADDING=1 align=center>'''
    print_row(['Gid', 'Title', 'Direct', 'Cf', 'Score', 'AUX', 'Good AUX', 'Strong AUX', 'Tags(Hex)', 'Multi', 'Type', 'Keyword', 'Weight', 'Location'], bgcolor='dodgerblue')
    color_flag = True
    for gid, (title, direct, type, ir_kwd, search_wgt, field, cf_flag, tot_score, num_aux, direct_aux, good_aux, pri_kw_tags, multi_word, weighted_keywordss) in fold_tuples.iteritems():
        if color_flag:
            bg_color = "#CCFCFF"
            color_flag = False
        else: 
            bg_color = "#99CCFF"
            color_flag = True
        print_row([gid, title, direct, cf_flag, tot_score, num_aux, good_aux, direct_aux, pri_kw_tags, multi_word, type, ir_kwd, search_wgt, field], bgcolor=bg_color)
    print '''</table>'''

#render genres
def render_genres(genres):
    print '<table BORDER=0 CELLSPACING=1 CELLPADDING=1 align=center>'
    print '<b><tr bgcolor=lightblue><td>Genres</td><td>%s</td>'%genres
    print '</table>'

#render kwd_tuples
def render_keywords(kwd_tuples):
    print '''<table BORDER=0 CELLSPACING=1 CELLPADDING=1 align=center>'''
    print_row(['Keyword', 'Weight', 'Field-Index', 'Keyword Location Field', 'Location', 'value-index'], bgcolor='dodgerblue')
    color_flag = True
    for kw, (weight, field_index, kw_loc_field, location, value_index, meta_string) in kwd_tuples.iteritems():
        if color_flag:
            bg_color = "#CCFCFF"
            color_flag = False
        else: 
            bg_color = "#99CCFF"
            color_flag = True
        print_row([kw, weight, field_index, kw_loc_field, location, value_index], bgcolor=bg_color)
    print '''</table>'''

#render debug string
def render_debug_string(folding_server, fold_tuples):
    debug_lines = ["Debug Strings:\n -------------\n"]
    for gid in fold_tuples:
        debug_str = folding_server.getFoldDebugString(gid)
        debug_lines.append(gid+':\n'+debug_str+'\n\n')
    if len(debug_lines) == 1:
        debug_lines.append("Debug strings not found\n\n")
    debug_string = string.join(debug_lines, ' ')
    print '<table BORDER=0 CELLSPACING=1 CELLPADDING=1 align=center>'
    print_row(['<textarea name="Debug" cols=106 rows=10> %s</textarea>' % debug_string], bgcolor="#CCFCFF", align="center")
    print '</table>'

def render_fold_results(fold_tuples, genres_string, kwd_tuples, folding_server):
    print '''<div>'''
    print '''<form>'''
    print '''<fieldset class="field_class"><legend>Folding results</legend>'''
    #render fold_tuples
    print '''<h5>Folded Gids:</h5>'''
    if fold_tuples:
        render_folds(fold_tuples)
    else:
        print '''<p align="center">Sorry!! No gids folded..</p>'''
    #render genre
    if genres_string:
        print '''<h5>Folded Genre:</h5>'''
        render_genre(genres_string)
    #render kw tuples 
    print '''<h5>Folded keywords:</h5>'''
    if kwd_tuples:
        render_keywords(kwd_tuples)
    else:
        print '''<p align="center">No keywords found..</p>'''
    #render debug string
    print '''<h5>Debug Info:</h5>'''
    render_debug_string(folding_server, fold_tuples)
    print '''</fieldset>'''
    print '''<form>'''
    print '''</div>'''



if __name__ == '__main__' :
    print_success_header()
    print_start_html()
    logger = initialize_logger("/tmp/generic_folding_client.log")
    #get cgi storage fields
    form = cgi.FieldStorage()
    #get submit type
    submit_type = form.getvalue('submit_type')
    #preprocess form
    kwd_file, port_no = preprocess_page(form)
    #if the form is opened the first time
    if submit_type is None or submit_type == "Reconfigure":
        render_configure_page(kwd=kwd_file, port=port_no)
    #as the configure folding button is pressed
    if submit_type == 'Configure Folding':
        #if the inputs are not given
        if not kwd_file or not port_no:
            alert_text('Please specify the above input fields..')
            render_configure_page(kwd=kwd_file, port=port_no)
        else:            
            #get the keyword types from the kwd file and render it
            fold_parameters_list = get_folding_parameters(kwd_file, form)
            if fold_parameters_list:
                render_folding_page(fold_parameters_list, kwd_file, port_no)
    
    #as the fold button is pressed
    if submit_type == 'fold':
        fold_parameters_list = get_folding_parameters(kwd_file, form)
        try:
            #invoke folding server
            folding_server = xmlrpclib.ServerProxy("http://localhost:%s" % port_no)            
        except:
            render_configure_page(kwd=kwd_file, port=port_no)
            alert_text("Please configure with correct details...")
        #apply gmrf and get results
        kwd_tuples, fold_tuples, genres_string = apply_gmrf_and_get_results(folding_server, fold_parameters_list)
        render_folding_page(fold_parameters_list, kwd_file, port_no)
        render_fold_results(fold_tuples, genres_string, kwd_tuples, folding_server)
    print_end_html()


