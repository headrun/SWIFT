#!/usr/bin/python
"""
cgi script to connect to mergevisulizer.
"""
import xmlrpclib
import os, sys
import cgi
import traceback, socket

def getForm(script_name, type_list):
    """
    return basic html form, with a search box
    """
    html_str = ""
    html_str += """
    <form name="title_search" action="%s" method="get">
    Title:
    <input type="text" name="title" />
    <input type="hidden" name="cmd" value="title_search">
    <br>
    """%(script_name)
    for type_name in type_list:
        html_str += '<input type="checkbox" name="types" value="%s"> %s'%(type_name, type_name)
    html_str += """
    <br>
    <input type="submit" value="Search" />
    </form> 
    """
    html_str += """
    <form name="aux_search" action="%s" method="get">
    Aux Guid:
    <input type="text" name="guid" />
    <input type="hidden" name="cmd" value="aux_search">
    <input type="submit" value="Search" />
    </form> 
    """%(script_name)
    html_str += """
    <form name="seed_search" action="%s" method="get">
    Seed Guid:
    <input type="text" name="guid" />
    <input type="hidden" name="cmd" value="seed_search">
    <input type="submit" value="Search" />
    </form> 
    """%(script_name)
    html_str += "<hr>"
    return html_str

def getRandomLinks(source_list, type_list, base_path):
    html_str = ''
    html_str += "<b>Random: </b>"
    html_str += """
    <form name="random" action="%s" method="get">
    <input type="hidden" name="cmd" value="random">
    """%(script_name)
    html_str += 'Source: <SELECT NAME="source">\n'
    for source in source_list:
        html_str += '<OPTION VALUE="%s"> %s\n'%(source, source)
    html_str += '</SELECT>\n'
    html_str += 'Type: <SELECT NAME="type">\n'
    for vt in type_list:
        html_str += '<OPTION VALUE="%s"> %s\n'%(vt, vt)
    html_str += '</SELECT>'
    html_str += ' <input type="submit" value="Get Random" />'
    html_str += "</form>"
    html_str += "<hr>"
    return html_str

if __name__ == '__main__':
    script_name = "VisualizeMerge.py"
    req = os.environ.get("REQUEST_METHOD", '')
    print "Content-Type: text/html\r\n\r\n"
    print "<html><head><title>%s</title></head>"%script_name
    print "<body>"
    print "<center>"
    if req.lower() == 'get':
        query_string = os.environ.get("QUERY_STRING", '')
    elif req.lower() == 'post':
        query_string = sys.stdin.read()
    else:
        print "I listen only GET or POST. You are talking in %s"%req
        query_string = ""
        #sys.exit()
    args = cgi.parse_qs(query_string)
    try:
        ##Connect to the server
        sp = xmlrpclib.ServerProxy("http://127.0.0.1:8674")
        print '<a href="%s"> Home </a> <br>'%script_name
        type_list = sp.getTypeList()
        print getForm(script_name, type_list)
        source_list = sp.getSourceList()
        print getRandomLinks(source_list, type_list, script_name)
        output = sp.getInfo(args, script_name)
        print output.encode('utf-8', 'replace')
    except socket.error:
        print 'Server unavailable. Plz try later or contact <a href="mailto:monu@veveo.net">Monu</a>'
    except:
        print "Exception Occured :("
        print '<br>'
        print ''.join(traceback.format_exception(*(sys.exc_info())))
        print '<br>'
        print 'Contact <a href="mailto:monu@veveo.net">Monu</a>'
    print "</center>"
    print "</body>"
    print "</html>"
