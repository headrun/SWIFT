#!/usr/bin/python

import os, sys

sys.path.append('..')

from vtv_utils import execute_shell_cmd, get_compact_traceback



def main(arg_list):
    file_name = arg_list[0]
    
    color_list = ["#ff0000", "#0000ff"]
    
    process_name = os.path.basename(file_name)
    process_name = process_name.split("_")[0]
    
    if os.access(file_name, os.F_OK):
        print "<h2>The Contents of Crash File %s</h2>" % file_name
        crash_lines = file(file_name).readlines()
        color = 0
        print "<h3>%s</h3>" % crash_lines[0]
    
        for line in crash_lines[1:]:
            color = (color + 1) % len(color_list)
            src_line = line
            if line.find(process_name) != -1:
                start_pos = line.find("[")
                end_pos = line.find("]")
                if start_pos != -1 and end_pos != -1:
                    address = line[start_pos + 1:end_pos]
                    cmd = "addr2line -e ../%s %s" % (process_name, address)
                    status, text = execute_shell_cmd(cmd, None)
                    if status == None:
                        src_line = text
            index1 = line.find('(')
            index2 = line.find('+')
            func_name = "-"
            if index1 != -1 and index2 != -1:
                func_name = line[index1+1:index2]
    
            #print '<font size=3>%s (%s)</font>' % (src_line, func_name)
            print '<font size=3 color="%s">%s (%s)</font>' % (color_list[color], src_line, func_name)
            print '<br>'
    
        print "<br>Crash file printed successfully"
    else:
        print "<h2>The Crash file doesn't exist</h2>"
        

if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
    except Exception, e:
        print "exception in main: %s" % get_compact_traceback(e)
        sys.exit(1)
