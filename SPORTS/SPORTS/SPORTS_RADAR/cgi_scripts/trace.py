#!/usr/bin/python

import os, sys

sys.path.append('..')

from vtv_utils import get_compact_traceback
from vtv_xml import TRACE_TAG, TRACE_TYPE_TAG, TRACE_USER_TAG, TRACE_FILE
from globals import wrap_with_tag
from httpd_ui import my_main


def encode_xml_request(arg_list):
    xml_request = ""

    trace_type = arg_list[0]
    if trace_type == "display":
        trace_file = os.path.join('..', TRACE_FILE)
        if not os.access(trace_file, os.F_OK):
            print "<h3><u>Trace file <b>%s</b> not found</u></h3>" % os.path.abspath(trace_file)
        else:
            print "<u>CONTENTS OF TRACE FILE: %s</u>" % trace_file
            print "<textarea name='contents' rows=30 cols=90>%s</textarea>" % open(trace_file).read()
        sys.exit(0)

    if trace_type == "remove":
        trace_file = os.path.join('..', TRACE_FILE)
        if os.access(trace_file, os.F_OK):
            os.remove(trace_file)
        print "Removal of tracefile: <b>%s</b> successful" % trace_file
        sys.exit(0)

    if trace_type == "enable":
        if len(arg_list) < 2:
            print "<h3>ERROR: Enter User name </h3>"
            sys.exit(0)

        user = arg_list[1]
        xml_request = wrap_with_tag(TRACE_USER_TAG, user, {})

    xml_request = wrap_with_tag(TRACE_TAG, xml_request, { TRACE_TYPE_TAG: trace_type })

    return xml_request


def decode_xml_response(client, xml_doc):
    print "Trace Response: %s" % xml_doc.toxml()


def main(arg_list):
    my_main(encode_xml_request, decode_xml_response, arg_list)


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
    except Exception, e:
        print "exception in main: %s" % get_compact_traceback(e)
        sys.exit(1)
