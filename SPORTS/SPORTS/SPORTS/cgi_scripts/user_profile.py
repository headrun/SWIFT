#!/usr/bin/python

import os, sys

sys.path.append('..')

from vtv_utils import get_compact_traceback
from vtv_xml import PROFILE_TAG, VALUE_TAG, ERROR_TAG, \
                    HSSDB_CHAN_LIST_TAG, HSSDB_PRIVATEID_TAG, HSSDB_PUBLICID_TAG, HSSDB_USER_TAG, HSSDB_ZIP_TAG, \
                    get_data_from_tag, get_last_node_from_tag_list

from httpd_ui import get_xml_request_from_tags, my_main


def encode_xml_request(arg_list):
    private_id, public_id=('', '')
    if len(arg_list) > 0:
        private_id = arg_list[0]
    if len(arg_list) > 1:
        public_id  = arg_list[1]
    
    tag_list = [(HSSDB_PUBLICID_TAG, {VALUE_TAG : public_id}), (HSSDB_PRIVATEID_TAG, {VALUE_TAG : private_id}), (HSSDB_USER_TAG, {})]

    return get_xml_request_from_tags(tag_list, PROFILE_TAG)


def decode_xml_response(client, xml_doc):
    child_node, child_dict = get_last_node_from_tag_list([PROFILE_TAG], xml_doc)
    if not child_node or ERROR_TAG in child_dict:
        child_node, child_dict = get_last_node_from_tag_list([PROFILE_TAG, ERROR_TAG], xml_doc)
        error_str = ""
        if child_node:
            error_str = get_data_from_tag(child_node) 
        print "User not found %s" % error_str
        return

    print "<h3>User Profile</h3>"
    print "<table border=\"2\">"
    print "<tr class=\"header\"><th>Variable</th><th>Value</th></tr>"

    print "<tr class=\"row_type_1\"><th bgcolor=lightgray>%s</th>" % ("private_id")
    print "<td width=50 bgcolor=lightblue>%s</td></tr>" % (get_data_from_tag(child_dict[HSSDB_PRIVATEID_TAG]))

    print "<tr class=\"row_type_1\"><th bgcolor=lightgray>%s</th>" % ("public_id")
    print "<td width=50 bgcolor=lightblue>%s</td></tr>" % (get_data_from_tag(child_dict[HSSDB_PUBLICID_TAG]))

    print "<tr class=\"row_type_1\"><th bgcolor=lightgray>%s</th>" % ("zipcode")
    print "<td width=50 bgcolor=lightblue>%s</td></tr>" % (get_data_from_tag(child_dict[HSSDB_ZIP_TAG]))
    print "</table>"

    if HSSDB_CHAN_LIST_TAG in child_dict:
        print "<h3>Channel List</h3>"
        print "<table border=\"2\">"
        print "<tr class=\"header\"><th>Channel Id</th><th>VC Number</th><th>CallSign</th>"
        print "<th>Description</th><th>HD Flag</th></tr>"

        chan_str = "%s" % get_data_from_tag(child_dict[HSSDB_CHAN_LIST_TAG])
        for channel in chan_str.split(':'):
            list = channel.split(',')
            chan_id, vc_num, callsign = list[:3]
            hd_flag = list[-1]
            description = "-"
            if len(list) == 5:
                desc = list[3]
            print "<tr class=\"row_type_1\"><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" %\
                      (chan_id, vc_num, callsign, description, hd_flag)
        print "</table>"


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
