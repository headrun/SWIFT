# -*- coding: utf-8 -*-
import sys
import os
import logging
import re

from vtv_task import VtvTask, vtv_task_main
from vtv_utils import execute_shell_cmd

FILE_SEPARATOR     = '|'
FIELD_CMD = """ egrep -e '%%s' %%s | awk '{ if (substr($0,1,3) == "Gi:") { if (NR > 1) { print "" } ; printf "%s",$0 } else { printf "%s",$0 } } ' > %%s """
SEPARATOR = '%%%%s%s' % FILE_SEPARATOR
FIELD_CMD = FIELD_CMD % (SEPARATOR, SEPARATOR)

src_file_name = '/var/tmp/madhav_test/ftvseries.data'
dst_file_name = '/var/tmp/madhav_test/mi_tvseries.data'
field_list = ['Gi', 'Ti']
new_list_file = os.path.join('/var/tmp/madhav_test', 'new_list_file.list')
new_file = open(new_list_file, 'w')

data_file = '/var/tmp/madhav_test/fepisode.data'

class Merge(VtvTask):


    def generate_gid_field_info(self,):
        '''Gi and Ti from the main data file'''
        field_pattern = '|'.join([ '^%s:' % f for f in field_list])
        cmd = FIELD_CMD % (field_pattern, src_file_name, dst_file_name)
        self.run_cmd(cmd) 
        '''data file from which taking only the particular tags'''
        fields = ['Mi']
        for field in fields:
            cmd = 'egrep -e "%s: " %s ' %(field, data_file)
            status, output = execute_shell_cmd(cmd, self.logger)
            for val in output:
                new_file.write(val)       
        new_file.close()
        clv = open('/var/tmp/madhav_test/new_list_file.list').readlines()
        clvs = set(clv)
        cl_dict = {}
        for data_value in clvs:
            gid_titles = []
            gids = data_value.split(': ')[-1].strip('\n').split('<>')
            tag = data_value.split(': ')[0]
            for gid in gids:
                gid_values = 'egrep -w "%s" %s' %(gid, dst_file_name)
                status, output = execute_shell_cmd(gid_values, self.logger)
                try:
                    output_values = output.split('Ti: ')[1].split('<>')
                except:
                    output_values = gid + '{eng}' 
                for output_value in output_values:
                    if not 'eng' in output_value:
                        continue
                    output_value = output_value.split('{')[0]
                    gid_title = output_value + '{' + gid + '}'
                    gid_titles.append(gid_title)
                gid_title_values = '<>'.join(gid_titles)
            tag = 'Ti'
            data = tag + ': ' + gid_title_values + '\n'
            data_value = data_value.strip('\n')
            cl_dict.update({data_value : data})   #old and new value in a dict
        st_fields = ['Mi: ']
        '''writing a new file'''
        f2 = open('new_episode_wiki.data1', 'w')
        with open('fepisode.data') as sf:

            for recr in sf:
                for field in st_fields:
                    if field in recr:
                        old_val = recr
                        recr = cl_dict.get(recr.strip())
                        new_val = recr
                        recr = old_val + new_val
                f2.write(recr)
        f2.close()


    def run_main(self):
        self.generate_gid_field_info()


if __name__ == '__main__':
    vtv_task_main(Merge)


    



