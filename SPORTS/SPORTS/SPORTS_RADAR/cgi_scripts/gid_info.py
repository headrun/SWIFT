#!/usr/bin/env python

################################################################################
#$Id: gid_info.py,v 1.1 2016/03/23 12:50:45 headrun Exp $
#Copyright(c) 2005 Veveo.tv
################################################################################

import sys
import os
import re
import time
import cgi
import codecs
import MySQLdb
import jinja2
import urllib2
import json

sys.path.append('..')

from vtv_utils import get_sysd_ip_in_monitor, execute_shell_cmd, remove_file
from vtv_task import VtvTask, vtv_task_main


DEFAULT_SERVER_IP = '10.4.18.103'

FILE_PREFIX = 'new_info'
FILE_PREFIX = 'gid_info'

IMAGE_CACHE_STR = '''http://imc.veveo.net/imc?g=%s&f=jpeg&w=300&h=300&ar=1&prefix=kg&u=%s'''

TYPE_2_DATGEN_FILE = {
    'movie'                 : 'movie.data.gen.merge',
    'tvvideo'               : 'tvvideo.data.gen.merge',
    'tvseries'              : 'tvseries.data.gen.merge',
    'episode'               : 'episode.data.gen.merge',
    'PersonalityFolding'    : 'fold.data.gen.merge',
    'musicArtist'           : 'fold.data.gen.merge',
    'Tournament'            : 'fold.data.gen.merge',
    'TeamFolding'           : 'fold.data.gen.merge',
    'PhraseFolding'         : 'fold.data.gen.merge',
    'ConceptFolding'        : 'fold.data.gen.merge',
    'award'                 : 'fold.data.gen.merge',
    'track'                 : 'fold.data.gen.merge',
    'channel'               : 'channel.data.gen.merge',
    'role'                  : 'role.data.gen.merge',
    }

RECORD_SEPARATOR = '#<>#'
FIELD_SEPARATOR  = '<>'

GID_REGEX = re.compile('(?P<prefix>[A-Z]+)(?P<id>[0-9]+$)')

DATAGEN_LIST = [ 'CONCEPTS', 'SEED', 'PHRASE', 'PHRASE_INCR', 'WIKI', 'TVAVAIL', 'INSTANT', 'INSTANT1', 'INSTANT2' ]
 
HTML_TABLE_STR       = '<table class="table table-bordered table-condensed table-hover">%s</table>'
HTML_TR_TH_STR       = '<tr><th>%s</th></tr>'
HTML_TR_TD_STR       = '<tr><td>%s</td></tr>'
HTML_TR_TD_SPAN2_STR = '<tr><td class="span2">%s</td></tr>'
HTML_TR_TD_SPAN3_STR = '<tr><td class="span3">%s</td></tr>'
HTML_TR_TD_SPAN4_STR = '<tr><td class="span4">%s</td></tr>'


class GidInfo(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)

        my_name = 'GID_INFO'
        self.OUT_DIR = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, my_name)

        self.REPORTS_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR, my_name)
        self.options.report_file_name = os.path.join(self.REPORTS_DIR, '%s.html' % self.name_prefix)

        self.server_ip = DEFAULT_SERVER_IP
        self.gid = ''
        self.src_gid = ''
        self.need_data_list = DATAGEN_LIST

        if self.options.not_cgi:
            server_ip = get_sysd_ip_in_monitor()
        else:
            server_ip = os.environ.get('SERVER_NAME', '')

        self.url_path = 'http://%s/cgi-bin/%s.py' % (server_ip, FILE_PREFIX)

        self.type = ''
        self.title = ''
        self.img_url = ''
        self.rich_data = ''

        self.field_value_dict = {}
        for name in DATAGEN_LIST:
             self.field_value_dict[name] = ([], {})

        self.gid_header = {}
        self.gid_info = []

    def set_options(self):
        self.parser.add_option('--not-cgi', '-p', default=False, metavar='BOOL', action="store_true", help='use production proxy')
        self.parser.add_option('-g', '--gid', default='', help='the gid')
        self.parser.add_option('', '--data', default='', help='type of the gid')
        self.parser.add_option('', '--query', default='', help='type of the gid')
        self.parser.add_option('', '--server', default='', help='search server ip')

    def get_normalized_list(self, lines):
        field_list = [line.strip().split(': ', 1) for line in lines ]
        value_list = [line for line in field_list if len(line) > 1 ]
        value_list = [ (key.strip(), value.strip().decode('utf8')) for key, value in value_list if value.strip() ]
        return value_list

    def get_cmd_text(self, name, cmd):
        status, text = execute_shell_cmd(cmd, None)
        if status != None:
            #text = 'Gi: Did not find Gid: %s status: %s cmd: %s\n' % (self.gid, status, urllib2.quote(cmd))
            text = 'Gi: Did not find Gid: %s\n' % (self.gid)

        return text

    def get_single_line_values(self, text):
        lines = text.strip().split(RECORD_SEPARATOR)
        return self.get_normalized_list(lines)

    def get_multi_line_values(self, text):
        lines = text.strip().split('Gi:')
        lines = ('Gi: ' + lines[1]).split('\n')
        return self.get_normalized_list(lines)

    def get_merge_data(self):
        merge_file_name = os.path.join(self.datagen_dirs.guid_merge_data_dir, 'vtv_guid_merge.list')

        cmd = "grep -m 1 '^%s%s' %s" % (self.gid, FIELD_SEPARATOR, merge_file_name)
        text = self.get_cmd_text('Merge', cmd)

        gid_list = text.strip().split(FIELD_SEPARATOR)
        if len(gid_list) > 1:
            self.src_gid = gid_list[-1]

    def format_value_and_gid(self, value, separator = '{'):
        kw_list = value.split(FIELD_SEPARATOR)

        new_row_list = []
        for kw in kw_list:
            field_list = kw.strip('}').split(separator)
            if len(field_list) > 0:
                f_gid = field_list[-1].strip()
                gid_parts = GID_REGEX.findall(f_gid)
                if gid_parts and gid_parts[0] and gid_parts[0][0] in [ 'WIKI', 'VM', 'VC', 'VS', 'G', 'SG' ]:
                    f_link = '<a href="%s?gid=%s">%s</a>' % (self.url_path, f_gid, f_gid)
                    field_list[-1] = f_link
            html_td_str = HTML_TR_TD_SPAN4_STR
            new_row_list.append(html_td_str % '</td><td>'.join(field_list))

        html_str = HTML_TABLE_STR % ''.join(new_row_list)

        return html_str
           
    def format_value_and_gid_from_list(self, item_dict, field_list, separator = '{'):
        for field in field_list:
            value = item_dict.get(field, '')
            if value:
                item_dict[field] = self.format_value_and_gid(value, separator)

    def format_value_and_weight(self, value, separator = '{', sort_pos = 1, header = []):
        kw_list = value.split(FIELD_SEPARATOR)

        row_list = []
        for kw in kw_list:
            field_list = kw.strip('}').split(separator)
            try:
                row_list.append((int(field_list[sort_pos]), field_list))
            except (IndexError, ValueError):
                # self.logger.error('Error in %s %s' % (kw_list, field_list))
                row_list.append((0, field_list))
        row_list.sort(reverse = True)

        new_row_list = []
        if header:
            new_row_list.append(HTML_TR_TH_STR % '</th><th>'.join(header))
        for weight, field_list in row_list:
            html_td_str = HTML_TR_TD_SPAN4_STR
            new_row_list.append(html_td_str % '</td><td>'.join(field_list))

        html_str = HTML_TABLE_STR % ''.join(new_row_list)

        return html_str
           
    def format_value_and_weight_from_list(self, item_dict, field_list, separator = '{', sort_pos = 1, header = []):
        for field in field_list:
            value = item_dict.get(field, '')
            if value:
                item_dict[field] = self.format_value_and_weight(value, separator, sort_pos, header)

    def format_rules(self, item_dict, sort_pos = 1, header = [ 'Phrase', 'Wt', 'Flag' ]):
        rules = item_dict.get('Rl', '')
        if not rules:
            return

        FLAG_LIST = [ 'Aux/Secondary', 'Primary', 'Parent-Child relation', 'Strong Aux', 'Weak Primary', 'Sports Abbr', 'Q Aux', 'Release year', 'Content type', 'Original Release Date', '', 'TRy', 'Inhibitory keywords', 'Season number', 'Other release years, Ay', 'Other release date, Ad' ]

        rule_list = rules.split(RECORD_SEPARATOR)

        row_list = []
        for rule in rule_list:
            field_list = rule.split(FIELD_SEPARATOR)
            flag = field_list[-1]
            if flag.isdigit():
                f = int(flag)
                if f < len(FLAG_LIST):
                    field_list[-1] = '%s: %s' % (flag, FLAG_LIST[f]) 

            row_list.append((int(field_list[sort_pos]), field_list))
        row_list.sort(reverse = True)

        new_row_list = []
        if header:
            new_row_list.append(HTML_TR_TH_STR % '</th><th>'.join(header))
        for weight, field_list in row_list:
            new_row_list.append(HTML_TR_TD_SPAN4_STR % '</td><td>'.join(field_list))

        html_str = HTML_TABLE_STR % ''.join(new_row_list)

        item_dict['Rl'] = html_str
           
    def get_gid_type(self):
        if not self.type:
            gid_prefix = re.sub('[0-9]+', '', self.gid)
            if gid_prefix in [ 'NCH', 'DCH' ]:
                self.type = 'channel'

        if not self.type:
            src_gid_prefix = re.sub('[0-9]+', '', self.src_gid)
            if src_gid_prefix == 'VM':
                self.type = 'movie'
            elif src_gid_prefix == 'VC':
                self.type = 'PersonalityFolding'

    def set_field_value_dict(self, name, item_list):
        item_dict = dict(item_list)
        self.field_value_dict[name] = (item_list, item_dict)

        if not self.type:
            self.type = item_dict.get('Ty', '')

        if not self.type:
            self.type = item_dict.get('Vt', '')

        self.get_gid_type()

        if not self.title:
            self.title = item_dict.get('Ti', '')

        if not self.img_url:
            self.img_url = item_dict.get('Im', '')

        return item_dict

    def get_concepts_data(self, name):
        concepts_file_name = os.path.join(self.datagen_dirs.concepts_data_dir, 'concepts.vdb')

        cmd = "grep -m 1 -A9 '^Gi: %s$' %s" % (self.gid, concepts_file_name)
        text = self.get_cmd_text(name, cmd)

        item_list = self.get_multi_line_values(text)
        item_dict = self.set_field_value_dict(name, item_list)

        self.format_rules(item_dict)

    def get_seed_data(self, name):
        self.get_gid_type()

        file_name = TYPE_2_DATGEN_FILE.get(self.type, 'fold.data.gen.merge')
        seed_file_name = os.path.join(self.datagen_dirs.vtv_seed_dir, file_name)

        cmd = "grep -m 1 -A99 '^Gi: %s$' %s" % (self.gid, seed_file_name)
        text = self.get_cmd_text(name, cmd)

        item_list = self.get_multi_line_values(text)
        item_dict = self.set_field_value_dict(name, item_list)

        self.rich_data = item_dict.get('Rd', '')

        field_list = [ 'Di', 'Pr', 'Wr', 'Co', 'Ca', 'Uc', 'Ic', 'Gg', 'Ge', 'Rl', 'Aw', 'Ig', 'Ng', 'Gk', 'Rd', 'Va', 'Cl', 'Ll', 'Tg', 'Sp' ]
        self.format_value_and_gid_from_list(item_dict, field_list)

        field_list = [ 'Ak', 'Ik', 'Ke', 'Oa', 'Ro' ]
        self.format_value_and_weight_from_list(item_dict, field_list)

        field_list = [ 'Tl', 'Rd' ]
        self.format_value_and_weight_from_list(item_dict, field_list, '#')

    def get_phrase_data_file(self, name, dir_name):
        item_list = []
        for file_name in [ 'gid_meta.file', 'gid_phrase.file' ]:
            phrase_file_name = os.path.join(dir_name, file_name)

            if not os.access(phrase_file_name, os.F_OK):
                continue

            cmd = "grep -m 1 '^Gi: %s#' %s" % (self.gid, phrase_file_name)
            text = self.get_cmd_text(name, cmd)

            partial_item_list = self.get_single_line_values(text)
            item_list.extend(partial_item_list)

        item_dict = self.set_field_value_dict(name, item_list)

        field_list = [ 'Ge', 'Tg', 'Sq', 'Mv' ]
        self.format_value_and_gid_from_list(item_dict, field_list)

        field_list = [ 'Ms' ]
        self.format_value_and_weight_from_list(item_dict, field_list, '#', 3, [ 'Phrase', 'Field', 'Wt', 'PkWt', 'Rr', 'Df' ])

    def get_phrase_data(self, name):
        self.get_phrase_data_file(name, self.datagen_dirs.kramer_data_dir)

    def get_phrase_incr_data(self, name):
        self.get_phrase_data_file(name, self.datagen_dirs.kramer_incr_data_dir)

    def get_wiki_data(self, name):
        item_list = []
        for op, file_name in [ ('single', 'wiki_tags.txt'), ('single', 'DATA_WIKIPEDIA_POPULARITY_FILE_inlink'), ('single', 'weighted_aka.txt'), ('multiple', 'mrf.txt') ]:
            wiki_file_name = os.path.join(self.datagen_dirs.wiki_archival_data_dir, file_name)
            if op == 'single':
                cmd = "grep -m 1  '^Gi: %s#' %s" % (self.gid, wiki_file_name)
            else:
                cmd = "grep -m 1 -A99 '^Gi: %s$' %s" % (self.gid, wiki_file_name)
            text = self.get_cmd_text(name, cmd)

            if op == 'single':
                partial_item_list = self.get_single_line_values(text)
            else:
                partial_item_list = self.get_multi_line_values(text)

            for k, v in partial_item_list:
                for i, (wk, wv) in enumerate(item_list):
                    if wk == k:
                        break
                else: 
                    item_list.append((k, v))

        item_dict = self.set_field_value_dict(name, item_list)

        self.format_rules(item_dict)

        field_list = [ 'Sy' ]
        self.format_value_and_gid_from_list(item_dict, field_list)

        field_list = [ 'Ug' ]
        self.format_value_and_gid_from_list(item_dict, field_list, ';')

        field_list = [ 'Va', 'Ak', 'Ik' ]
        self.format_value_and_weight_from_list(item_dict, field_list)

    def get_tvpublish_data(self, name):
        DATA_DIR_DICT = { 'TVAVAIL' : self.datagen_dirs.vtv_avail_data_dir, 'INSTANT' : self.datagen_dirs.vtv_instant_data_dir,
                      'INSTANT1' : self.datagen_dirs.vtv_instant1_data_dir, 'INSTANT2' : self.datagen_dirs.vtv_instant2_data_dir }
       
        tvpublish_file_name = os.path.join(DATA_DIR_DICT.get(name), 'DATA_MC_FILE')

        cmd = "grep -m 1 '^Gi: %s#' %s" % (self.gid, tvpublish_file_name)
        text = self.get_cmd_text(name, cmd)

        item_list = self.get_single_line_values(text)
        item_dict = self.set_field_value_dict(name, item_list)

        field_list = [ 'Di', 'Pr', 'Wr', 'Co', 'Ca', 'Uc', 'Ic', 'Gg', 'Ge', 'Rl', 'Aw', 'Ig', 'Ng', 'Gk', 'Tl', 'Va', 'Cl', 'Ll', 'Is', 'Tg', 'Sp', 'Re' ]
        self.format_value_and_gid_from_list(item_dict, field_list)

        field_list = [ 'Ak', 'Ik', 'Ke', 'Oa', 'Ro' ]
        self.format_value_and_weight_from_list(item_dict, field_list)

        field_list = [ 'Rd', 'Sk', 'Fd' ]
        self.format_value_and_weight_from_list(item_dict, field_list, '#')

    def get_gid_info(self):
        START_LIST = [ 'Gi', 'Pp', 'Pi', 'Ti', 'Ep', 'Va', 'Vt', 'Ty', 'Xt', 'Sy', 'Do', 'Sp', 'Sq', 'Mv', 'Xr', 'Ra', 'Re', 'Fy', 'Tg', 'Bp', 'Wp', 'Bt', 'Rr', 'Sx', 'Bd', 'Ry', 'Od', 'Du', 'Ll', 'Cl', 'Gg', 'Ge', 'Ug', 'Ng', 'Gk' ] 

        all_keys_list = []
        junk = [ all_keys_list.extend(item_dict.keys()) for name, (item_list, item_dict) in self.field_value_dict.items() ]

        need_list = set(START_LIST).intersection(set(all_keys_list))

        key_list = [ k for k in START_LIST if k in need_list ]
        for name, (item_list, item_dict) in self.field_value_dict.items():
            junk = [ key_list.append(k) for k, v in item_list if k not in key_list ]

        END_LIST = [ 'Di', 'Pr', 'Wr', 'Co', 'Ca', 'Uc', 'Ic', 'Ro', 'De', 'Dr', 'Sd', 'Tl', 'Sk', 'Rd', 'Fd', 'Ms', 'Ig', 'Ak', 'Oa', 'Ik', 'Ke', 'Rl', 'Th' ]
        for k in END_LIST:
            if k not in key_list:
                continue

            key_list.remove(k)
            key_list.append(k)

        field_dict = {}
        for name, (item_list, item_dict) in self.field_value_dict.items():
            for key, value in item_dict.items():
                value_list = field_dict.setdefault(key, [])
                for i, (n, v) in enumerate(value_list):
                    if v and v == value:
                        value_list[i] = ('%s<br/>%s' % (n, name), value)
                        break
                else:
                    value_list.append((name, value))
                        
        self.gid_info = []
        for key in key_list:
            value_list = field_dict.get(key, [])
            if value_list:
                new_key = '%s:' % key
                for name, value in value_list:
                    self.gid_info.append((new_key, name, value))
                    new_key = ''

        #print field_dict
        #print self.gid_info

    def get_gid_header(self):
        img_url = IMAGE_CACHE_STR % (self.gid, urllib2.quote(self.img_url))

        if self.src_gid:
            src_link = '<h4>Veveo: %s<h4>' % (self.src_gid)
        else:
            src_link = ''

        if 'WIKI' in self.gid:
            wiki_link = '<h4>WIKI: <a href="http://en.wikipedia.org/?curid=%s">%s</a><h4>' % (self.gid.replace('WIKI', ''), self.gid)
        else:
            wiki_link = ''

        imdb_link = ''

        value_list = []

        if self.rich_data:
            rich_data_dict = dict([ v.split('#') for v in self.rich_data.split(FIELD_SEPARATOR) ])

            imdb_title = rich_data_dict.get('imdb_title', '')
            imdb_name = rich_data_dict.get('imdb_name', '')
            if imdb_title:
                imdb_link = '<h4>IMDB: <a href="http://www.imdb.com/title/%s">%s</a><h4>' % (imdb_title, imdb_title)
            elif imdb_name:
                imdb_link = '<h4>IMDB: <a href="http://www.imdb.com/name/%s">%s</a><h4>' % (imdb_name, imdb_name)
            else:
                imdb_link = ''

            INFO_LIST = [ 'startype', 'domain' ]
            for info in INFO_LIST:
                info_str = rich_data_dict.get(info, '')
                if info_str:
                    value_list.append('<h4>%s<h4>' % (info_str))

        html_str = ''.join(value_list + [ src_link, wiki_link, imdb_link ])

        self.gid_header = { 'Gid' : self.gid, 'Server' : self.server_ip, 'Image' : img_url, 'Title' : self.title,
                            'Type' : self.type, 'Info' : html_str, 'Time' : '' }

        #print self.gid_header
       
    def get_gid_data(self):
        if self.gid:
            self.get_merge_data()

        FUNC_LIST = [ ( 'CONCEPTS',     self.get_concepts_data    ),
                      ( 'SEED',         self.get_seed_data        ),
                      ( 'PHRASE',       self.get_phrase_data      ),
                      ( 'PHRASE_INCR',  self.get_phrase_incr_data ),
                      ( 'WIKI',         self.get_wiki_data        ),
                      ( 'TVAVAIL',      self.get_tvpublish_data   ),
                      ( 'INSTANT',      self.get_tvpublish_data   ),
                      ( 'INSTANT1',     self.get_tvpublish_data   ),
                      ( 'INSTANT2',     self.get_tvpublish_data   )
                    ]

        if self.gid:
            for name, func in FUNC_LIST:
                if name in self.need_data_list: 
                    func(name)

        self.get_gid_header()

        if self.gid:
            self.get_gid_info()

    def get_url_response(self, url, t_out, return_header= False):
        response = None
        try:
            req = urllib2.Request(url)
            r = urllib2.urlopen(req, timeout=t_out)
            response = r.read()
            r.close()
        except:
           pass

        return response

    def get_query_data(self):
        if not self.query:
            query = 'FL=SynFilterMovies'
        else:
            query = 'w=%s' % self.query.replace(' ', '.')

        url = "http://%s/search?XPID=pkg00@NH53383.Comcast&Ver=3.0&ECT=5&RPR=20&RFTR=256&DS=255&XUA=tvwidget&%s" % (self.server_ip, query)

        response = self.get_url_response(url, t_out=10)
        query_dict = json.loads(response)

        cns_list = query_dict['VtvRsps']['RC']['CNs']
        cn_count = cns_list['n'];
        ci_list = cns_list['CI'];

        a_ref = '<a href="%s?gid=%%s">%%s</a>' % (self.url_path)
        i_ref = '<img src="http://imc.veveo.net/imc?g=%s&f=jpeg&w=42&h=42&ar=1&prefix=kg&u=%s">'

        self.gid_info = []
        for ci_dict in ci_list:
            if 'G' not in ci_dict:
                continue

            gid = ci_dict['G']
            type = ci_dict['TYP']
            title = ci_dict['T']
            image = ci_dict.get('CMI', '')
 
            self.gid_info.append([i_ref % (gid, urllib2.quote(image)), a_ref % (gid, gid), type, title])

    def run_main(self):
        if not self.options.not_cgi:
            print "Content-Type: text/html\n"

        if self.options.not_cgi:
            self.query = self.options.query
            self.gid = self.options.gid
            if self.options.server:
                self.server_ip = self.options.server
            if self.options.data:
                self.need_data_list = self.options.data.upper().split(',')
        else:
            form = cgi.FieldStorage()

            self.gid = ''
            self.query = ''

            if 'gid' in form:
                self.gid = form['gid'].value

            if 'server' in form:
                self.server_ip = form['server'].value

            self.need_data_list = [ x for x in DATAGEN_LIST if x.lower() in form ]
            if not self.need_data_list:
                self.need_data_list = DATAGEN_LIST
         
            if 'query' in form:
                self.query = form['query'].value

        jinja_file_name = '%s.jinja' % FILE_PREFIX

        self.gid = self.gid.strip()

        if self.gid:
            self.get_gid_data()
            self.gid_header['Cmd'] = 'info'
        else:
            self.get_query_data()
            self.gid_header['Cmd'] = 'query'

        end_time = time.time()
        self.gid_header['Time'] = '%.2f' % (end_time - self.start_time)

        self.gid_header['Url'] = self.url_path

        jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader(os.getcwd()))
        table_html = jinja_environment.get_template(jinja_file_name).render(gid_header = self.gid_header, gid_info = self.gid_info)

        if self.options.not_cgi:
            codecs.open(os.path.join(self.REPORTS_DIR, 'gid_info.html'), 'w', 'utf8').write(table_html)
        else:
            print table_html.encode('utf8')

    def print_stats(self):
        if self.options.not_cgi:
            VtvTask.print_stats(self)

    def post_print_stats(self):
        if self.options.not_cgi:
            self.print_report_link()

    def cleanup(self):
        if self.options.not_cgi:
            self.move_logs(self.OUT_DIR, [ ('.', 'gid_info*log') ] )
            self.remove_old_dirs(self.OUT_DIR, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)
        else:
            remove_file(self.options.log_file_name, self.logger)


if __name__ == '__main__':
    vtv_task_main(GidInfo)
    sys.exit( 0 )

