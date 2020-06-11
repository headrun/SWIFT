#!/usr/bin/env python
import MySQLdb
import datetime
from vtv_utils import initialize_timed_rotating_logger, vtv_send_html_mail_2

now = datetime.datetime.now().strftime('%Y-%m-%d')

class CheckFields():

    def __init__(self):
        self.logger        = initialize_timed_rotating_logger('sports_validator.log')
        self.conn          = MySQLdb.connect(host='10.28.218.81', user='veveo', passwd='veveo123', db='SPORTSDB', charset='utf8')
        #self.conn          = MySQLdb.connect(host='10.28.216.45', user='veveo', passwd='veveo123', db='SPORTSDB_DEV', charset='utf8')
        self.today         = str(datetime.datetime.now().date())
        self.cursor        = self.conn.cursor()
        self.file_         =  open('/home/veveo/reports/html_entites_%s.html' % self.today, 'w')

    def send_mail(self, text):
        subject    = 'Html Entities Report For SPORTSDB'
        server     = 'localhost'
        #sender     = 'sports@headrun.com'
        sender = 'headrun@veveo.net'
        #receivers = ['raman.arunachalam@rovicorp.com', 'vineet.agarwal@rovicorp.com', 'sports@headrun.com']
        receivers = ['sports@headrun.com']
        vtv_send_html_mail_2(self.logger, server, sender, receivers, subject, '', text, '')


    def generate_msg(self, other_stats):
        '''create message body'''
        msg = 'Hi, <br /> <br />'

        msg = msg + '<b>Started time:</b> %s<br />' % datetime.datetime.now()
        msg =  msg + self.prepare_each_field_stats_msg(other_stats)
        self.send_mail(msg)

    def prepare_each_field_stats_msg(self, app_stats_dict):
        table_data = '<br /><br /><b>%s</b><br /><table border="1" \
                    style="border-collapse:collapse;" cellpadding="3px" cellspacing="3px"><tr>' % 'Sports Tables'

        headers = ['Table', 'Field', 'quot', 'amp', '\\n_count', '\\t_count', '<', '>', 'Null', '?', '#', '@', '$', '*', '^', '!', 'Toomany Spaces', '{', '}', '=', ':', ',', '[', ']']
        for header in headers:
            table_data += '<th>%s</th>' % header
        table_data += '</tr>'
        tables = app_stats_dict.keys()
        tables.sort()
        
        for table in tables:
            app_values = app_stats_dict[table]
            for app_value in app_values:
                if not [val for val in app_value[1:] if str(val)!='0']:
                    continue
                table_data += '<tr>'
                table_data += '<td>%s</td>' % (table)
                for ind, app in enumerate(app_value):
                    if ind !=0 and str(app) != '0':
                        table_data += '<td style="color: %s">%s</td>' % ('red', app)
                    else:
                        table_data += '<td>%s</td>' % (str(app))
                table_data += '</tr>'
        table_data += '</table>'

        if table_data:
            msg = table_data
            self.file_.write('%s\n' % table_data)
        else: msg = 'Checked with the entities "new_line", "/0", "&amp", "&quot"'
        return msg



    def execute_query(self, query):
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result:
            result = result[0]
        return result


    def source_extra_details(self, source_table_name):
        if isinstance(source_table_name, tuple):
            source_table_name = source_table_name

        result_ls = []
        f_query = 'desc %s' % source_table_name
        self.cursor.execute('desc %s' % source_table_name)
        fields = self.cursor.fetchall()
        for field_ in fields:
            field, data_type = field_[0], field_[1]
            if data_type in ['datetime', 'timestamp']:
                continue
            #if 'text' in data_type or 'varchar' in data_type:
            q = '%&quot;%'
            a = '%&amp;%'
            n = '\\n'
            z = '\\0'
            lt = '%&lt;%'
            gt = '%&gt;%'
            t = '\\t'
            ts = ''
            tms = '  '
            br_o = '{'
            br_c = '}'
            equal= '='
            col = ':'
            cam = ','
            squar = '['
            squ = ']'

            q_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, q)
            q_count = self.execute_query(q_query)

            a_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, a)
            a_count = self.execute_query(a_query)

            n_query = 'select count(*) from %s where %s like "%%%s%%"' % (source_table_name, field, n)
            n_count = self.execute_query(n_query)

            t_query = 'select count(*) from %s where %s like "%%%s%%"' % (source_table_name, field, t)
            t_count = self.execute_query(t_query)

            lt_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, lt)
            lt_count = self.execute_query(lt_query)

            gt_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, gt)
            gt_count = self.execute_query(gt_query)

            slash_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, z)
            slash_count = self.execute_query(slash_query)

            null_query = 'select count(*) from %s where %s is NULL' % (source_table_name, field)
            null_count = self.execute_query(null_query)

            spl_query =  'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%?%')
            spl_count = self.execute_query(spl_query)

            hash_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%#%')
            hash_count = self.execute_query(hash_query)

            amp_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%@%')
            amp_count = self.execute_query(amp_query)

            dol_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%$%')
            dol_count = self.execute_query(dol_query)

            star_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%*%')
            star_count = self.execute_query(star_query)

            cap_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%^%')
            cap_count = self.execute_query(cap_query)

            ex_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%!%')
            ex_count = self.execute_query(ex_query)

            tms_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, tms)
            tms_count = self.execute_query(tms_query)

            bro_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%{%')
            bro_count = self.execute_query(bro_query)

            brc_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%}%')
            brc_count = self.execute_query(brc_query)

            equal_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%=%')
            equal_count = self.execute_query(equal_query)

            col_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%:%')
            col_count = self.execute_query(col_query)

            cam_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%,%')
            cam_count = self.execute_query(equal_query)

            squar_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%[%')
            squar_count = self.execute_query(squar_query)

            squ_query = 'select count(*) from %s where %s like "%s"' % (source_table_name, field, '%]%')
            squ_count = self.execute_query(squ_query)



            result = (field, q_count, a_count, n_count, t_count, lt_count, gt_count, null_count, spl_count, hash_count, amp_count, dol_count, star_count, cap_count, ex_count, tms_count, bro_count, brc_count, equal_count, col_count, cam_count, squar_count, squ_count)
            result_ls.append(result)

        return result_ls

    def main(self):
        '''call app store tables'''
        other_stats = {}
        query = 'show tables like "sports_%"'
        self.cursor.execute('show tables like "sports_%"')
        VOD_TABLES = self.cursor.fetchall()
        VOD_TABLES = [table[0] for table in VOD_TABLES]
        for table in VOD_TABLES:
            if table in ['sports_videos', 'sports_wiki_merge']:
                continue
            result = self.source_extra_details(table)
            if result is not None:
                other_stats[table] = result
        self.generate_msg(other_stats)


if __name__ == "__main__":
    OBJ = CheckFields()
    OBJ.main()
