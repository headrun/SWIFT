import sys
import os
import json
import time
import MySQLdb
import MySQLdb.cursors
import urllib, urllib2
import traceback
import requests
import codecs
import lxml.html

from vtv_utils import VTV_CONTENT_VDB_DIR, copy_file, execute_shell_cmd
from vtv_task import VtvTask, vtv_task_main
from vtv_db import get_mysql_connection
from vtv_utils import vtv_send_html_mail_2

PARTIAL_URL_FILE = "partial_url.txt"
DBIP             = "10.4.18.119"
PREFIX           = "http://ftpmirror.your.org/pub/wikimedia/images/wikipedia"
OUT_FILE         = "gid_filename_prefix_frequency.txt"
IMAGE_NOT_FOUND_AFTER_HTTP = "image_not_found_after_http_request.txt"
REGEX            = "//a/text()"
PATTERN          = "upload.wikimedia.org/wikipedia"
IMAGEDB_IP = "localhost"

count = 0


class WikiImageProcess(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.init_config(self.options.config_file)
        self.wiki_date_url          = self.get_default_config_value('WIKI_DATE_URL')
        self.db_ip                  = self.get_default_config_value('IMAGEDB_IP')
        self.db_name                = self.get_default_config_value('IMAGEDB_NAME')
        self.wiki_db_ip             = self.get_default_config_value('WIKI_IMAGEDB_IP')
        self.wiki_image_db          = self.get_default_config_value('WIKI_IMAGEDB')
        self.partial_url_txt        = os.path.join(os.getcwd(), "partial_url.txt")
        self.image_not_mapped_txt   = os.path.join(os.getcwd(), "image_not_mapped.txt") 

    def send_mail(self, body, subject):
        msg = MIMEText(body)
        msg['Subject'] = subject
        _to = [ 'aditya.kumar@rovicorp.com', 'aditya.kumar130901@gmail.com',
                'Sai.Pulikunta@rovicorp.com', 'Abhijit.Savarkar@rovicorp.com',
                'niranjan@headrun.com']
        _from = 'noreply@veveo.net'


    def get_image_name_to_url(self):
        name_to_url_dict = {}
        partial_url_file = open(PARTIAL_URL_FILE,'a+')

        for i, line in enumerate(partial_url_file):
            line = line.rstrip().split("/")
            image_name = line[-1]

            if len(image_name) > 4 and "." in image_name:
                name_to_url_dict[image_name] = "/".join(line[:-1])
                if i % 150000 == 0:
                    print i,line,  time.time()

        partial_url_file.close()
        return name_to_url_dict

    def dump_data_for_db(self):
        self.open_cursor(self.wiki_db_ip, self.wiki_image_db)
        filename_dict = {}
        no_map = open(self.image_not_mapped_txt, 'w')
        name_to_url_updated_list = self.get_image_name_to_url()

        '''
        print "Dict population is over"
        query = "SELECT il_from, il_to FROM imagelinks INTO OUTFILE '/tmp/imagelink.tsv' FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';"
        self.cursor.execute(query)
        print "Complete execute on imagelinks"
        '''

        imagelinks  = file("/tmp/imagelink.tsv", "r+").readlines()
        for row in imagelinks:
                try:
                    gid, image = row.strip().split("\t")
                except:
                    print row.strip()
                    continue
                #this method has to be repeted again becuase now we have more complete list in partial url files
                if name_to_url_updated_list.has_key(image):
                    if filename_dict.has_key(image):
                        freq = filename_dict[image][2]
                        freq = freq +1
                        filename_dict[image][2] = freq
                    else:
                        prefix = name_to_url_updated_list[image]
                        filename_dict[image] = [gid, prefix, 1]
                else:
                    no_map.write("%s\n" %(image))

        self.cursor.close()
        no_map.close()
        self.update_IMAGEDB(filename_dict)

    def update_IMAGEDB(self, filename_dict):
        self.open_cursor(self.db_ip, self.db_name)

        for key, val in filename_dict.items():
            filename    = key
            gid         = "WIKI%s" %(val[0])
            prefix      = val[1]
            freq        = val[2]
            try:
                query = 'INSERT INTO wiki_image_meta (filename, gid, prefix, frequency, modified_at) VALUES ("%s", "%s", "%s", %s, now()) on duplicate key update modified_at=now()' %(filename, gid, prefix, freq)
                self.cursor.execute(query)
            except:
                pass
        self.partial_url_uniq()

    def partial_url_uniq(self):
        partial_url_list = []
        partial_url_file = open(PARTIAL_URL_FILE,'rw')
        for i, line in enumerate(partial_url_file):
            partial_url_list.append(line)

        uniq_list = list(set(partial_url_list))
        partial_url_file.close()
        partial_url_file = open(PARTIAL_URL_FILE,'w')
        for line in uniq_list:
            partial_url_file.write(urllib.unquote(line))

        partial_url_file.close()

    def db_to_partial_url_mapping(self, name_to_url_dict):
        print "Loading wiki image il to dumps"
        self.open_cursor(self.wiki_db_ip, self.wiki_image_db)
        query = "SELECT distinct il_to from imagelinks"
        self.cursor.execute(query)
        image_not_found_list = set([])
        count = 0

        for row in self.get_fetchmany_results():
                count = count + 1
                if count % 10000 == 0:
                    print "loaded il : ", count

                image = row[0]
                if not name_to_url_dict.has_key(image):
                        image_not_found_list.add(image)

        image_not_found_list = list(image_not_found_list)
        #self.find_partial_url(image_not_found_list)
        self.dump_data_for_db()

    def find_partial_url(self, image_not_found_list):
        start    = 0
        step     = 40
        end      = step
        list_len = len(image_not_found_list)
        image_list_http = []
        print "Length of image_not_found", list_len

        while(end < list_len):
            image_list_http = image_not_found_list[start:end]
            start = end + 1
            if end % 10000 == 0:
                print end
            end = end + step
            self.make_http_call(image_list_http)

    def make_http_call(self, image_list_http):
        image_list_http1 = []
        for image in image_list_http:
            image = "File:%s" %(image)
            image_list_http1.append(image)
        titles  = "|".join(image_list_http1)
        titles  = urllib2.unquote(titles)
        url     = 'http://en.wikipedia.org/w/api.php?action=query&titles=%s&prop=imageinfo&iiprop=url&rawcontinue='%(titles)

        try:
            get_response        = requests.get(url=url)
            response            = get_response.content
            html_doc            = lxml.html.fromstring(response)
            #full_url_list       = html_doc.xpath(REGEX)
            x = "".join(html_doc.xpath("//pre//text()")).replace("\n", "")
            full_url_list  = json.loads(x)
            partial_url_file    = codecs.open(self.partial_url_txt,'a+', "utf-8")
            self.update_partial_url_file(full_url_list, partial_url_file)
        except:
            print traceback.print_exc()

    def update_partial_url_file(self, full_url_list, partial_url_file):
        #x['query']['pages']

        print "In update_partial_url_file"
        for pages, image_url in full_url_list['query']['pages'].iteritems():
            image_urls = image_url.get('imageinfo', [])
            for image_url in image_urls:
                image_url = image_url.get('url', "")
                if PATTERN in image_url:
                        try:
                                print image_url
                                partial_url = image_url.split(PATTERN)[1]
                                partial_url_file.write(urllib.unquote(partial_url))
                                partial_url_file.write("\n")
                        except:
                                print traceback.print_exc()

    def download_wiki_dump(self, date):
        #dump_list = ['imagelinks', 'image']
        dump_list = ['image']

        for dump in dump_list:
            cmd = "wget 'dumps.wikimedia.org/enwiki/%s/enwiki-%s-%s.sql.gz'" %(date,date, dump)
            status, output = execute_shell_cmd(cmd, self.logger)

            filename = "enwiki-%s-%s.sql.gz" %(date, dump)
            cmd = 'gunzip %s' %(filename)
            status, output = execute_shell_cmd(cmd, self.logger)

            filename = "enwiki-%s-%s.sql" %(date, dump)
            self.update_wiki_dump_db(filename)

    def update_wiki_dump_db(self, filename):
        cmd = "sed 's/ENGINE=InnoDB/ENGINE=MYISAM/g' %s > latest_wiki_dump.sql" %(filename)
        status, output = execute_shell_cmd(cmd, self.logger)

        print "Updating db with raw dumps"
        cmd = 'mysql -h %s WIKI_IMAGEDB < latest_wiki_dump.sql'%(IMAGEDB_IP)
        ret = os.system(cmd)
        status, output = execute_shell_cmd(cmd, self.logger)        

        print "Done updating"

    def get_date(self, url):
        get_response    = requests.get(url=url)
        response        = get_response.content
        html_doc        = lxml.html.fromstring(response)
        full_url_list   = html_doc.xpath("//a")
        date = ""
        for url in full_url_list:
            if not date and url.text == 'enwiki':
                date = url.values()[0]
                date = date.split("enwiki/")[1]
        return date

    def get_stats(self):
        self.open_cursor(self.db_ip, self.wiki_image_db) 
        query = "SELECT count((il_to)) from imagelinks"
        self.cursor.execute(query)

        for row in self.cursor.fetchall():
            number_of_imagelinks = row[0]

        query = "select count(img_name) from image"
        try:
            self.cursor.execute(query)
            for row in self.ursor.fetchall():
                number_of_image = row[0]
        except:
            number_of_image = 0
        
        self.cursor.close()

        self.open_cursor(self.db_ip, self.db_name)
        query = "select count(*) from wiki_image_meta"
        self.cursor.execute(query)
        for row in self.cursor.fetchall():
            total_wiki_images = row[0]

        fp = open(self.image_not_mapped_txt, 'r')
        image_not_found = len(fp.readlines())

        return image_not_found, number_of_imagelinks, number_of_image, total_wiki_images

    def set_options(self):
        config_file = os.path.join(os.getcwd(), 'wiki_image_loader.cfg')
        self.parser.add_option('-c', '--config-file', default=config_file, help='configuration file')

    def run_main(self):
        #pre_image_not_mapped, pre_imagelinks, pre_image, pre_wiki_db_images = self.get_stats()
        pre_message = "BEFORE RUN: \nTotal imagelinks: %s\nTotal image: %s\nTotal Wiki images: %s\nTotal Image Not Mapped: %s\n"
        #pre_message = pre_message % (pre_imagelinks, pre_image, pre_wiki_db_images, pre_image_not_mapped)
        #print pre_message
        date = self.get_date(self.wiki_date_url)
        
        print "Processing for date - %s" %(date)
        #self.download_wiki_dump(date)

        name_to_url_dict = self.get_image_name_to_url()
        self.db_to_partial_url_mapping(name_to_url_dict)

        #post_image_not_mapped, post_imagelinks, post_image, post_wiki_db_images = self.get_stats()
        post_message = "\n\nAFTER RUN: \nTotal imagelinks: %s\nTotal image: %s\nTotal Wiki images: %s\nTotal Image Not Mapped: %s\n" 
        post_message = post_message % (post_imagelinks, post_image, post_wiki_db_images, post_image_not_mapped)
        print post_message
        #send_mail("%s%s"%( pre_message, post_message), "WIKI DUMP STATS")
         

if __name__ == '__main__':
    vtv_task_main(WikiImageProcess)
    sys.exit( 0 )
