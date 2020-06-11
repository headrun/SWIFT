from vtv_task import VtvTask, vtv_task_main
import genericFileInterfaces
from data_schema import get_schema
import os
import re
import traceback
import leveldb
from datetime import *
from pyelasticsearch import ElasticSearch
from pyes import *

from vtv_utils import execute_shell_cmd
from ssh_utils import scp
import redis
import collections

pattern = re.compile("rovi_id_1.1#\d+#\w+")
machine_ip  = "http://10.8.21.23:9200/"
pyel_conn   = ElasticSearch(machine_ip, timeout=180)

class ViewLoader(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.data_file  = os.path.join(os.getcwd(), "rovi.data")
        #need to move these things to config file
        self.stats      = collections.Counter()
        self.redis_host = "10.4.18.64"

        self.today_date      = datetime.now().strftime("%Y%m%d")
        self.index_machine   = "10.4.18.35"
        self.es_port         = "9200"
        self.mapping         = {'item': {'_source': {'compress': True},
                                'properties': {
                                    'En': {'type': 'integer'},
                                    'Ep': {'analyzer': 'standard', 'type': 'string'},
                                    'Gi': {'analyzer': 'standard', 'type': 'string'},
                                    'Od': {'type': 'date'},
                                    'Ry': {'type': 'integer'},
                                    'Sn': {'type': 'integer'},
                                    'En': {'type': 'integer'},
                                    'Fd': {'type': 'nested'},
                                    'Ti': {'analyzer': 'standard', 'type': 'string'},
                                    'Vt': {'type': 'string'},
                                    'cast'            : {'type': 'string'},
                                    'director'        : {'type': 'string'},
                                    'language'        : {'type': 'string'},
                                    'rating'          : {'type': 'string'},
                                    'description'     : {'type': 'string'},
                                    'other_type'      : {'type': 'string'},
                                    'source_key'      : {'type': 'string'},
                                    'Pi'              : {'type': 'string'},
                                    'genre'           : {'type': 'string'},
                                    'country'         : {'type': 'string'},
                                    'duration'        : {'type': 'integer'},
                                    'mi'              : {'type': 'string'},
                                    'host'            : {'type': 'string'},
                                    'writer'          : {'type': 'string'},
                                        }}}
        self.index_name      = "roviview"
        self.conn = ES("%s:%s" %(self.index_machine, self.es_port))
        #self.create_redis_conn()
        self.dest_dir         = os.path.join(os.getcwd(), "tgz_files")
        self.source_ip        = "10.4.2.187"
        self.remote_file_path = "/home/veveo/datagen/tgzs/rovi_data/rovi_data_%s*.tgz"

    def create_redis_conn(self):
        try:
            self.redis_con   = redis.StrictRedis(host=self.redis_host, port=6379, db=0)
            self.redis_con.flushall()
        except:
            print traceback.format_exc()

    def delete_index(self):
        try:
            print "Deleting index"
            self.conn.delete_index(self.index_name)
        except:
            print "Got error while deleting"

    #Need to move this function from this script
    def create_index(self):
        try:
            print "Started Creating Index ...."
            self.conn.create_index(self.index_name) 
            self.conn.put_mapping("roviview", {'properties': self.mapping}, [self.index_name])
            print "Completed creating index ...."
        except:
            pass

    def clean_fd(self,fd):
        fds = fd.split("<>")
        list_of_fds = []

        for i in fds:
            if pattern.match(i):
                rovi_version, rovi_id, program_type = i.split("#")
                list_of_fds.append("%s_%s_%s" %(rovi_version, program_type, rovi_id))
            else:
                rovi_version, rovi_id = i.split("#")
                list_of_fds.append("%s_%s" %(rovi_version, rovi_id))
        return list_of_fds

    def bulk_insert(self):
        try:
            
            print "Came to  bulk index", datetime.now()
            result = pyel_conn.bulk_index(index = self.index_name,
                                          doc_type  = "item",
                                          docs      = self.main_docs)
            self.main_docs = []
            print "completed",datetime.now()
        except:
            print traceback.format_exc()
            print "Completed ..."

    def read_view_load_file(self):
        try:
            schema = {'Gi' : 0, 'Ti': 1, 'Ep' : 2, 'Ak': 3, 'Vt': 4,
                      'Ry' : 5, 'Od': 6, 'Sn': 7, 'En': 8, 'Fd' : 9,
                      'Ca' : 10, 'Di' : 11, 'Ll' : 12, 'Ra' : 13,
                      'De' : 14, 'Ot' : 15, 'Sk' : 16, 'Pi' : 17,
                      'Ge' : 18, 'Cl' : 19, 'Du' : 20, 'Mi' : 21, 
                      'Ho': 22, 'Wr':23 }
            self.main_docs  = []
            data_file = os.path.join(self.dest_dir, "rovi.data")

            print "In read_view_load_file"
            for record in genericFileInterfaces.fileIterator(data_file, schema):
                gid             = record[schema['Gi']]
                title           = record[schema['Ti']]
                ep_title        = record[schema['Ep']]
                aka             = record[schema['Ak']]
                video_type      = record[schema['Vt']]
                release_year    = record[schema['Ry']]
                release_date    = record[schema['Od']]
                season_number   = record[schema['Sn']]
                episode_number  = record[schema['En']]
                fd_ids          = record[schema['Fd']]
                cast            = record[schema['Ca']]
                director        = record[schema['Di']]
                language        = record[schema['Ll']]
                rating          = record[schema['Ra']]
                description     = record[schema['De']]
                other_type      = record[schema['Ot']]
                source_key      = record[schema['Sk']]
                Pi              = record[schema['Pi']]
                genre           = record[schema['Ge']]
                country         = record[schema['Cl']]
                duration        = record[schema['Du']]
                mi              = record[schema['Mi']]
                host            = record[schema['Ho']]
                writer          = record[schema['Wr']]
                    
                fds = self.clean_fd(fd_ids)
                rd = ''
                if release_date:
                    rd = datetime.strptime(release_date, "%Y-%m-%d").isoformat()

                if release_year: release_year = int(release_year)
                if season_number: season_number = int(season_number)
                if episode_number: episode_number = int(episode_number)

                data_dict = {
                              'Gi' : gid, "Ti" : title, "Ep" : ep_title, "Ak" : aka,
                              'Vt' : video_type, 'Ry' : release_year, "Od" : rd,
                              "Sn" : season_number, "En" : episode_number,
                              "Fd" : fds, 'cast' : cast, 'director' : director,
                              'language' : language, 'rating' : rating,
                              'description' : description, 'other_type': other_type,
                              'source_key' : source_key, 'Pi' : Pi,
                              'genre' : genre,
                              'country' : country,
                              'duration' : duration,
                              'mi' : mi,
                              'host' : host,
                              'writer' : writer
                            }

                if not rd:
                    data_dict.pop("Od")
                
                self.main_docs.append(data_dict)
                if len(self.main_docs) % 1000 == 0:
                    print "Going to bulk insert ..." 
                    self.bulk_insert()

                self.stats['Total'] += 1
            self.bulk_insert()
            #self.remove_the_files()
        except:
            print traceback.format_exc()
            print 'here'


    def untar(self):
        cmd  = "tar -xvf %s/rovi_data_%s*.tgz -C %s" %(self.dest_dir, self.today_date, self.dest_dir)
        status, output   = execute_shell_cmd(cmd)

    def copyfile(self):
        #move all these to top
        #if self.file_exists():
        #	sys.exit()

        file_name = self.remote_file_path % self.today_date
        status    = scp(self.vtv_password, '%s@%s:%s' % (self.vtv_username, self.source_ip, file_name), self.dest_dir)

        if status:
            self.untar()

    def run_main(self):
        #self.copyfile()
        #self.untar()
        self.delete_index()
        self.create_index()
        self.read_view_load_file()

if __name__ == '__main__':
    vtv_task_main(ViewLoader)
