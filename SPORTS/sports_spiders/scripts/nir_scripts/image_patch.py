import sys
import os
import re
import urllib
import md5
import json
import copy
import MySQLdb
import glob
import urllib2
import traceback
import datetime, collections
from datetime import timedelta

from vtv_utils import VTV_CONTENT_VDB_DIR, copy_file, execute_shell_cmd
from vtv_task import VtvTask, vtv_task_main
from vtv_db import get_mysql_connection
from data_schema import RECORD_SEPARATOR, TV_SEED_FOLD_SCHEMA_FIELDS, GID_REGEX, \
                        get_schema
import genericFileInterfaces
import foldFileInterface
from guidPairLoaderUtils import guidloadGuidPairList

class RoviImageLoader(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)


    def run_main(self):
        self.open_cursor("10.4.2.207", "IMAGEDB")
        '''
        need_dict = {}
        MC_FILE = "DATA_IMAGE_MC_FILE.PATCH"
        out_file = "images_mc"
        self.out_file = open("DATA_IMAGE_MC_FILE.PATCH1", 'w')
        done_list = []
        '''

        schema = {'Gi' : 0, 'Im' : 1, "Ll" : 2}
        '''
        for record in genericFileInterfaces.fileIterator(out_file, schema):
                gid   = record[schema['Gi']]
                image = record[schema['Im']]
                lang  = record[schema['Ll']] 
                need_dict[gid] = (image, lang)
        '''

        '''
        for record in genericFileInterfaces.fileIterator(MC_FILE, schema):
                gid   = record[schema['Gi']]
                image = record[schema['Im']]
                lang  = record[schema['Ll']]

                if need_dict.has_key(gid):
                    image, lang = need_dict[gid]

                image = image.split("#")[0]
                done_list.append(gid)
                record = [gid, "%s" %(image), "eng"]
                genericFileInterfaces.writeSingleRecord(self.out_file, record, schema, 'utf-8')

        '''
        '''
        for k, v in need_dict.iteritems():
            if k in done_list:
                continue

            image, lang = need_dict[k]
            record = [k, "%s#" %(image), "eng"]
            genericFileInterfaces.writeSingleRecord(self.out_file, record, schema, 'utf-8')
        '''
        
        import traceback 
        set_2 = {}
        schema = {'Gi' : 0 , "Im" : 1 }
        for record in genericFileInterfaces.fileIterator("IMAGES_FILMI", schema):
            gid     = record[schema['Gi']]
            image   = record[schema['Im']]
            sk      = gid.replace("WIKI", "")

            if gid.startswith("WIKI"):
                print image
            else:continue

            imagehash = md5.md5(image).hexdigest()
            query = 'insert into image_meta(image_url, aspect_ratio, imagehash, is_valid, created_at, modified_at) values (%s, %s, %s, 1, now(), now()) on duplicate key update modified_at = now()'
            values = (image,'', imagehash)
            self.cursor.execute(query, values)

            query = 'select id from image_meta where imagehash=%s'
            self.cursor.execute(query, (imagehash))
            image_meta_id = self.cursor.fetchall()[0][0]

            query = "insert into source_image_map(source, sk, type, image_meta_id, created_at, modified_at) values (%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), image_meta_id=%s"
            values = ("wiki", sk, '', image_meta_id, image_meta_id)
            self.cursor.execute(query, values)

            query = "insert into gid_source_map(source, sk, type, gid, iso, created_at, modified_at) values (%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
            values = ("wiki", sk, '', gid, "eng")
            self.cursor.execute(query, values)


            #genericFileInterfaces.writeSingleRecord(self.out_file, record, schema, 'utf-8')


if __name__ == '__main__':
    vtv_task_main(RoviImageLoader)
    sys.exit( 0 )
