#!/usr/bin/env python

import os
import re
import MySQLdb
import traceback
import simplejson
import collections
import optparse
import shutil
import os.path
import sys
import glob
import time
import datetime
import logging, logging.handlers

from pyes import *
from pyes.exceptions import IndexAlreadyExistsException

from operator import itemgetter
import genericFileInterfaces
from _utils import *
from ssh_utils import *

SCHEMA_DIR = os.getcwd()
SCRIPT_PATH = os.path.abspath(os.path.dirname(__file__))
#conn = ES('10.4.18.35:9200', timeout=120)
es_machines = {'dev': '10.4.18.35:9200', 'prod': '10.4.18.101:9200'}

"""
DATA_FILES_PATHS =  {
                        'trends' : ('/data/DATAGEN/current/topics_in_news_data', 'DATA_TRENDING_TOPICS_MC_FILE', 
                                    'contentVersion.txt', 'data_version'),
                        'trending_topics': ('/data/DATAGEN/current/topics_in_news_data', 'DATA_TRENDING_TOPICS_AUX_FILE',
                                            'contentVersion.txt', 'data_version'),
                        'connections': ('/data/DATAGEN/current/topics_in_news_data', 'DATA_TRENDING_CONNECTIONS_AUX_FILE',
                                        'contentVersion.txt', 'data_version')
                    }
"""

DATA_FILES_PATHS =  {
                        'trends' : ('/home/veveo/datagen/TOPICS_IN_NEWS/topics_in_news_data/', 'DATA_TRENDING_TOPICS_MC_FILE', 
                                    'contentVersion.txt', 'data_version'),
                        'trending_topics': ('/home/veveo/datagen/TOPICS_IN_NEWS/topics_in_news_data/', 'DATA_TRENDING_TOPICS_AUX_FILE',
                                            'contentVersion.txt', 'data_version'),
                        'connections': ('/home/veveo/datagen/TOPICS_IN_NEWS/topics_in_news_data/', 'DATA_TRENDING_CONNECTIONS_AUX_FILE',
                                        'contentVersion.txt', 'data_version')
                    }   


def get_default_indexes():

    def_indexes = { 'trends': 'trends_info',
                    'trending_topics': 'trending_topics_info',
                    'connections': 'connections_info'
    }

    return def_indexes

def insert_index_values(_source, _type, index_dict):
    _conn, _cursor = create_cursor()

    _source = " ".join(_source.split("_")).title()
    log.info("Source: %s - Type: %s - Index Dict: %s", _source, _type, index_dict)
    query = "INSERT INTO es_indexes (source, type, index_name, status, status_text, created_at, modified_at) VALUES (%s, %s, %s, %s, %s, NOW(), NOW());"

    for _type, index_val in index_dict.iteritems():
        log.info("Inside loop _type: %s", _type)

        index_name, _status, _status_msg = index_val
        values = (_source, _type, index_name, _status, _status_msg)
        log.info("Insert ES index, Query: %s", query % values)
        _cursor.execute(query, values)
        _conn.commit()

    _conn.close()

def index_new_data(es_conn, value, index_name, _setup, failed_gids):
    _count, is_indexed, _error_msg = 0, False, ''
    while (_count < 6 and is_indexed == False):
        try:
            result = es_conn.index(value, index_name, "item", value['_id'])
            is_indexed, _error_msg = True, 'Success'
        except:
            _es_conn = create_es_conn(_setup)
            es_conn = _es_conn if _es_conn else es_conn
            _error_msg = traceback.format_exc().splitlines()[-1]
            log.error("Error while indexing data, msg: %s", traceback.format_exc())

        _count += 1
        if not is_indexed: log.info("In Indexing Loop, Count: %s --- Is_indexed: %s --- Sk: %s", _count, is_indexed, value['_id'])

    if not is_indexed:
        failed_gids.append(value['_id'])
        log.error("Error while indexing,  Error: %s", traceback.format_exc())

    return _error_msg

def create_index(es_conn, index_name, mapping):
    new_index = index_name.strip() +'_'+ datetime.datetime.now().strftime("%m_%d")
    log.info("In Create Index, Index: %s --- New Index: %s", index_name, new_index)
    try:
        es_conn.create_index(new_index)

        es_conn.put_mapping(doc_type="item", indexes=new_index, mapping=mapping)

        es_conn.update_settings(new_index, {"index" : {"number_of_replicas" : 0}})
    except Exception:
        log.info("New Index already Exists, New Index: %s", new_index)
        log.error("Error While creating index: %s" % traceback.format_exc())

        new_index += '_' + datetime.datetime.now().strftime("%H_%M")
        log.info("Creating new index again New Index: %s", new_index)
        es_conn.create_index(new_index)

        es_conn.put_mapping(doc_type="item", indexes=new_index, mapping=mapping)

        es_conn.update_settings(new_index, {"index" : {"number_of_replicas" : 0}})

    return new_index

def create_trends_index(es_conn, index_name):
    mapping = {"item": { "_source": {"compress": True}, "properties":{
                                    "Gi": {"type": "string", "analyzer": "standard"},
                                    "Ti": {"type": "string", "analyzer": "standard"},
                                    "Bp": {"type": "integer"},
                                    "Cu": {"type": "integer"},
                                    "Tm": {"type": "string"},
                                    "Cn": {"type": "string", "analyzer": "standard"},
                                    "Ke": {"type": "string", "analyzer": "standard"}
                }}}

    new_index = create_index(es_conn, index_name, mapping)
 
    return new_index

def dump_trends_data(es_conn, _setup, seed_file, index_name, data_type):
    log.info("In Seed %s Data Dump", data_type)

    _ids, failed_gids = [], []
    count, _status = 0, 1
    with open(seed_file, 'r') as f_outer:

        recs = f_outer.readlines()
        if not check_records_count(data_type, 'Veveo', es_conn, len(recs), index_name):
            sys.exit(1)

        for index, rec_values in enumerate(recs):
            value = dict(strip_values(i.split(':', 1)) for i in rec_values.split('#<>#') if ':' in i)

            value = encode_values(value)
            value['Bp'] = int(value['Bp']) if value['Bp'] else 0
            value['rank'] = index + 1
            value['_id'] = value['Gi']
            _status_msg = index_new_data(es_conn, value, index_name, _setup, failed_gids)
            _ids.append(value['_id'])
            count += 1
            if count %10000 == 0: log.info('FLUSHED 10000 records %s', count)

    if failed_gids: _status = 0; write_failed_gids_into_file(data_type, failed_gids)
    log.info("Len of Total Records: %s", count)

    return _status, _status_msg

def create_trending_topics_index(es_conn, index_name):
    mapping = {"item": { "_source": {"compress": True}, "properties":{
                                    "Gi": {"type": "string", "analyzer": "standard"},
                                    "Ti": {"type": "string", "analyzer": "standard"},
                                    "Sr": {"type": "string", "analyzer": "standard"},
                                    "Sk": {"type": "string", "analyzer": "standard"},
                                    "Tm": {"type": "string", "analyzer": "standard"},
                                    "Rn": {"type": "integer"},
                                    "Tt": {"type": "string", "analyzer": "standard"},
                                    "Tx": {"type": "string", "analyzer": "standard"},
                                    "Ur": {"type": "string", "analyzer": "standard"}
                }}}

    new_index = create_index(es_conn, index_name, mapping)
 
    return new_index

def x(data):
    try:
        return ''.join([chr(ord(x)) for x in data]).decode('ascii', 'ignore')
    except:
        return data


def dump_trending_topics_data(es_conn, _setup, seed_file, index_name, data_type):
    log.info("In Seed %s Data Dump", data_type)

    _ids, failed_gids = [], []
    count, _status = 0, 1
    with open(seed_file, 'r') as f_outer:

        recs = f_outer.readlines()
        if not check_records_count(data_type, 'Veveo', es_conn, len(recs), index_name):
            sys.exit(1)

        for rec_values in recs:
            value = dict(strip_values(i.split(':', 1)) for i in rec_values.split('#<>#') if ':' in i)

            value = encode_values(value)
            unique = '#<>#'.join([value['Gi'], value['Sr'], x(value['Sk'].replace('+', '').replace(' ', '')), value['Tm'], value['Rn']])
            value['_id'] = unique
            _status_msg = index_new_data(es_conn, value, index_name, _setup, failed_gids)
            _ids.append(value['_id'])
            count += 1
            if count %10000 == 0: log.info('FLUSHED 10000 records %s', count)

    if failed_gids: _status = 0; write_failed_gids_into_file(data_type, failed_gids)
    log.info("Len of Total Records: %s", count)

    return _status, _status_msg

def create_connections_index(es_conn, index_name):
    mapping = {"item": { "_source": {"compress": True}, "properties":{
                                    "Gi": {"type": "string", "analyzer": "standard"},
                                    "Ti": {"type": "string", "analyzer": "standard"},
                                    "Cn": {"type": "string", "analyzer": "standard"},
                                    "Ke": {"type": "string", "analyzer": "standard"},
                                    "Sr": {"type": "string", "analyzer": "standard"},
                                    "Cu": {"type": "integer"}
                }}}

    new_index = create_index(es_conn, index_name, mapping)
 
    return new_index

def dump_connections_data(es_conn, _setup, seed_file, index_name, data_type):
    log.info("In Seed %s Data Dump", data_type)

    _ids, failed_gids = [], []
    count, _status = 0, 1
    with open(seed_file, 'r') as f_outer:
        for rec_values in f_outer:
            value = dict(strip_values(i.split(':', 1)) for i in rec_values.split('#<>#') if ':' in i)

            value = encode_values(value)
            value['_id'] = '#<>#'.join([value['Gi'], value['Cn'], x(value['Ke'].replace(' ' , '')), value['Sr']])

            _status_msg = index_new_data(es_conn, value, index_name, _setup, failed_gids)
            _ids.append(value['_id'])

            count += 1
            if count %10000 == 0: log.info('FLUSHED 10000 records %s', count)

    if failed_gids: _status = 0; write_failed_gids_into_file(data_type, failed_gids)
    log.info("Len of Total Records: %s", count)

    return _status, _status_msg

def send_status_mail(new_recs_count, old_records_count, es_conn, index_name, _type):
    conn, cursor = create_cursor()

    path, file_name, version_file, version_name = DATA_FILES_PATHS[_type]

    subject = 'Trends on %s' % datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    body = ''
    body += '<p>New records count is less than old records count</p>'
    body += '<p>Existing Version: %s, Records Count: %s' % (get_seed_version(conn, cursor, _type), old_records_count)
    body += '<p>New Version: %s, Records Count: %s' % (DATA_VERSION['trends'], new_recs_count)
    body += '<p>Path: 10.4.2.187: %s/%s</p>' % (path, file_name)

    send_mail(subject, body, receivers=['web@headrun.com'], cc=['veveo@headrun.com'])

def check_records_count(type, source, es_conn, new_recs_count, index_name):
    conn, cursor = create_cursor()
    query = 'select index_name from es_indexes where id in (select max(id) from es_indexes where type="seed_%s" and status=1 and source = "%s");'
    cursor.execute(query % (type, source))
    data = cursor.fetchone()
    latest_index = data[0].strip()

    old_records_count = es_conn.get_indices().get(latest_index).get('num_docs', 0)

    status = True
    if new_recs_count < ((old_records_count*60)/100):
        log.info("New records count is less than old records count, new records count: %s", new_recs_count)
        log.info('sending mail...')
        send_status_mail(new_recs_count, old_records_count, es_conn, index_name, type)
        log.info('deleting newly created index: %s', index_name)
        es_conn.delete_index(index_name)
        status = False
    
    cursor.close()
    conn.close()

    return status   

def del_db_entry(conn, cursor, table, ids):
    del_query = 'delete from %s where id in (%s);' % (table, ','.join([str(i) for i in ids]))
    try:
        log.info("Deleting Old entries from %s, Query: %s", table, del_query)
        cursor.execute(del_query)
        conn.commit()
    except:
        log.error("Error While deleting indexes from db, Ids: %s", ids)
        log.error("Error Message: %s", traceback.format_exc())

def delete_old_indexes(es_conn, source, type):
    delete_indexes = []
    conn, cursor = create_cursor()
    query = 'select index_name from es_indexes where id in (select max(id) from es_indexes where type="seed_%s" and status=1 and source = "%s");'
    cursor.execute(query % (type, source))
    data = cursor.fetchone()
    if not data: return delete_indexes
   
    _ids = []
    latest_index = data[0].strip()
    d_index_names = 'select id, index_name from es_indexes where index_name !="%s" and type = "seed_%s";' % (latest_index, type)
    cursor.execute(d_index_names)
    recs = cursor.fetchall()
    for rec in recs:
        _id, index_name = rec
        _ids.append(_id)
        try:
            es_conn.delete_index(index_name)
            delete_indexes.append(index_name)
        except:
            log.error("Error While deleting index, Index Name: %s", index_name)
            log.error("Error Message: %s", traceback.format_exc())

    if _ids: del_db_entry(conn, cursor, 'es_indexes', _ids)
    return delete_indexes
    
def update_data_push_log(conn, cursor, process_id, status, status_text):
    up_query = 'UPDATE visualmerge_data_push_log set status=%s, status_text="%s", modified_at=NOW() where id=%s;'
    up_query = up_query % (status, status_text, process_id)
    try:
        cursor.execute(up_query)
        conn.commit()
        conn.close()
    except:
        log.error("Error while Updating visualmerge_data_push_log table, Query: %s", up_query)
        log.error("Error Message: %s", traceback.format_exc())
        conn.close()

def get_push_dataid(data_type):
    file_type = 'trends_related_files'
    data_id = data_type.capitalize() +'_'+  datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d%H%M") 
    if data_type == 'all':
        data_id = "TRENDS" +'_related_'+  datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d%H%M")
    else:
        data_dir, data_file, content_ver_file, version_key = DATA_FILES_PATHS[data_type]
        data_id = data_file +'_'+  datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d%H%M")
    return data_id, file_type

def create_data_push_log_entry(conn, cursor, data_type):
    insert_query = "insert into visualmerge_data_push_log (%s) values (1, 'Veveo', '%s', '%s', '', '', 0, 'Running', NOW(), NOW());"
    fields = ['user_id', 'source', 'data_id', 'file_type', 'merge_target', 'merge_file_format', 'status',
                                                                'status_text', 'created_at', 'modified_at']

    data_id, file_type = get_push_dataid(data_type)
    insert_query = insert_query % (', '.join(fields), data_id, file_type)
    try:
        cursor.execute(insert_query)
        conn.commit()
    except cursor.IntegrityError:
        log.error("Data trying to load is already loaded into ES, DataId: %s --- Filetype: %s", data_id, file_type)
        conn.close()
        sys.exit(1)

    select_query = 'select id from visualmerge_data_push_log where data_id = "%s" and file_type = "%s";' % (data_id, file_type)
    cursor.execute(select_query)
    data = cursor.fetchone()
    if data:
        return data[0]
    else:
        log.error("Failed to get process id, DataId: %s --- Filetype: %s", data_id, file_type)
        conn.close()
        sys.exit(1)
    
def get_seed_version(conn, cursor, data_type):
    query = 'select version from seed_version where type = "%s";' % data_type
    cursor.execute(query)
    data = cursor.fetchone()
    if data:
        version = data[0].strip()
    else:
        version = ''
    return version

def scp_files_from_dest(_path, _file):
    global INPUT_DIR

    INPUT_DIR = os.path.join(SCHEMA_DIR, 'trends_related_files')
    if not os.path.isdir(INPUT_DIR):
        os.mkdir(INPUT_DIR)
    else:
        shutil.rmtree(INPUT_DIR)
        os.mkdir(INPUT_DIR)
    
    #log.info("Machine Ip: %s - Path: %s - File: %s", machine_ip, _path, _file)
    cmd = "veveo@%s:%s/%s" % (DATA_IP, _path, _file)
    status = scp('veveo123', cmd, INPUT_DIR)

    file_path = os.path.join(INPUT_DIR, _file)
    if os.path.isfile(file_path):
        status = 1
    else:
        status = 0
        log.error("Failed to Scp, Ip: %s --- Path: %s --- File: %s", DATA_IP, path, _file)
    return status 

def insert_new_version(conn, cursor, data_type, new_version):
    query = 'insert into seed_version (type, version, created_at) values ("%s", "%s", NOW()) ON DUPLICATE KEY UPDATE version="%s", modified_at=NOW();' % (data_type, new_version, new_version)
    try:
        cursor.execute(query)
        conn.commit()
    except:
        log.error("Error while inserting New seed version, Query: %s", query)
        log.error("Error Message: %s", traceback.format_exc())

def check_content_version(conn, cursor, data_type):
    global DATA_VERSION

    DATA_VERSION = {}
    type_to_scp = DATA_FILES_PATHS.keys() if data_type == 'all' else [data_type]
    for type in type_to_scp:
        scp_dir, scp_file, content_ver_file, version_key = DATA_FILES_PATHS[type]
        existing_data_version = get_seed_version(conn, cursor, type)
        scp_status = scp_files_from_dest(scp_dir, content_ver_file)
        if not scp_status: continue
        file_path = os.path.join(INPUT_DIR, content_ver_file)
        lines = open(file_path).readlines()
        for line in lines:
            if not line.startswith(version_key): continue
            ver_k, new_version = [i.strip() for i in line.split('=')]
            if new_version > existing_data_version:
                DATA_VERSION[type] = new_version
                insert_new_version(conn, cursor, type, new_version)
            else:
                log.info("Latest Version data is not available, DataType: %s, Ip: %s --- Path: %s --- File: %s", type, DATA_IP, scp_dir, content_ver_file)
                log.info("DataType: %s, Existing Version: %s --- New Version: %s", type, existing_data_version, new_version)

 
def main(options):
    global DATA_IP, log

    log = init_logger(options.logfile)
    conn, cursor = create_cursor()
    DATA_IP = options.data_ip
    process_id = options.process_id
    data_type = options.data_type
    _setup = options.setup
    source = options.source

    if data_type != 'all' and data_type not in DATA_FILES_PATHS.keys():
        log.error("Accepted Data types are: %s --- Given Data Type: %s", DATA_FILES_PATHS.keys(), data_type)
        if process_id:
            s_msg = "Load data type not supported"
            update_data_push_log(conn, cursor, process_id, 2, s_msg) 
        sys.exit(1)

    check_content_version(conn, cursor, data_type)
    if len(DATA_VERSION) == 0:
        s_msg = "Latest Version data is not available"
        log.error(s_msg)
        if process_id:
            update_data_push_log(conn, cursor, process_id, 2, s_msg)
        sys.exit(1)
            
    if not process_id:
        log.info("Process Id not defined, Creating new entry in visualmerge_data_push_log table")
        process_id = create_data_push_log_entry(conn, cursor, data_type)
        log.info("Process Id: %s", process_id)

    if not _setup: log.info("Setup option was empty, ex -s dev or -s prod"); return
    if _setup not in es_machines.keys(): log.info("Setup option should be prod or dev"); return

    log.info("Starting Loading Process, Setup: %s --- Data Type: %s --- Processing DataTypes: %s", _setup,
                                                                             data_type, DATA_VERSION.keys())
    log.info("Start Time Is: %s", str(datetime.datetime.now()))

    load_srt_time = datetime.datetime.now()
    es_conn = create_es_conn(_setup)
    if not es_conn: log.info("Failed to create Es connection"); return

    index_dict = {}
    default_indexes = get_default_indexes()

    create_index_func_dict = {'trends': create_trends_index,
                              'trending_topics': create_trending_topics_index,
                              'connections': create_connections_index}

    dump_func_dict = { 'trends': dump_trends_data,
                       'trending_topics': dump_trending_topics_data,
                       'connections': dump_connections_data}

    for _type, file_info in DATA_VERSION.iteritems():
        data_dir, data_file, content_ver_file, version_key = DATA_FILES_PATHS[_type]
        scp_status = scp_files_from_dest(data_dir, data_file)
        if not scp_status: continue

        data_file_path = os.path.join(INPUT_DIR, data_file)
        log.info("Started Populating %s data", _type)
        if not is_file_exist(data_file_path):
            conn, cursor = create_cursor()
            s_msg = "Given file %s, not found in directory" % (data_file_path)
            log.error(s_msg)
            update_data_push_log(conn, cursor, process_id, 2, s_msg)
            sys.exit(1)

        data_strt = datetime.datetime.now()
        new_index = create_index_func_dict.get(_type, create_trends_index)(es_conn, default_indexes[_type])
        log.info("New Index: %s", new_index)
        if new_index:
            try:
                _status, _status_msg = dump_func_dict.get(_type, dump_trends_data)(es_conn, _setup, data_file_path, new_index, _type)
                if _status_msg.lower() == 'success':
                    deleted_indexes = delete_old_indexes(es_conn, source, _type)
                    log.info("Deleted Old Indexes are: %s", deleted_indexes)
            except Exception, e:
                print traceback.format_exc()
                _status, _status_msg = 0, e.message
                log.error("Error: %s", traceback.format_exc())

            index_dict['seed_%s' % _type] = (new_index, _status, _status_msg)

        data_dur = str(datetime.datetime.now() - data_strt)
        log.info("Duration of Populating %s Data is: %s\n", _type, data_dur)
     
    log.info("Index Dict: %s", index_dict)
    insert_index_values(source, data_type, index_dict)
    
    conn, cursor = create_cursor()
    update_data_push_log(conn, cursor, process_id, 1, 'Completed')
    end_time = datetime.datetime.now()
    log.info("Load Duration Is: %s", str(end_time - load_srt_time))
    return 0

if __name__ == "__main__":

    parser = optparse.OptionParser()
    parser.add_option('-l', '--logfile', metavar='FILE', default='load_trends.log')
    #parser.add_option('-a', '--data-ip', default='10.4.2.187', help='Datafiles Machine Ip, ie: 10.4.2.187')
    parser.add_option('-a', '--data-ip', default='10.4.18.49', help='Datafiles Machine Ip, ie: 10.4.2.187')
    parser.add_option('-d', '--source', default='Veveo', help='Source, Ie: Veveo')
    parser.add_option('-t', '--data-type', default='all', help='all/trends/trending_topics/connections')
    parser.add_option('-s', '--setup', default='dev', help='prod/dev')
    parser.add_option('-p', '--process-id', default="", help="Process id")

    (options, args) = parser.parse_args()
    try:
        retval = main(options)
    except (SystemExit, KeyboardInterrupt):
        raise
    except:
        log.error("Error: %s", traceback.format_exc())
        log.exception('quitting main')
        retval = -1
    
    sys.exit(retval)

