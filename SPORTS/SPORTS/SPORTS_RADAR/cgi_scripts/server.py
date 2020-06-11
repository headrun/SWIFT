#!/usr/bin/python           
import os, sys
import socket               # Import socket module
import marshal, logging
from optparse import OptionParser
sys.path.append('/home/veveo/release/server')
import StringUtil

def initialize_logger(instance):
    f_name = '%s_server.log'%instance
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', filename=f_name)
    logger = logging.getLogger()
    return logger

def load_gid_title_map(logger, veveo_genre_list):
    gid_title_map = {}
    if not os.path.exists(veveo_genre_list):
        logger.warning("veveo genre list not persent")
        return gid_title_map
    f = open(veveo_genre_list)
    for line in f:
        ty, title, gid = line.strip().split('\t')
        gid_title_map[gid.lower()] = [title, ty] 
    return gid_title_map   

def append_gid_with_title(gid_string, gid_to_title_map):
    new_gid_list = []
    if not gid_string:
        return ''
    gids = gid_string.strip().split('<>')
    for gid in gids:
        title, ty = gid_to_title_map.get(gid, ['',''])
        new_gid_list.append('%s{%s}'%(title, gid))
    new_gids = '<>'.join(new_gid_list)
    return new_gids

def get_attr_value(record, schema, attr):
    value = ''
    if attr in schema:
        return record[schema[attr]]    

def set_attr_value(record, schema, attr, new_val):
    if attr in schema:
        record[schema[attr]] = new_val  

def update_program_mc_hash(archival_program_mc_hash, program_mc_schema, genre_gid_title_map):
    record_gid_title_map = {}
    for gid in archival_program_mc_hash:
        record = archival_program_mc_hash[gid]
        Gg = get_attr_value(record, program_mc_schema, 'Gg')
        new_Gg = append_gid_with_title(Gg, genre_gid_title_map)
        set_attr_value(record, program_mc_schema, 'Gg', new_Gg)
        Ge = get_attr_value(record, program_mc_schema, 'Ge')
        new_Ge = append_gid_with_title(Ge, genre_gid_title_map)
        set_attr_value(record, program_mc_schema, 'Ge', new_Ge)
        Sg = get_attr_value(record, program_mc_schema, 'Sg')
        new_Sg = append_gid_with_title(Sg, genre_gid_title_map)
        set_attr_value(record, program_mc_schema, 'Sg', new_Sg)
        Su = get_attr_value(record, program_mc_schema, 'Su')
        new_Su = append_gid_with_title(Su, genre_gid_title_map)
        set_attr_value(record, program_mc_schema, 'Su', new_Su)
        # creating record gid title map
        Ti = record[1]
        record_gid_title_map[gid] = Ti
    return record_gid_title_map 
    
def load_marshal(logger, marshal_file):
    if not os.path.exists(marshal_file):
        logger.warning("%s file doesn't exist"%marshal_file)
        sys.exit()
    marshal_f = open(marshal_file, 'r')
    logger.info("Loading marshal file: %s"%marshal_file)
    (archival_program_mc_hash, program_mc_schema, archival_il_hash, archival_max_score_hash, archival_similar_hash) = marshal.load(marshal_f)
    #load genre_gid_map
    logger.info("Loading genre gid map: veveo_genre_list")
    genre_gid_title_map = load_gid_title_map(logger, 'genre_list')
    #update archival program_mc_hash with genre title
    logger.info("Updating program_mc_hash genres with title")
    record_gid_title_map = update_program_mc_hash(archival_program_mc_hash, program_mc_schema, genre_gid_title_map)
    logger.info("Loading marshal Done.")
    print ("Loading marshal Done")
    return record_gid_title_map, archival_program_mc_hash, program_mc_schema, archival_similar_hash

def search_keyword(keyword, record_gid_title_map):
    suggestions = []
    keyword = StringUtil.cleanString(keyword)
    for gid, title in record_gid_title_map.iteritems():
        title = StringUtil.cleanString(title)
        if keyword in title:
            suggestions.append((gid, title))
    return suggestions

def filter_programs(logger, keyword, record_gid_title_map, archival_program_mc_hash, program_mc_schema):
    genres = keyword.split(',')   
    genre_list = [] 
    for genre in genres:
        if StringUtil.cleanString(genre):
            genre_list.append(StringUtil.cleanString(genre))
    print genre_list        
    suggestions = []
    for gid, title in record_gid_title_map.iteritems():
        if gid not in archival_program_mc_hash:
            continue
        ge = archival_program_mc_hash[gid][program_mc_schema['Ge']]
        ge = StringUtil.cleanString(ge)        
        sg = archival_program_mc_hash[gid][program_mc_schema['Sg']]
        sg = StringUtil.cleanString(sg)
        all_ge = ge+' '+sg
        for genre in genre_list:
            if genre in all_ge:
                suggestions.append((gid, title))
                break
    return suggestions        

def open_socket(record_gid_title_map, archival_program_mc_hash, program_mc_schema, archival_similar_hash, port, logger):
    logger.info("starting socket at port %s"%port)
    s = socket.socket()         # Create a socket object
    host = socket.gethostname() # Get local machine name
    s.bind((host, port))        # Bind to the port

    s.listen(5)                 # Now wait for client connection.
    while True:
        c, addr = s.accept()     # Establish connection with client.
        #print 'Got connection from', addr
        query = c.recv(1024)
        logger.info('Addr: %s, Query: %s'%(addr, query))
        gid, query_type = query.split('|')
        response = ''
        if query_type == 'related':
            logger.info("related query %s"%gid)
            response = archival_similar_hash.get(gid, [])
        elif query_type == 'metaInfo':
            logger.info("metaInfo query :%s"%gid)
            response = archival_program_mc_hash.get(gid, [])
        elif query_type == 'search':
            logger.info("search query :%s"%gid)
            response = search_keyword(gid, record_gid_title_map)
        elif query_type == 'filter':
            logger.info("filter geners :%s"%gid)
            response = filter_programs(logger, gid, record_gid_title_map, archival_program_mc_hash, program_mc_schema)
        elif query_type == 'schema':
            response = program_mc_schema         
        logger.debug("result: %s"%response)
        d = marshal.dumps(response)
        c.sendall(d)
        c.close()                # Close the connection
    

def main():
    parser = OptionParser()
    parser.add_option('-m', '--marshal-file', default=None, help='Marshal file containg metadata information and related information')
    parser.add_option('-d', '--data-type', default=None, help='movie, tvseries etc')
    parser.add_option('-i', "--instance", default=None, help="any name")
    parser.add_option('-p', "--port", default=None, help="any port number to open the socket")
    #Parse options
    options, args = parser.parse_args()
    # initialize loggin
    logger = initialize_logger(options.instance) 
    #loading marshal file #Gids metadata and related
    record_gid_title_map, archival_program_mc_hash, program_mc_schema, archival_similar_hash = load_marshal(logger, options.marshal_file)
    #open socket
    open_socket(record_gid_title_map, archival_program_mc_hash, program_mc_schema, archival_similar_hash, int(options.port), logger)

if __name__ == '__main__':
    main()	

