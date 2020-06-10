from os import path, getcwd, mkdir, system
from datetime import datetime
from MySQLdb import connect
from scrapy.utils.project import get_project_settings


OUTPUT_DIR = path.join(getcwd(), 'OUTPUT')
QUERY_FILES_DIR = path.join(OUTPUT_DIR, 'crawl_out')
QUERY_FILES_PROCESSING_DIR = path.join(OUTPUT_DIR, 'processing')

SETTINGS = get_project_settings()

MYSQL_CONNECT_TIMEOUT_VALUE = 30
DB_HOST = SETTINGS['DB_HOST']
DB_UNAME = SETTINGS['DB_USERNAME']
DB_PASSWD = SETTINGS['DB_PASSWORD']
URLQ_DB_NAME = SETTINGS['URLQ_DATABASE_NAME']
LOGS_DIR = SETTINGS['LOGS_DIR']
CONN, CURSOR = None, None

def get_mysql_connection(server=DB_HOST, db_name=URLQ_DB_NAME):
    try:
        conn = connect(host=server, user=DB_UNAME, passwd=DB_PASSWD, db=db_name, charset="utf8", use_unicode=True)
        #if db_name: conn.autocommit(True)
        cursor = conn.cursor()

    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        conn, cursor = None, None

    return conn, cursor

def get_urlq_cursor():
    global CONN
    global CURSOR
    if CURSOR is not None: return CONN, CURSOR
    CONN, CURSOR = get_mysql_connection(db_name=URLQ_DB_NAME)
    return CONN, CURSOR

def update_urlqueue_with_resp_status(source_name, crawl_status, sk):
    table_name = '%s_crawl' % source_name
    delete_query = 'DELETE FROM %s WHERE crawl_status=%s AND sk = "%s"'
    update_query = 'UPDATE %s SET crawl_status=%s, modified_at=NOW() WHERE sk="%s"'

    CONN, CURSOR = get_urlq_cursor()
    CURSOR.execute(delete_query % (table_name, crawl_status, sk))
    CURSOR.execute(update_query % (table_name, crawl_status, sk))
    CONN.commit()

def close_mysql_connection():
    if CURSOR is not None: CURSOR.close()
    if CONN is not None: CONN.close()

def create_default_dirs():
    default_dirs = ['crawl_out', 'processing', 'processed', 'un-processed']
    if not path.exists(OUTPUT_DIR):
        mkdir(OUTPUT_DIR)

    for _dir in default_dirs:
        _dir = path.join(OUTPUT_DIR, _dir)
        if not path.exists(_dir):
            mkdir(_dir)

def get_queries_file(name):
    queries_filename = path.join(QUERY_FILES_DIR, "%s_%s.queries" % (name, get_current_ts_with_ms()))
    queries_file = open(queries_filename, 'w')

    return queries_file

def get_current_ts_with_ms():
    dt = datetime.now().strftime("%Y%m%dT%H%M%S%f")
    return dt

def extract(sel, xpath, sep=' '):
    return clean(compact(textify(sel.xpath(xpath).extract(), sep)))

def extract_data(data, path, delem=''):
    return delem.join(i.strip() for i in data.xpath(path).extract() if i).strip()

def extract_list_data(data, path):
    return data.xpath(path).extract()

def get_nodes(data, path):
    return data.xpath(path)

def textify(nodes, sep=' '):
    if not isinstance(nodes, (list, tuple)):
        nodes = [nodes]

    def _t(x):
        if isinstance(x, (str, str)):
            return [x]

        if hasattr(x, 'xmlNode'):
            if not x.xmlNode.get_type() == 'element':
                return [x.extract()]
        else:
            if isinstance(x.root, (str, str)):
                return [x.root]

        return (n.extract() for n in x.select('.//text()'))

    nodes = chain(*(_t(node) for node in nodes))
    nodes = (node.strip() for node in nodes if node.strip())

    return sep.join(nodes)

def clean(text):
    if not text:
        return text

    value = text
    value = re.sub("&amp;", "&", value)
    value = re.sub("&lt;", "<", value)
    value = re.sub("&gt;", ">", value)
    value = re.sub("&quot;", '"', value)
    value = re.sub("&apos;", "'", value)
    value = re.sub("              ", " ", value)

    return value

def compact(text, level=0):
    if text is None:
        return ''

    if level == 0:
        text = text.replace("\n", " ")
        text = text.replace("\r", " ")

    compacted = re.sub("\s\s(?m)", " ", text)
    if compacted != text:
        compacted = compact(compacted, level+1)

    return compacted.strip()

def move_file(source, dest):
    cmd = "mv %s %s" % (source, dest)
    system(cmd)

