import re
import csv
from hashlib import md5
from itertools import chain
from datetime import datetime
from os import path, getcwd, mkdir, system
from scrapy.utils.project import get_project_settings
SETTINGS = get_project_settings()

OUTPUT_DIR = path.join(getcwd(), 'OUTPUT')
CSV_FILES_DIR = path.join(OUTPUT_DIR, 'crawl_out')
CSV_FILES_PROCESSING_DIR = path.join(OUTPUT_DIR, 'processed')

def create_default_dirs():
    default_dirs = ['crawl_out', 'processed']
    if not path.exists(OUTPUT_DIR):
        mkdir(OUTPUT_DIR)

    for _dir in default_dirs:
        _dir = path.join(OUTPUT_DIR, _dir)
        if not path.exists(_dir):
            mkdir(_dir)

def get_csv_file(name):
    name = name.split('_')[0]
    csv_filename = path.join(CSV_FILES_DIR, "%s_csv_%s.csv" % (name, get_current_ts_with_ms()))
    csv_file = open(csv_filename, 'w')
    csv_writer = csv.writer(csv_file)
    return csv_writer, csv_file

def get_current_ts_with_ms():
    dt = datetime.now().strftime("%Y%m%dT%H%M%S%f")
    return dt

def xcode(text, encoding='utf8', mode='strict'):
    return text.encode(encoding, mode) if isinstance(text, str) else text

def encode_md5(x):
    return md5(xcode(x)).hexdigest()

def move_file(source, dest):
    cmd = "mv %s %s" % (source, dest)
    system(cmd)

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

    compacted = re.sub(r"\s\s(?m)", " ", text)
    if compacted != text:
        compacted = compact(compacted, level+1)

    return compacted.strip()

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

def extract(sel, xpath, sep=' '):
    return clean(compact(textify(sel.xpath(xpath).extract(), sep)))

def extract_data(data, path, delem=''):
    return delem.join(i.strip() for i in data.xpath(path).extract() if i).strip()

def extract_list_data(data, path):
    return data.xpath(path).extract()

def get_nodes(data, path):
    return data.xpath(path)
