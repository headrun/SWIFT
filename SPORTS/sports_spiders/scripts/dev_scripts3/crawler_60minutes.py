#!/usr/bin/env python
import sys
import optparse
import urllib,os,string

import util.smeagoline.crawlengine as crawlengine
from dbuniverse.dbserver.utils import DotAccessDict

import re
get_show_id = re.compile('\d+')
def generate_urls(options, terminal_dir ,browse_dir ):
    urls = []
    url = 'http://60minutes.yahoo.com/'
    url = crawlengine.TopLevelCrawlRequest(url)
    page_store_dir = terminal_dir+"/"
    if not os.access(page_store_dir, os.F_OK):
        os.system('mkdir -p %s'%page_store_dir)
    error_dir = page_store_dir+"/"+"error_dir"    
    if not os.access(error_dir, os.F_OK):
        os.system('mkdir -p %s'%error_dir)
    url.arguments = {'store_dir':page_store_dir, 'error_dir':error_dir, 'postproc_file':options.postproc_file}
    urls.append(url)

    return urls

def main(options):
    terminal_dir = options.output_dir+"/terminal_pages/"
    browse_dir = options.output_dir+"/browse_pages/"
    for dir in [terminal_dir, browse_dir]:
        if not os.access(dir, os.F_OK):
          os.system('mkdir -p %s'%dir)
 
    urls = generate_urls(options, terminal_dir ,browse_dir )
    # Crawl
    xsls = ["60minutes_top.xsl.py","60minutes_details.xsl.py"]
    crawl_options = DotAccessDict()
    crawl_options.dbserver_address = options.address
    crawl_options.outfile = options.out_file
    crawl_options.timeout = 100
    crawl_options.retries = 10
    #crawl_options.tidy = None
    crawl_options.schema_name = 'webclip'
    try:
        crawlengine.crawl(urls, xsls, '60minutes', options.schema_dir, options=crawl_options)
    except Exception, e:
        print "ERROR OCCURED IN MAIN ",e

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option("-a", "--address", metavar="IP:PORT",
                      default=None, help="Address of the dbserver to pump records to")
    parser.add_option("-o", "--out-file", metavar="FILE",
                      default=None, help="File to write records into")

    parser.add_option("-i", "--in-file",
                      default=None, help="query out put")

    parser.add_option("-n", "--output-dir",
                      default=None, help=" output dir")

    parser.add_option("", "--schema-dir", metavar="PATH",
                      default=None, help="Schema directory")

    parser.add_option("-p","--postproc-file",default=None,
                      help="file to writeout postprocessing data")

    parser.add_option("-q","--queries-file",default=None,
                      help=" file containing querites to update mysql table rows")

    (options, args) = parser.parse_args()

    main(options)
 
