
import scrapy
import logging
import random
#import netifaces
#from netifaces import AF_INET 
from scrapy import signals
from scrapy.conf import settings
from w3lib.http import basic_auth_header
proxy_list = ['fl.east.usa.torguardvpnaccess.com', 'atl.east.usa.torguardvpnaccess.com', 'ny.east.usa.torguardvpnaccess.com', 'chi.central.usa.torguardvpnaccess.com', 'dal.central.usa.torguardvpnaccess.com', 'la.west.usa.torguardvpnaccess.com', 'lv.west.usa.torguardvpnaccess.com', 'sa.west.usa.torguardvpnaccess.com', 'nj.east.usa.torguardvpnaccess.com', 'central.usa.torguardvpnaccess.com','centralusa.torguardvpnaccess.com','west.usa.torguardvpnaccess.com','westusa.torguardvpnaccess.com','east.usa.torguardvpnaccess.com','eastusa.torguardvpnaccess.com']+ ['au.torguardvpnaccess.com', 'melb.au.torguardvpnaccess.com', 'bul.torguardvpnaccess.com', 'cp.torguardvpnaccess.com', 'egy.torguardvpnaccess.com', 'iom.torguardvpnaccess.com', 'isr.torguardvpnaccess.com', 'fin.torguardvpnaccess.com', 'br.torguardvpnaccess.com', 'ca.torguardvpnaccess.com', 'vanc.ca.west.torguardvpnaccess.com', 'frank.gr.torguardvpnaccess.com', 'ice.torguardvpnaccess.com', 'ire.torguardvpnaccess.com', 'in.torguardvpnaccess.com', 'jp.torguardvpnaccess.com', 'nl.torguardvpnaccess.com', 'lon.uk.torguardvpnaccess.com', 'ro.torguardvpnaccess.com', 'ru.torguardvpnaccess.com', 'mos.ru.torguardvpnaccess.com', 'swe.torguardvpnaccess.com', 'swiss.torguardvpnaccess.com', 'bg.torguardvpnaccess.com', 'hk.torguardvpnaccess.com', 'cr.torguardvpnaccess.com', 'hg.torguardvpnaccess.com', 'my.torguardvpnaccess.com', 'thai.torguardvpnaccess.com', 'turk.torguardvpnaccess.com', 'tun.torguardvpnaccess.com', 'mx.torguardvpnaccess.com', 'singp.torguardvpnaccess.com', 'saudi.torguardvpnaccess.com', 'fr.torguardvpnaccess.com', 'pl.torguardvpnaccess.com', 'czech.torguardvpnaccess.com', 'gre.torguardvpnaccess.com', 'it.torguardvpnaccess.com', 'sp.torguardvpnaccess.com', 'no.torguardvpnaccess.com', 'por.torguardvpnaccess.com', 'za.torguardvpnaccess.com', 'den.torguardvpnaccess.com', 'vn.torguardvpnaccess.com', 'sk.torguardvpnaccess.com', 'lv.torguardvpnaccess.com', 'lux.torguardvpnaccess.com', 'nz.torguardvpnaccess.com', 'md.torguardvpnaccess.com', 'uae.torguardvpnaccess.com', 'slk.torguardvpnaccess.com', 'fl.east.usa.torguardvpnaccess.com', 'atl.east.usa.torguardvpnaccess.com', 'ny.east.usa.torguardvpnaccess.com', 'chi.central.usa.torguardvpnaccess.com', 'dal.central.usa.torguardvpnaccess.com', 'la.west.usa.torguardvpnaccess.com', 'lv.west.usa.torguardvpnaccess.com', 'sa.west.usa.torguardvpnaccess.com', 'nj.east.usa.torguardvpnaccess.com', 'central.usa.torguardvpnaccess.com', 'centralusa.torguardvpnaccess.com', 'west.usa.torguardvpnaccess.com', 'westusa.torguardvpnaccess.com', 'east.usa.torguardvpnaccess.com', 'eastusa.torguardvpnaccess.com']

round_robin_iplist = [ "eth0:0", "eth0:1", "eth0:2", "eth0:3", "eth0:4", "eth0:5", "eth0:6", "eth0:7", "eth0:8", "eth0:9", "eth0:10", "eth0:11", "eth0:12", "eth0:13", "eth0:14", "eth0:15", "eth0:16", "eth0:17", "eth0:18", "eth0:19", "eth0:20", "eth0:21", "eth0:22", "eth0:23", "eth0:24", "eth0:25", "eth0:26", "eth0:27", "eth0:28", "eth0:29", "eth0:30"]

class InterfaceRoundRobinMiddleware(object):
    def process_request(self, request, spider):
        vir_itf_count = 30
        ether = [item for item in netifaces.interfaces() if ':' in item][0].split(':')[0]
        round_robin_ip = netifaces.ifaddresses('%s:%d'%(ether, random.randrange(0,vir_itf_count)))[AF_INET][0]['addr']
        request.meta["bindaddress"]= ("127.0.0.1",random.randrange(49152,65535))
        ip_port_tuple = ( round_robin_ip , random.randrange(49152,65535) )
        request.meta["bindaddress"]= ip_port_tuple
        logging.warning("request bindaddress ip_tuple = ('%s','%s')"%ip_port_tuple)
        return None

    def process_response(self, request, response, spider):
        return response

    def process_exception(self, request, exception, spider):
        return None

class ProxyMiddleware(object):
   def process_request(self, request, spider):
       proxy = random.choice(proxy_list)
       request.meta['proxy'] = 'http://'+ proxy+':6060'
       request.headers['Proxy-Authorization'] = basic_auth_header('vinuthna@headrun.com','Hotthdrn591!')
