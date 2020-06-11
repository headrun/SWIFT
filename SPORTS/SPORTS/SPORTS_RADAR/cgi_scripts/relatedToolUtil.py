#!/usr/bin/env python

import os,sys
import optparse, logging
import socket

sys.path.insert(0, "/home/veveo/release/server")

from vtv_utils import get_compact_traceback, find_and_kill_process, execute_shell_cmd

class RelatedTool:
    def __init__(self, options, logger):
        self.logger = logger
        self.confFile = 'ToolInstances.cfg'
        self.instance_name = options.instance_name
        self.movie_marshal_file = options.movie_marshal_data
        self.tv_marshal_file = options.tv_marshal_data
        self.ip_address = socket.gethostbyname(socket.gethostname())
        self.used_port = set()
        self.instance_hash = self.readConfigFile(self.confFile)
        self.instance_hash = self.updateConfig()

    def updateConfig(self):  
        f = open(self.confFile, 'w')
        new_instance_hash = {}
        if not self.instance_hash:
            return new_instance_hash
        for instance in self.instance_hash:
            port, marshal_file = self.instance_hash[instance]
            name, ty = instance.split('#')
            if self.isUsedPort(port):
                f.write('%s\t%s\t%s\t%s\n'%(ty, name, port, marshal_file))
                new_instance_hash[instance] = [port, marshal_file]
                self.used_port.add(port)
        f.close()
        return new_instance_hash        
    
    def readConfigFile(self, file_name):
        instance_hash = {}
        if not os.path.exists(file_name):
            return
        f = open(file_name)
        for line in f:
            line = line.strip()
            if not line:
                continue
            ty, name, port, marshal = line.split('\t') #port = movie:1234<>tvseries:2345
            if not name.strip() or not port.strip():
                self.logger.info("some problem in config line : %s"%line)
                continue
            key = "%s#%s"%(name.strip(), ty.strip())    
            instance_hash[key] = [port.strip(), marshal.strip()]
        return instance_hash       

    def isUsedPort(self, port):
        cmd = 'lsof -i tcp:%s'%port
        status, output = execute_shell_cmd(cmd, self.logger)
        if status:
            return False
        if not output:
            return False
        else:
            return True

    def startInstance(self):
        # start the server
        if self.movie_marshal_file:
            if os.path.exists(self.movie_marshal_file):
                self.startMovieServer()
            else:
                print "Provided Movie Marshal file doesn't exist."
        if self.tv_marshal_file:
            if os.path.exists(self.tv_marshal_file):
                self.startTVServer()
            else:    
                print "Provided TV Marshal file doesn't exist"
        if not self.movie_marshal_file and not self.tv_marshal_file:
            self.logger.warning("No marshal file provided :( ")
            sys.exit()    
        print "You can use client as %s/cgi-bin/similar_client.py?instance=%s"%(self.ip_address, self.instance_name)

    def getAvailPort(self):
        port = 12345
        while str(port) in self.used_port:
            port += 1
        return port

    def startMovieServer(self):
        key = '%s#movie'%self.instance_name
        if key in self.instance_hash:
            string = "Instance with name %s for movie is already UP. Check config file ToolInstances.cfg"%self.instance_name
            print string
            self.logger.info(string)       
            return
        port = self.getAvailPort()
        cmd = "python server.py --port %s --instance %s --data-type movie --marshal-file %s &"%(port, self.instance_name, self.movie_marshal_file)
        print "Running command: %s"%cmd
        status = os.system(cmd)
        if not status:
            self.instance_hash[self.instance_name]= [port, self.movie_marshal_file]
            self.used_port.add(str(port))
            print "Movie server up at port %s"%port
            print "You can use client as %s/cgi-bin/simmovie_client.py?port=%s"%(self.ip_address, port)
            f = open(self.confFile,'a')
            f.write('%s\t%s\t%s\t%s\n'%('movie', self.instance_name, port, self.movie_marshal_file))     
            f.close()
        else:
            print "Error in stating movie server tool"
            sys.exit()

    def startTVServer(self):
        key = '%s#tv'%self.instance_name
        if key in self.instance_hash:
            string = "Instance with name %s for tv is already UP. Check config file ToolInstances.cfg"%self.instance_name
            print string
            self.logger.info(string)       
            return
        port = self.getAvailPort()
        cmd = "python server.py --port %s --instance %s --data-type tvseries --marshal-file %s &"%(port, self.instance_name, self.tv_marshal_file)
        print "Running command: %s"%cmd
        status = os.system(cmd)
        if not status:
            self.instance_hash[self.instance_name]= [port, self.tv_marshal_file]    
            self.used_port.add(str(port))
            print "TVseries server up at port %s"%port
            print "You can use client as %s/cgi-bin/simtv_client.py?port=%s"%(self.ip_address, port)
            f = open(self.confFile,'a')
            f.write('%s\t%s\t%s\t%s\n'%('tv', self.instance_name, port, self.tv_marshal_file))
            f.close()
        else:
            print "Error in stating tvseries server tool"
            sys.exit()

    def stopInstance(self):
        key1 = '%s#%s'%(self.instance_name, 'movie')
        key2 = '%s#%s'%(self.instance_name, 'tv')
        keys = []
        if key1 in self.instance_hash:
            keys.append(key1)        
            #ports.append(self.instance_hash[key1][0])
        if key2 in self.instance_hash:
            keys.append(key2)
            #ports.append(self.instance_hash[key2][0])
        if not keys:
            string = "Instance with name %s is already DOWN. Check config file ToolInstances.cfg"%self.instance_name
            print string
            self.logger.info(string)
            return
        if keys:
            proc_name = "'instance %s '"%(self.instance_name)
            find_and_kill_process(proc_name, self.logger, print_error = 0, sleep_time=1)
            for key in keys:
                del self.instance_hash[key]
        self.updateConfig()       
        print "Successfully Stoped" 

def initialize_logger():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', filename='relatedToolUtil.log')
    logger = logging.getLogger()
    return logger 

def main(logger, options):
    rt = RelatedTool(options, logger)
    if options.action == 'DOWN':
        rt.stopInstance()
    elif options.action == 'UP':
        rt.startInstance()
    else:
        print "I don't know what to do :( . I just work with 'UP' or 'DOWN'"
        sys.exit()

if __name__ == '__main__':
    #Define options
    default_logger = os.path.splitext(sys.argv[0])[0] + os.path.extsep + 'log'
    parser = optparse.OptionParser()
    parser.add_option('-i', '--instance-name', default=None, help='Any instance name which is already not running. e.g data_20130619')
    parser.add_option('-m', '--movie-marshal-data', default=None, help='movie marshal data location')
    parser.add_option('-t', '--tv-marshal-data', default=None, help='tvseries marshal data location')
    parser.add_option('-a', '--action', default=None, help='DOWN or UP')


    logger = initialize_logger()
    #Parse options
    options, args = parser.parse_args()
    if not options.action or not options.instance_name:
        err_str = "Either action or instance-name not provided. Please provide both."
        logger.info(err_str)
        print err_str 
        sys.exit()
    #Call main function to generate output
    try:
        logger = initialize_logger()
        main(logger, options)
    except Exception, e:
        print "Exception: %s" % get_compact_traceback(e)
        sys.exit(2)
