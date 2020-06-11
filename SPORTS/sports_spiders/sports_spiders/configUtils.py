"""
Utilities to read and process configuration files.
$Id: configUtils.py,v 1.15 2015/02/10 07:01:47 akshat Exp $
"""

import sys
import os
from configparser import ConfigParser


class VtvConfigParser:
    def __init__(self, myglobals, config_file, default_section=None):
        self.myglobals = myglobals
        self.conf_file = config_file
        self.sections = {}
        self.attribs = {}
        # if specified and no section is given during get_conf_value, use this section
        self.default_section = default_section

        self.parse_config()

    def set_default_section(self, default_section):
        self.default_section = default_section

    def parse_config(self):
        parser = ConfigParser.SafeConfigParser()
        parser.readfp(open(self.conf_file))
        self.sections = dict.fromkeys(parser.sections())
        for section in self.sections.iterkeys():
            self.sections[section] = dict(parser.items(section))
            self.attribs.update(dict(parser.items(section)))

    def override_config(self, new_config_file):
        parser = ConfigParser.ConfigParser()
        parser.readfp(open(new_config_file))
        for section in parser.sections():
            section_dict = dict(parser.items(section))
            if section not in self.sections:
                self.sections[section] = section_dict
            else:
                self.sections[section].update(section_dict)
            self.attribs.update(section_dict)

    def get_conf_value(self, key, default_value=None, section=None):
        key = key.lower()
        section = section or self.default_section
        if section:
            section_config = self.sections.get(section, None)
            if section_config:
                return section_config.get(key, default_value)
        else:
            return self.attribs.get(key, default_value)

        return default_value

    def set_conf_value(self, key, value, section=None):
        if section:
            d = self.sections.setdefault(section, {})
            d[key] = value

        self.attribs[key] = value

    def get_conf_bool(self, key, default='false', section=None):
        return self.get_conf_value(key, default, section).lower() == 'true'

    def get_conf_int(self, key, default='0', section=None):
        temp = self.get_conf_value(key, default, section)
        return int(temp)

    def override_section_a_with_section_b(self, section_a, section_b):
        '''
        (k, v) in section_a is replaced with (k, v) in section_b
        '''
        if section_a == section_b:
            return
        section_a_dict = self.sections.get(section_a, None)
        section_b_dict = self.sections.get(section_b, None)

        if not section_a_dict or not section_b_dict:
            return

        self.sections[section_a].update(section_b_dict)
        self.attribs.update(section_b_dict)


class VtvConfigWrapper:
    def __init__(self, config_obj):
        self.config = config_obj

    def getConfValue(self, var, default=None, section=None):
        var = var.lower()
        val = self.config.get_conf_value(var, default, section)
        return val

    def getConfBool(self, key, default='false', section=None):
        return self.getConfValue(key, default, section).lower() == 'true'

    def getConfInt(self, key, default='0', section=None):
        temp = self.getConfValue(key, default, section)
        return int(temp)

    def getConfThreshold(self, key, default='-1', section=None):
        return self.getConfInt(key, default, section)
        threshold = -1
        temp = self.getConfValue(key, default, section)
        if temp and temp.isdigit():
            threshold = int(temp)
        return threshold

    def getConfDelimitedList(self, key, delim=',', default='', section=None):
        temp = self.getConfValue(key, default, section).strip().split(delim)
        return [x.strip() for x in temp if x and x.strip()]

    def getConfList(self, key, default='[]', section=None):
        temp_str = self.getConfValue(key, default, section)
        try:
            temp = eval(temp_str)
        except SyntaxError:
            temp = []
        return temp

    def setConfValue(self, key, value, section=None):
        key = key.lower()
        self.config.set_conf_value(key, value, section)

    def getSectionKeys(self, section):
        return self.config.sections.get(section, {}).keys()


def readConfFile(conf_file, logger=None, conf_val=None):
    if conf_val == None:
        conf_val = {}
    # read the config file
    try:
        conf = open(conf_file, 'r')
    except IOError:
        err_msg = 'Configuration file: %s not present :(' % conf_file
        if logger:
            print(err_msg)
            logger.error(err_msg)
        else:
            print('ERROR: ' + err_msg)
        sys.exit(-1)
    for line in conf:
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        var, val = line.split('=', 1)
        conf_val[var.strip()] = val.strip()
    return conf_val


def checkAndCreateDirectory(path):
    try:
        if not os.path.isdir(path):
            os.makedirs(path)
    except:
        return False
    return True


if __name__ == '__main__':
    obj = VtvConfigParser(None, os.path.join('../../config/music_publish.cfg'))
    print(obj.sections)
    print(obj.attribs)
    print(obj.get_conf_value('name'))
