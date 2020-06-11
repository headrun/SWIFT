# -*- coding: utf-8 -*-
import time
import json
import re
date_pat = re.compile("(\d{4}-\d{1,2}-\d{1,2})")

OD_PATTERN = { '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$' : ('%Y-%m-%d', ),
               '(^[0-9]{4})-[0-9]{1,2}-[0-9]{1,2}': ('%Y-%m-%d', ),
               '^\w+ [0-9]{1,2}, ([0-9]{4})$'     : ('%B %d, %Y', '%b %d, %Y'),
               '^[0-9]{1,2} \w+ ([0-9]{4})$'      : ('%d %B %Y', '%d %b %Y'),
               '^[0-9]{1,2}\w+ ([0-9]{4})$'       : ('%d%B %Y', '%d%b %Y'),
               '(^[0-9]{4})-[0-9]{1,2}$'          : ('%Y-%m',),
               '(^[0-9]{4})$'                     : ('%Y', ),
               '\w+ ([0-9]{4})'                   : ('%B %Y', '%b%Y'),
             }

class Wiki:
    def __init__(self):
        self.infobox = file("infobox.json", "r+").readlines()
        self.sports_package_data = open('sports_package_data2', 'w')
        self.exception_file = open("exception", "w")
        #self.founded_data = open('sports_founded_data1', 'w')

    def process_date(self, item):
        for key, value in OD_PATTERN.iteritems():
            date_string = re.findall(key, item)

            if not date_string:
                continue
            for date_tuple in value:
                try:
                    pgm_date = time.strptime(item, date_tuple)
                    program_date = '%s-%s-%s' % (pgm_date.tm_year, pgm_date.tm_mon, pgm_date.tm_mday)
                    return program_date
                except:
                    pass
            else:
                print item

    def main(self):
        for line in self.infobox:
            line    = json.loads(line)
            key     = line.keys()[0]
            if_data = line[key]['If']
           
            if not isinstance(if_data, dict): continue

            if if_data.has_key('infobox football club'):
                main_data = if_data['infobox football club']
                founded = ''
                if main_data.has_key('founded'):
                    try:
                        if isinstance(main_data['founded'], unicode):
                            founded = main_data['founded']
                        elif main_data['founded'].has_key('start date and years ago'):
                            founded = main_data['founded']['start date and years ago']
                        elif main_data['founded'].has_key('start date and age'):
                            founded = main_data['founded']['start date and age']
                    except:
                        pass

                    if founded:
                        if isinstance(founded, list):
                            founded = founded[0][0]

                if main_data.has_key('ground'):
                    stadium  = main_data['ground']


                if main_data.has_key('nickname'):
                    nickname = main_data['nickname']
                
                record = {}
                record[key] =  { 'founded': founded,
                                 'stadium': stadium,
                                 'nickname': nickname}

                if stadium:
                    self.get_stadium(stadium)

                date = ''
                if isinstance(founded, dict):
                    founded = self.get_formed(founded)
                if isinstance(founded, unicode) or isinstance(founded, str):
                    date = self.process_date(founded)
                if date:
                    try:
                        self.sports_package_data.write('%s<>%s\n' %(key, date))
                    except:
                        pass

    def get_stadium(self, data):
        if isinstance(data, list):
            print data[0]

    def get_formed(self, data):
        dt_list = data.values()
        year    = data.get('1', '')
        month   = data.get('2', '')
        date_   = data.get('3', '')

        if year and month and date_:
            formed = '%s-%s-%s' % (year, month, date_)
        elif year and month:
            formed = '%s-%s-01' % (year, month)
        elif year:
            formed = '%s-01-01' % year
        return formed

if __name__ == "__main__":
    Wiki().main()
