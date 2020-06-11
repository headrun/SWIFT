#!/usr/bin/env python
import sys
import optparse
import time
import datetime
import MySQLdb



def main():
    import pdb;pdb.set_trace()
    conn = MySQLdb.connect(host = '10.4.15.132', user = 'root', db = 'SPORTSDB_BKP').cursor()
