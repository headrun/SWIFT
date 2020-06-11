# -*- coding: utf-8 -*-
import sys
import time
import os
import json
import jinja2
import codecs
reload(sys)
sys.setdefaultencoding('utf-8')
from collections import OrderedDict
from data_report import VtvHtml
from vtv_utils import copy_file
from vtv_task import VtvTask, vtv_task_main
from vtv_dir import VtvDataDir
from vtv_db import get_mysql_connection
from ssh_utils import scp
import datetime
import subprocess
DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'

class CheckDuplicateInfo(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name    = "SPORTSDB"
        self.db_ip      = "10.28.218.81"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                      self.db_name, cursorclass='',
                                                      user = 'veveo', passwd='veveo123')
        self.obj_map_dict = OrderedDict()
        self.mo_details = {}
        self.matchreport = {}
        self.guid = {}
        self.tgids = {}
        self.jrep = []
        self.final = []
        self.lis = []
        self.repo = []

    def get_rosters(self,tid):
        db_datas = []
        
        sel_qry = 'select SP.id,SP.title from sports_participants SP ,sports_teams ST where SP.id = ST.participant_id and ST.tournament_id = %s'
        values = (int(tid))
        self.cursor.execute(sel_qry,values)
        db_data = self.cursor.fetchall()
        for db_ in db_data:
            db_id = str(db_[0])
            db_title = db_[1]
            db_data1 = db_id + "<>" + db_title
            db_datas.append(db_data1) 
        self.json_rosters(tid,db_datas)

    def get_guidmerge(self):
        _guid = open('guid_merge.list', 'r+')
        for _gui in _guid:
                _gui = _gui.split('<>')
                _guiW = _gui[0]
                _guiG = _gui[1].strip('\n')
                self.guid.setdefault(_guiW,_guiG)

    def json_rosters(self,tid,db_datas):
        tid = int(tid)
        _data = open('roster_json_file_from_template_pages.json')
        dcont = _data.read()
        _guid = open('guid_merge.list', 'r+')
        _data1 = open('roster_json_file_from_article_pages.json')
        dcont1 = _data1.read()
        new_file = open('SingleFile.json',"w")
        new_file.write(dcont + dcont1)
        new_file.close
        
        _data = open('SingleFile.json') 
        #_data = open('roster_json_file_from_template_pages.json', 'r+')
        
        mo_datas = []
        self.wiki_dict = {}
        self.mo_details = {}
        for data in _data:
            try:
                da = json.loads(data)
            except:
                continue
                import pdb;pdb.set_trace()
                print da
            try:
                mo_team = da.values()[0].values()[0]
            except:
                import pdb;pdb.set_trace()
                print mo_team
            print mo_team
            try:
                mo_title =  mo_team.split('{')[0]
            except:
                import pdb;pdb.set_trace()
                print mo_title
            print mo_title
            mo_gid =  mo_team.split('{')[1].strip('}')

            self.mo_details.setdefault(mo_title, mo_gid)
             
            sel_qry = 'select child_gid from GUIDMERGE.sports_wiki_merge where exposed_gid = %s'
            values = (mo_gid)
            self.cursor.execute(sel_qry,values)
            team_gid = self.cursor.fetchall()
            '''startTime = time.time()
            print "Start time for grep"
            print startTime
            hosts_process = subprocess.Popen(['grep',mo_gid + '<>','guid_merge.list'], stdout= subprocess.PIPE)
            hosts_out, hosts_err = hosts_process.communicate()
            try:
                team_gid = hosts_out.split('<>')[1].split('\n')[0]
                self.tgids.setdefault(mo_title,team_gid)
            except:
                continue
            print "end time for grep"
            print time.time() 
            for val in self.guid:
                if val != mo_gid:
                    continue
            
                team_gid = self.guid[val]'''
            
            if not team_gid:
                continue
                print mo_team
            else:
                team_gid = team_gid[0][0]
                sel_qry = 'select T.tournament_id from sports_teams T,sports_participants P where P.id = T.participant_id and P.gid = %s'
                values = (team_gid)
                self.cursor.execute(sel_qry,values)
                mo_touid = self.cursor.fetchall()
                if not mo_touid:
                    continue
                else:
                    mo_touid = int(mo_touid[0][0])
            mo_players = []
            if tid == mo_touid:
                mo_data = mo_title.title()
                if mo_data == "Philadelphia 76Ers":
                    mo_data = "Philadelphia 76ers"
                if mo_data == "Los Angeles Angels":
                    mo_data = "Los Angeles Angels of Anaheim"
                if mo_data == "San Francisco 49Ers":
                    mo_data = "San Francisco 49ers"
                mo_datas.append(mo_data)    
                mo_tempplayers = da.values()[0].values()[2]
                for mo_player in mo_tempplayers:
                    mo_player = mo_player.get(u'name')
                    if len(mo_player) < 3:
                        continue
                    mo_players.append(mo_player)
                for key in self.wiki_dict:
                    if mo_data != key:
                        continue
                    else:
                        for mop in mo_players:
                            self.wiki_dict[key].append(mop)
                self.wiki_dict.setdefault(mo_data, mo_players)
            else:
                continue
        self.matching_rosters(tid,db_datas)
            
    def matching_rosters(self,tid,db_datas):
        self.wiki_match = {}
        self.wiki_mismatch = {}
        self.wiki_match1 = {}
        self.wiki_mismatch1 = {}
        self.source = {}
        self.wikid = {}
        self.dbcount = {}
        _guid = open('guid_merge.list', 'r+')
        f = open('myfile.txt', 'w')
        for key,value in self.wiki_dict.iteritems():
            wiki_gids = []
            self.wiki = {}
            for db_data in db_datas:
                db_data = db_data.split('<>')             
                 
                if key in db_data:
                    tou_id = db_data[0]
                    sel_qry = 'select SP.title,SP.gid from sports_roster SR,sports_participants SP where SP.id = SR.player_id and SR.team_id = %s and status= "active"; '    #roster from the DB
                    values = (tou_id)
                    self.cursor.execute(sel_qry,values)
                    pids = self.cursor.fetchall()
                    self.dbcount.setdefault(key,len(pids))
                    for pid in pids:
                        pidg = pid[1]+ '$'
                        hosts_process = subprocess.Popen(['grep',pidg,'guid_merge.list'], stdout= subprocess.PIPE)
                        hosts_out, hosts_err = hosts_process.communicate()
                        wikigid = hosts_out.strip('\n').split('<>')[0]
                        self.wikid.setdefault(wikigid,pid)
                        if not hosts_out:
                            
                            hosts_process = subprocess.Popen(['grep',pidg,'merge_final_wiki_sport_player.list'], stdout= subprocess.PIPE)
                            hosts_out, hosts_err = hosts_process.communicate()
                            wikigid = hosts_out.split('<>')[0]
                            xcv = wikigid + '<>' + pidg
                            f.write(xcv)
                            f.write('\n')
                            self.wikid.setdefault(wikigid,pid)
                            if not hosts_out:

                                wikigid = pid[1]
                        
                
                            
                        try:
                            wikigid = wikigid + "<>" + pid[0].decode('utf-8')    #wikigid = pid[0] + "<>" + wikigid[0][0]
                        except:
                            wikigid = pid[1] + "<>" + pid[0].decode('utf-8')
                            wiki_gids.append(wikigid)   #'''No GUID MERGE PLAYERS FROM DB'''
                        if not wikigid:
                            continue    
                        wiki_gids.append(wikigid)
                        
                        
                else:
                    continue
            mo_values = []
            self.mo_v = {}
            for val in value:
                va = val.split('{')               
                mo_ti = str(va[0].title()).decode('utf-8')
                mo_gi = str(va[1].strip('}'))
                try:
                    mo_ti = mo_ti.split(' (')
                    mo_ti = mo_ti[0]
                except:
                    mo_ti = mo_ti    
            
                mo_value = mo_gi + "<>" + mo_ti
                mo_values.append(mo_value)
            wgids = []
            mgids = []
            for wgs in wiki_gids:
                wg = wgs.split('<>')
                k = wg[0]
                v = wg[1]
                wgids.append(k)
                self.wiki.setdefault(k,v)


            for wgs in mo_values:
                wg = wgs.split('<>')
                k = wg[0]
                v = wg[1]
                mgids.append(k)
                self.mo_v.setdefault(k,v)
            ros_match = set(wgids) & set(mgids)
            ros_mismatch = set(wgids + mgids) - ros_match
            ros_match = sorted(list(ros_match))
            ros_mismatch = sorted(list(ros_mismatch))
            ros1 = []
            ros2 = []
            for ros in ros_match:
                for k in self.wiki.keys():
                    if k != ros:
                        continue
                    va = self.wiki[k]
                    ros_mat = k + "<>" + va
                    ros1.append(ros_mat)
                self.wiki_match.setdefault(key,ros1)
                for k in self.mo_v.keys():
                    if k != ros:
                        continue
                    va = self.mo_v[k]
                    ros_mat = k + "<>" + va
                    ros2.append(ros_mat)
                self.wiki_match.setdefault(key,ros2)
    
            for ros in ros_mismatch:
                for k in self.mo_v.keys():
                    if k != ros:
                        continue
                    va = self.mo_v[k]
                    ros_mis = k + "<>" + va
                    ros_mismatch.append(ros_mis)
                self.wiki_mismatch.setdefault(key,ros_mismatch)
                for k in self.wiki.keys():
                    if k != ros:
                        continue
                    va = self.wiki[k]
                    ros_mis = k + "<>" + va
                    ros_mismatch.append(ros_mis)
                self.wiki_mismatch.setdefault(key,ros_mismatch)
                
            
            '''ros_match = set(wiki_gids) & set(mo_values)                                        # MATCHING ROSTERS
            ros_mismatch = set(wiki_gids + mo_values) - ros_match
            ros_match = list(ros_match)
            ros_mismatch = list(ros_mismatch)'''
            for ros_mis in ros_mismatch:
                if ros_mis in wiki_gids: 
                    vals = "SPORTSDB"
                    self.source.setdefault(ros_mis,vals)   
                elif ros_mis in mo_values:
                    vals = "WIKI" 
                    self.source.setdefault(ros_mis,vals)
            '''self.wiki_match1.setdefault(key,ros_match)
            self.wiki_mismatch1.setdefault(key,ros_mismatch)'''
        f.close()
    def report(self,tid):
        self.lis = []
        self.matchreport = {}
        self.obj_map_dict = OrderedDict()
        if tid == '229':
            tot= '30'
        if tid == '88':
            tot = '30'
        if tid == '197':
            tot = '32'
        if tid == '240':
            tot = '31'
        if tid == '34':
            tot = '22'
        if tid == '223':
            tot = '12'

        toti = tot
        for key in self.sports:
            if key != tid:
                continue
            spt = self.sports[key]
        headers = ['No.','Matching','id','Title']
        totcount = len(self.wiki_match.keys())
        totcol = self.get_status_color('Success')
        strii = 'Success'
        tot = totcol.replace(strii,str(tot))
        na = 1
        ac = 0
        for key,val in self.wiki_match.iteritems():
            html_obj = VtvHtml()
            title = key
            for mary in self.mo_details:
                key = key.lower()
                if mary != key:
                    continue
                tit_gid = self.mo_details[mary]
            key = key.title()
            for rost in self.wiki_dict:
                if key != rost:
                    continue
                meh = self.wiki_dict[rost]
                meh = set(meh)
                meh = list(meh)
                WKC = len(meh)
            for rost in self.dbcount:
                if key != rost:
                    continue
                DBC = self.dbcount[rost]
            totval = len(val)
            if totval == WKC == DBC:
                ac = ac + 1
            if totval != WKC:
                totvalc = "<font color='red'>%s</font>" % str(totval)
            else:
                totvalc = "<font color='green'>%s</font>" % str(totval)
            if DBC != WKC:
                DBCC = "<font color='red'>%s</font>" % str(DBC)
            else:
                DBCC = "<font color='green'>%s</font>" % str(DBC)


            WKCC = "<font color='green'>%s</font>" % str(WKC)
            link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (tit_gid)
            key = '<a href=%s>%s</a>' % (link, key)
            key = str(key)
            page = html_obj.create_page('%s. %s' % (na,key))
            page.add('<h4>DB Count Matched: %s,Total DB Count: %s,Wiki Count: %s</h4>' %(totvalc,DBCC,WKCC))
            if totvalc == DBCC == WKCC:
            
                na = na + 1
            else:
                continue
            value = val
            values = []
            no = 0
            for val in value:
                vl = []
                
                v = val.split('<>')
                abc = v[0]
                link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (abc)
                a = '<a href=%s>%s</a>' % (link, abc)
                a = str(a)
                try:
                    b = v[1]
                except:
                    continue
                no = no + 1
                vl.append(no)
                vl.append(a)
                for val in self.wikid:
                    ab = self.wikid[abc]
                    ab = ab[1]
                vl.append(ab)
                vl.append(b)

                values.append(vl)
            if totvalc == DBCC == WKCC:
                
                t_str = html_obj.get_html_table_with_rows(headers, values)
            
                page.add(t_str)
                self.obj_map_dict.update({title: html_obj})      
            else:
                continue
        main_html_obj = VtvHtml()
        main_page = main_html_obj.create_page('%s %s Roster Matching Report: %s' % (spt, toti,  datetime.datetime.now().date()))
        main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
        main_html_obj.container_open()
        report_file_format = '%s_roster_matching_%s.html'
        for key, obj in self.obj_map_dict.iteritems():
            page_str = obj.get_page_str()
            main_html_obj.page.add(page_str)

        report_file_name = report_file_format % (spt, self.today_str)
        report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)
        page_str = str(main_page)
        open(report_file_name,'w').write(page_str)
        copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (spt, 'latest')), self.logger) 
        self.matchreport.setdefault("Matching",report_file_name)
        vc = totcount
        if vc != 0:
            vccol = self.get_status_color('Fail')
            strif = 'Fail'
            vc = vccol.replace(strif,str(vc)) 
        else:
            vccol = self.get_status_color('Success')
            strii = 'Success'
            vc = vccol.replace(strii,str(vc))

        if ac == toti:
            accol = self.get_status_color('Success')
            strii = 'Success'
            ac = accol.replace(strii,str(ac))
        else:
            accol = self.get_status_color('Fail')
            strif = 'Fail'
            ac = accol.replace(strif,str(ac))
        
        ac = "<a href='%s'>%s</a>" % (report_file_name.replace('/data',''), ac)
        vc = "<a href='%s'>%s</a>" % (report_file_name.replace('/data',''), vc)
        tot = "<a href='%s'>%s</a>" % (report_file_name.replace('/data',''), tot)
        self.lis.append(tot)
        self.lis.append(ac)
        #self.lis.append(vc)
        
        #self.jrep.append(lis)
        print "bleh"

        

    def dif_report(self,tid):
        if tid == '229':
            tot= '30'
        if tid == '88':
            tot = '30'
        if tid == '197':
            tot = '32'
        if tid == '240':
            tot = '31'
        if tid == '34':
            tot = '22'
        if tid == '223':
            tot = '12'
        toti = tot

        for key in self.sports:
            if key != tid:
                continue
            spt = self.sports[key]
        self.jrep = []
        self.obj_map_dict = OrderedDict()
        headers = ['No.','Wikigid','id','Title','Source']
        totalcount = len(self.wiki_mismatch.keys())
        totcol = self.get_status_color('Success')
        strii = 'Success'
        tot = totcol.replace(strii,str(tot))
        na = 1
        mab = 0
        scount = 0
        for key,val in self.wiki_mismatch.iteritems():
            for tit in self.wiki_match:
                if tit != key:
                    continue
                DBM = self.wiki_match[tit]
                DBM= len(DBM)
                print DBM
                
            html_obj = VtvHtml()
            title = key
            for mary in self.mo_details:
                key = key.lower()
                if mary != key:
                    continue
                tit_gid = self.mo_details[mary]
            totval = int(len(val)/2)
            key = key.title()
            for rost in self.wiki_dict:
                if key != rost:
                    continue
                meh = self.wiki_dict[rost]
                meh = set(meh)
                meh = list(meh)
                WKC = len(meh)
            for rost in self.dbcount:
                if key != rost:
                    continue
                DBC = self.dbcount[rost]
            WIKIC = WKC - DBM
            if totval == WKC == DBC:
                mab = mab + 1

            if totval != WKC:
                totvalc = "<font color='red'>%s</font>" % str(totval)
            else:
                totvalc = "<font color='green'>%s</font>" % str(totval)
            if DBC != WKC:
                DBCC = "<font color='red'>%s</font>" % str(DBC)
            else:
                DBCC = "<font color='green'>%s</font>" % str(DBC)
        
            WKCC = "<font color='green'>%s</font>" % str(WKC)
            link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (tit_gid)
            key = '<a href=%s>%s</a>' % (link, key)
            key = str(key)
            
            page = html_obj.create_page('%s. %s' % (na,key))
            page.add('<h4>Total Mis Count: %s,DB : %s,Wiki : %s,Wiki Mis Count: %s</h4>' %(totvalc,DBCC,WKCC,WIKIC))
            na = na + 1
            value = val
            values = []
            no = 0
            for val in value:
                ab = ''
                vl = []
                v = val.split('<>')
                abc = str(v[0])
                if 'PL' in abc:
                    a = abc
                else:

                    link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (abc)
                    a = '<a href=%s>%s</a>' % (link, abc)
                    a = str(a)
                try:
                    b = v[1]
                except:
                    continue
                no = no + 1
                vl.append(no)
                vl.append(a)
                if 'PL' in abc:
                    ab = abc
                    vl.append(ab)
                    ab = 'NONE'
                else:
                    for key  in self.wikid:
                        if key != abc:
                            continue
                        ab = self.wikid[abc]
                        vl.append(ab[1])
                    if abc not in self.wikid.keys():
                        ab = "NONE"
                        vl.append(ab)
                vl.append(b)
                for sou in self.source:
                    if sou != val:
                        continue
                    scount = scount + 1
                    sour = self.source[sou]
                    vl.append(sour)
                values.append(vl)
            t_str = html_obj.get_html_table_with_rows(headers, values)

            page.add(t_str)
            self.obj_map_dict.update({title: html_obj})
        main_html_obj = VtvHtml()
        main_page = main_html_obj.create_page('%s %s Roster Mis-Matching Report: %s' % (spt, toti,  datetime.datetime.now().date()))
        main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
        main_html_obj.container_open()
        report_file_format = '%s_roster_mis_matching_%s.html'
        for key, obj in self.obj_map_dict.iteritems():
            page_str = obj.get_page_str()
            main_html_obj.page.add(page_str)

        report_file_name = report_file_format % (spt, self.today_str)
        report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)
        page_str = str(main_page)
        open(report_file_name,'w').write(page_str)
        copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (spt, 'latest')), self.logger)
        self.matchreport.setdefault("Non Matching",report_file_name)
        if mab == toti:
            accol = self.get_status_color('Success')
            strii = 'Success'
            mab = accol.replace(strii,str(mab))
        else:
            accol = self.get_status_color('Fail')
            strif = 'Fail'
            mab = accol.replace(strif,str(mab))
            
        bc = totalcount
        if bc != 0:
            vccol = self.get_status_color('Fail')
            strif = 'Fail'
            bc = vccol.replace(strif,str(bc))
        else:
            vccol = self.get_status_color('Success')
            strii = 'Success'
            bc = vccol.replace(strii,str(bc))    
         
        mab = "<a href='%s'>%s</a>" % (report_file_name.replace('/data',''), mab)
        bc = "<a href='%s'>%s</a>" % (report_file_name.replace('/data',''), bc)
        tot = "<a href='%s'>%s</a>" % (report_file_name.replace('/data',''), tot)
        #self.lis.append(tot)
        #self.lis.append(mab)
        self.lis.append(bc)
        self.jrep.append(self.lis)





    def main_report(self,tid):
        if tid == '229':
            tot= '30'
        if tid == '88':
            tot = '30'
        if tid == '197':
            tot = '32'
        if tid == '240':
            tot = '31'
        if tid == '34':
            tot = '22'
        if tid == '223':
            tot = '12'
        toti = tot

        for key in self.sports:
            if key != tid:
                continue
            title = self.sports[key]
        main_html_obj = VtvHtml()
        main_page = main_html_obj.create_page('%s Report: %s' % (title, datetime.datetime.now().date()))
        main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
        main_html_obj.container_open()
        report_file = 'summary_report_%s_%s.html' % (title, self.today_str)
        report_file = os.path.join('/data/REPORTS/SPORTS/', report_file)
        continents_name = ['Matching', 'Non Matching']
        main_html_obj.page.add('<b>Reports<b><br>')
        main_html_obj.page.ol()
        for c_name in continents_name:
            record = self.matchreport.get(c_name,'')

            if not record:
                continue
            report_file_name = record
            report_file_name = report_file_name.split('/')[-1]
            report_file_name = "<a href='%s'>%s</a>" % (report_file_name, c_name)
            #report_file_name= "<a href='%s'>%s</a>" % (report_file_name.replace('/data',''),c_name)
            main_html_obj.page.li(report_file_name)
            page_str = str(main_page)
            open(report_file,'w').write(page_str)
        report_file_format = 'summary_report_%s_%s.html'
        copy_file(report_file, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (title, 'latest')), self.logger)
        report_file = 'summary_report_%s_%s.html' % (title, 'latest')
        report_file = "<a href='%s'>%s</a>" % (report_file, title)
        if 'toti' in self.jrep[0][1]:
            accol = self.get_status_color('Success')
            strii = 'Success'
            report_file= accol.replace(strii,report_file)
        else:
            accol = self.get_status_color('Fail')
            strif = 'Fail'
            report_file = accol.replace(strif,report_file)
        self.jrep = [report_file] + self.jrep[0]
        self.final.append(self.jrep)
        
 

 
    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.','duplicates_check*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)

    def get_status_color(self, status):
        COLOR_DICT = { 'Success' : 'badge-success', 'Fail' : 'badge-important', 'Incomplete' : 'badge-warning' }

        return '<span class="badge %s">%s</span>' % (COLOR_DICT.get(status, ''), status)
    def run_main(self):
        tids = ['88','229','240','197','223','34']
        #tids = ['229']
        self.sports = {'88': 'MLB','229': 'NBA','240': 'NHL','197': 'NFL','34': 'MLS','223': 'WNBA'}
        for tid in tids:
            self.get_rosters(tid)
            self.report(tid)
            self.dif_report(tid)
            self.main_report(tid)
        jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader(os.getcwd()))
        table_html = jinja_environment.get_template('rosters_check.jinja').render(today_date = datetime.datetime.now(), big_lists=self.final)
        codecs.open(os.path.join('/data/REPORTS/SPORTS/', 'roster_summary_report_latest.html'), 'w', 'utf8').write(table_html)
        codecs.open(os.path.join('/data/REPORTS/SPORTS/', 'roster_summary_report_%s.html' % self.today_str), 'w', 'utf8').write(table_html)


if __name__ == '__main__':
    vtv_task_main(CheckDuplicateInfo)


    



