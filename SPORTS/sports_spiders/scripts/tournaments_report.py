import datetime
import sys
import os
import re
import json
import datetime

from collections import OrderedDict
from vtv_task import VtvTask, vtv_task_main
from data_report import VtvHtml
from vtv_utils import copy_file
from ssh_utils import scp
from vtv_db import get_mysql_connection
from data_report import VtvHtml

wiki_gid_qry = 'select child_gid from GUIDMERGE.sports_wiki_merge where exposed_gid=%s'
tou_id_qry = 'select id, sport_id from sports_tournaments where gid=%s'

DATA_LOGS   = '/data/REPORTS/DIFF_LOGS'
class TournamentsTeamsReport(VtvTask):

    def __init__(self):
        VtvTask.__init__(self)
        self.db_name = "SPORTSDB"
        self.db_ip   = "10.28.218.81"
        self.cursor, self.conn = get_mysql_connection(self.logger, self.db_ip,
                                                  self.db_name, cursorclass='',
                                                  user = 'veveo', passwd='veveo123')
        self.today         = str(datetime.datetime.now().date())
        self.roster_dict = OrderedDict()
        self.tou_dict = OrderedDict()
        self.main_file = {} 
        self.teams_dict = {}
        self.all_teams = []
        self.final_team_list = []
        self.obj_dict = {}
        self.wiki_dict = {}
        self.rep = {}
        self.obj_map_dict = OrderedDict()
        self.diff_map_dict = OrderedDict()
        self.tour_details = OrderedDict()
        self.sport_id =  {}
        self.missing_toulist = {}
        self.south_america = ['Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Paraguay', 'Peru', 'Uruguay', 'Venezuela']
        self.africa = ['Algeria','Angola','Burkina Faso','Burundi','Cameroon','Cape Verde','Central African Republic','Chad','Comoros','Congo','Democratic Republic of the Congo','Republic of the Congo','Djibouti','Egypt','Equatorial Guinea','Eritrea','Ethiopia','Gabon','Gambia','Ghana','Guinea','Guinea-Bissau','Ivory Coast','Kenya','Lesotho','Liberia','Libya','Madagascar','Malawi','Mali','Mauritania','Mauritius','Morocco','Mozambique','Namibia','Niger','Nigeria','Reunion','Rwanda','Sao Tome and Principe','Senegal','Seychelles','Sierra Leone','Somalia','South Africa','South Sudan','Sudan','Swaziland','Tanzania','Togo','Tunisia','Uganda','Zambia','Zanzibar','Zimbabwe','Benin','Botswana']
        self.north_america = ['Anguilla','Antigua and Barbuda','Bahamas','Barbados','Belize','Bermuda','Canada','Costa Rica','Cuba','Dominica','Dominican Republic','El Salvador','Grenada','Guatemala','Haiti','Honduras','Jamaica','Mexico','Nicaragua','Panama','Saint Kitts and Nevis','Saint Lucia','Saint Vincent and the Grenadines','Trinidad and Tobago','United States']
        self.asia = ['Afghanistan','Australia','Bahrain','Bangladesh','Bhutan','Brunei','Cambodia','China','Guam','Hong Kong','India','Indonesia','Iran','Iraq','Japan','Jordan','Kuwait','Kyrgyzstan','Laos','Lebanon','Macau','Malaysia','Maldives','Mongolia','Myanmar','Nepal','North Korea','Northern Mariana Islands','Oman','Pakistan','Palestine','Philippines','Qatar','Saudi Arabia','Singapore','South Korea','Sri Lanka','Syria','Taiwan','Tajikistan','Thailand','Timor-Leste','Turkmenistan','United Arab Emirates','Uzbekistan','Vietnam','Yemen']
        self.oceania = ['American Samoa','Cook Islands','Fiji','Kiribati','New Caledonia','New Zealand','Niue','Papua New Guinea','Samoa','Solomon Islands','Tahiti','Tonga','Tuvalu','Vanuatu']
        self.europe = ['Albania','Andorra','Armenia','Austria','Azerbaijan','Belarus','Belgium','Bosnia and Herzegovina','Bulgaria','Croatia','Cyprus','Czech Republic','Denmark','England','Estonia','Faroe Islands','Finland','France','Georgia','Germany','Greece','Hungary','Iceland','Israel','Italy','Kazakhstan','Kosovo','Latvia','Liechtenstein','Lithuania','Luxembourg','Macedonia','Malta','Moldova','Monaco','Montenegro','Netherlands','Northern Ireland','Norway','Poland','Portugal','Republic of Ireland','Romania','Russia','San Marino','Scotland','Serbia','Slovakia','Slovenia','Spain','Sweden','Switzerland','Turkey','Ukraine','Wales', 'United Kingdom']
        self.samerica_continent = {}
        self.africa_continent = {}
        self.namerica_continent = {}
        self.asia_continent = {}
        self.oceania_continent = {}
        self.europe_continent = {}
        self.samerica_continent1 = {}
        self.africa_continent1 = {}
        self.namerica_continent1 = {}
        self.asia_continent1 = {}
        self.oceania_continent1 = {}
        self.europe_continent1 = {}

        self.machine_ip = "10.28.218.78"
        self.logs_path = "/home/veveo/datagen/DC_WIKI_SPORTS_PARSER_DATAGEN/"
        self.log_pat = "tournament_counts.json"
        self.log_path = "/home/veveo/release/test"
        self.missing_tou_list = open('missing_tou_list', 'w+')
        self.countzero = open('zerocount', 'w+')
        self.REPORTS_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR, 'SPORTS')
        self.sport_dict = ['baseball', 'basketball', 'ice hockey', 'american football', 'soccer']

    def copy_latest_file(self):
        mc_path  = "%s%s" %(self.logs_path, self.log_pat)
        source       = '%s@%s:%s' % ("veveo", self.machine_ip, mc_path)
        status       = scp("veveo123", source, self.log_path)
        if status != 0:
             self.logger.info('Failed to copy the file: %s:%s' % (self.machine_ip, self.log_path))
             sys.exit()

    def get_tou_id(self, leagune_gid, leagune_nm, tou_type, sport_id):
        tm_values = (leagune_gid)
        leagune_nm = leagune_nm
        if "sevens" in leagune_nm.lower():
            sport_id = 167
        else:
            sport_id = sport_id
        self.cursor.execute(wiki_gid_qry, tm_values )
        tm_data = self.cursor.fetchone()
        tou_id = tou_gid = ''
        if tm_data:
            tou_gid = tm_data[0]
            values = (tou_gid)
            self.cursor.execute(tou_id_qry, values)
            t_data = self.cursor.fetchone()
            if t_data:
                tou_id = t_data[0]
        else:
            values = (leagune_nm, sport_id)
            sel_qry = 'select id, gid from sports_tournaments where title =%s and sport_id=%s'
            self.cursor.execute(sel_qry, values)
            tm_data = self.cursor.fetchone()
            if not tm_data:
                sel_qry = 'select id, gid from sports_tournaments where aka =%s and sport_id=%s'

                self.cursor.execute(sel_qry, values)
                tm_data = self.cursor.fetchone()
            if tm_data:
                tou_id = tm_data[0]
                tou_gid = tm_data[1]

        return tou_id, tou_gid

    def update_missing_tournaments(self, page, title, obj, dict_name, summary_header, missing_details):
        page.h4('Missing Tournments')
        t_str = obj.get_html_table_with_rows(summary_header, missing_details)
        page.add(t_str)
        dict_name.update({title: obj})

    def sport_wise_stats(self):
        sports_types_list = []
        query = 'select distinct(sport_id) from sports_participants where participant_type="team"'
        self.cursor.execute(query)
        sports_types = self.cursor.fetchall()
        for sport_type in sports_types:
            query = 'select id,title from sports_types where id = %s'
            values = int(sport_type[0])
            self.cursor.execute(query, values)
            sports_types = self.cursor.fetchall()
            ignore_list = ['tennis' , 'golf', 'cycling', 'curling', 'swimming', 'table tennis', 'ski jumping', 'aquatics', 'netball', 'athletics', 'multi sport', 'water polo' ]
            if sports_types[0][1] in ignore_list:
                continue
            record = sports_types
            sport_id, title = record[0]
            sport_id = int(sport_id)
            self.get_details(sport_id, title)
            rep,dif_rep = self.get_continent_report(title)
            self.get_summary_report(title)
            self.get_diff_summary_report(dif_rep, title)
            self.main_summary_report(title)
            #self.get_tournaments(sport_id, title)
            self.get_teams(sport_id, title)

    def get_details(self, sport_id, spt):
        self.samerica_continent = {}
        self.africa_continent = {}
        self.namerica_continent = {}
        self.asia_continent = {}
        self.oceania_continent = {}
        self.europe_continent = {}
        self.roster_dict = OrderedDict()
        self.tour_details = OrderedDict()
        
        _data = open('wiki_sports_tournaments_from_templates.json', 'r+')
        _info = open('tournament_counts.json').read()
        key_list = []
        for data in _data:
            if spt == "soccer":
                spt = "Football"
            if spt == "ice hockey":
                spt = "Ice hockey"
            if spt == "american football":
                spt = "American football"
            if spt == "rugby league":
                spt = "Rugby legaue"
            if spt == "rugby union":
                spt = "Rugby union"
            if not spt in data:
                continue
            _data = json.loads(data.strip())
            con_name =  _data.keys()[0].split('{')[0]. \
            replace('Template:Association football', 'Template:Football'). \
            replace('Soccer in the ', 'Football in the '). \
            replace('Template:Soccer in ', 'Template:Football in '). \
            replace('Template:Canada Soccer', 'Template:Football in Canada'). \
            replace('Template:USSoccer', 'Template:Football in United States'). \
            replace('Template:', '').strip()

            titlehighs = ['Ice hockey','American football','Rugby legaue','Rugby union']
            if spt not in titlehighs:
                spt = spt.title()
            con_name = con_name.replace('Template:', '').strip().encode('utf-8').replace('%s in the ' % (spt) , '').replace('%s in ' % (spt), '').strip()
            if con_name == "Georgia (country)":
                con_name = con_name.split()[0]
            elif con_name == "Republic of Macedonia":
                con_name = con_name.split()[2]
            if "Netherlands" in con_name:
                con_name = "Netherlands"
            con_wiki = _data.keys()[0].split('{')[1].split('}')[0]. \
            replace('Template:Association football', 'Template:Football'). \
            replace('Soccer in the ', 'Football in the '). \
            replace('Template:Soccer in ', 'Template:Football in '). \
            replace('Template:Canada Soccer', 'Template:Football in Canada'). \
            replace('Template:USSoccer', 'Template:Football in United States'). \
            replace('Template:', '').strip()
            link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (con_wiki)
            try:
                con_wiki = '<a href="%s">%s</a>' % (link, con_name.encode('utf8'))
                con_wiki = str(con_wiki)
                self.wiki_dict.setdefault(con_name, con_wiki)
            except:
                continue
                

            aka = ''
            try:
                major_leagues =  _data.values()[0].get('tournament', '').get('league', '').get('men', '')
            except:
                continue 
            domestic_leagues   = _data.values()[0].get('tournament', '').get('domestic', '').get('men', '')
            key_list.append(con_name)

            count  = 0
            leagues_list = ['league', 'domestic']
            for league in leagues_list[::1]:
                major_leagues =  _data.values()[0].get('tournament', '').get(league, '').get('men', '')
                for mg_leagues in major_leagues[::1]:
                    league_name = mg_leagues
                    leagune_nm = league_name.split('{')[0].strip()
                    leagune_gid = league_name.split('{')[-1].strip().replace('}', '')
                    leagune_gid = str(leagune_gid)
                    #league_wikigid = leagune_gid
                    if not leagune_gid or "WIKI" not in leagune_gid or u'\u2013' in leagune_nm \
                    or '19' in leagune_nm or "playoffs" in leagune_nm or "association football" in leagune_nm \
                    or '17' in leagune_nm or '16' in leagune_nm or " clubs in " in leagune_nm \
                        or "Women" in leagune_nm or "women's" in leagune_nm or "Conference" in leagune_nm or " top scorers" in leagune_nm \
                        or "Commonwealth" in leagune_nm or "Under 20" in leagune_nm or "Olympics" in leagune_nm:
                        continue
                    tou_type = 'tournament'
                    aka = ''
                    if " (" in leagune_nm:
                        aka  = leagune_nm
                        leagune_nm = leagune_nm.split('(')[0].strip()

                    tou_id, tou_gid = self.get_tou_id(leagune_gid, leagune_nm, tou_type, sport_id)
                    sel_location = 'select id from sports_locations where country=%s and city="" and state=""'
                    loc_values = (con_name.replace('United States', 'USA'))
                    self.cursor.execute(sel_location, loc_values)
                    loc_data = self.cursor.fetchone()
                    location_id = ''
                    if loc_data:
                        location_id = loc_data[0]
                    else:
                        continue
                    if not tou_id:
                        league_gid = leagune_gid
                        link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (leagune_gid)
                        leagune_gid = '<a href=%s>%s</a>' % (link, leagune_gid)
                        leagune_gid = str(leagune_gid)

                        try:
                            if isinstance(leagune_nm, unicode):
                                leagune_nm = leagune_nm.decode('u8')
                                self.missing_toulist.setdefault(con_name, set()).add((leagune_gid, '', '', leagune_nm.decode('u8')))
                            else:
                                self.missing_toulist.setdefault(con_name, set()).add((leagune_gid, '', '', leagune_nm.encode('u8').decode('u8')))
                                leagune_nm = leagune_nm.encode('u8').decode('u8')
                        except:
                            self.missing_toulist.setdefault(con_name, set()).add((leagune_gid, '', '', leagune_nm))
                            leagune_nm = leagune_nm
                        self.roster_dict.setdefault(con_name, OrderedDict()).setdefault(('', '', league_gid, leagune_nm), []).append(['', '', '', '', '', '']) 
                        self.tour_details.setdefault(con_name, []).append(['', '', league_gid, leagune_nm])

                    if tou_id:

                        data_qry = 'select P.id, P.gid, T.title, P.title, L.short_title, L.display_title, P.aka, P.created_at, T.created_at from sports_participants P, sports_teams L, sports_tournaments T where P.id = L.participant_id and L.tournament_id = T.id and L.tournament_id = %s order by T.title'
                        values = (tou_id)
                        self.cursor.execute(data_qry, values)
                        tou_data = self.cursor.fetchall()
                        if tou_data:
                            last_node = tou_data[0][1]
                        if not tou_data:
                            data_qry = 'select P.id, P.gid, T.title, P.title, L.short_title, L.display_title, P.aka, P.created_at, T.created_at from sports_participants P, sports_teams L, sports_tournaments T, sports_tournaments_participants TP where P.id = L.participant_id and TP.tournament_id = T.id and TP.tournament_id = %s and TP.participant_id=L.participant_id and P.id=TP.participant_id order by T.title'
                            values = (tou_id)
                            self.cursor.execute(data_qry, values)
                            tou_data = self.cursor.fetchall()
                            if tou_data:
                                last_node = tou_data[0][1]

                        if not tou_data:
                            league_gid = leagune_gid
                            link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (leagune_gid)
                            leagune_gid = '<a href=%s>%s</a>' % (link, leagune_gid)
                            leagune_gid = str(leagune_gid)
                            self.missing_tou_list.write('%s\n' % link)
                            try:
                                if isinstance(leagune_nm, unicode):
                                    self.missing_toulist.setdefault(con_name, set()).add((leagune_gid, tou_gid, tou_id, leagune_nm.decode('u8')))
                                    leagune_nm = leagune_nm.decode('u8')
                                else:
                                    self.missing_toulist.setdefault(con_name, set()).add((leagune_gid, tou_gid, tou_id, leagune_nm.encode('u8').decode('u8')))
                                    leagune_nm = leagune_nm.encode('u8').decode('u8')
                            except:
                                self.missing_toulist.setdefault(con_name, set()).add((leagune_gid, tou_gid, tou_id, leagune_nm))
                                leagune_nm = leagune_nm
                            self.roster_dict.setdefault(con_name, OrderedDict()).setdefault((tou_id, tou_gid, league_gid, leagune_nm), []).append(['', '', '', '', '', ''])
                            self.tour_details.setdefault(con_name, []).append([tou_id, tou_gid, league_gid, leagune_nm])


                        for  tou_data_ in tou_data[::1]:

                            Team_id, Team_gid, Tou_title, Team_title, Team_st, Team_dt, aka, Team_created, Tou_created = tou_data_
                            wiki_gid_qry = 'select exposed_gid from GUIDMERGE.sports_wiki_merge where child_gid=%s'
                            self.cursor.execute(wiki_gid_qry, Team_gid )
                            team_gid_link = self.cursor.fetchone()
                            if not team_gid_link:
                                Team_gid = Team_gid
                            else:
                                link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (team_gid_link)
                                Team_gid = '<a href="%s">%s</a>' % (link, Team_gid)
                                Team_gid = '%s<br>%s' % ('<a href="%s">%s</a>' % (link, str(team_gid_link[0])), Team_gid)

                            self.roster_dict.setdefault(con_name, OrderedDict()).setdefault((tou_id, tou_gid, leagune_gid, Tou_title.decode('u8')), []).append([Team_id, Team_gid, Team_title.decode('u8'), Team_st.decode('u8'), Team_dt.decode('u8'), aka.decode('u8')])
                            self.tour_details.setdefault(con_name, []).append([tou_id, tou_gid, leagune_gid, Tou_title.decode('u8')])

        key_list = sorted(key_list)
        sa , na, af, asi, oce, eur = 0, 0, 0, 0, 0, 0
        summary_header = ('Wikigid', 'Gid', 'Id', 'Title')
        dbc = 0
        rdbc = 0
        naleagc1, saleagc1, afleagc1, euleagc1, ocleagc1, asleagc1, titlecount = 0, 0 ,0, 0, 0, 0, 0
        self.europecount, self.northamericacount, self.southamericacount, self.africacount, self.asiacount, self.oceaniacount = {}, {}, {}, {}, {}, {}
        nagc, narc, sagc, sarc, afgc, afrc, asgc, asrc, eugc, eurc, ocgc, ocrc = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        naz, saz, afz, asz, euz, ocz = 0, 0, 0, 0, 0, 0
        self.asia_country_nonzero , self.africa_country_nonzero, self.europe_country_nonzero, self.northamerica_country_nonzero, self.southamerica_country_nonzero, self.oceania_country_nonzero= {}, {}, {}, {}, {}, {}
        self.europe_country_zero, self.asia_country_zero, self.africa_country_zero, self.northamerica_country_zero, self.southamerica_country_zero, self.oceania_country_zero = {}, {}, {}, {}, {}, {}
        for index, key in enumerate(key_list):
            casiazero , ceuropezero, cafricazero, cnorthamericazero, csouthamericazero, coceaniazero= 0, 0 ,0 ,0 ,0 ,0
            casianonzero, ceuropenonzero, cafricanonzero, csouthamericanonzero, cnorthamericanonzero, coceanianonzero = 0, 0, 0, 0, 0, 0
            each_tour_dict = self.roster_dict.get(key, {}) 
            tour_details = self.tour_details.get(key, {}) 
            title_wiki = self.wiki_dict.get(key, '')
            html_obj = VtvHtml()
            m_html_obj = VtvHtml()
            d_html_obj = VtvHtml()
            title = key
            missing_tou_details = self.missing_toulist.get(title, [])
            missing_details =  [ tuple(x) for x in missing_tou_details]
            all_count_page = m_html_obj.create_page('%s. %s' % (index + 1, title))
            d_count_page = d_html_obj.create_page('%s' % title)
            if title in self.north_america:
                na = na + 1
                page = html_obj.create_page('%s. %s' % (na, title_wiki))
                #if not each_tour_dict:
                 #   self.update_missing_tournaments(page, title, html_obj, self.namerica_continent, summary_header, missing_details)
            elif title in self.south_america:
                sa = sa + 1
                page = html_obj.create_page('%s. %s' % (sa, title_wiki))
                #if not each_tour_dict:
                 #   self.update_missing_tournaments(page, title, html_obj, self.samerica_continent, summary_header, missing_details)
            elif title in self.africa:
                af = af + 1
                page = html_obj.create_page('%s. %s' % (af, title_wiki))
                #if not each_tour_dict:
                 #   self.update_missing_tournaments(page, title, html_obj, self.africa_continent, summary_header, missing_details)

            elif title in self.asia:
                asi = asi + 1
                page = html_obj.create_page('%s. %s' % (asi, title_wiki))
                #if not each_tour_dict:
                 #   self.update_missing_tournaments(page, title, html_obj, self.asia_continent, summary_header, missing_details)
            elif title in self.oceania:
                oce = oce + 1
                page = html_obj.create_page('%s. %s' % (oce, title_wiki))
                #if not each_tour_dict:
                  #  self.update_missing_tournaments(page, title, html_obj, self.oceania_continent, summary_header, missing_details)
            elif title in self.europe:
                eur = eur + 1
                page = html_obj.create_page('%s. %s' % (eur, title_wiki))
                #if not each_tour_dict:
                 #   self.update_missing_tournaments(page, title, html_obj, self.europe_continent, summary_header, missing_details)
            else:
                continue

            page.ol()
            all_count_page.ol()
            for tour_info, value in each_tour_dict.iteritems():
                tou_id, tou_gid, leagune_gid, tou_title = tour_info
                link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (leagune_gid)
                wiki_gid = '<a href=%s>%s</a>' % (link, leagune_gid)
                wiki_gid = str(wiki_gid)
                cou = json.loads(_info).get(leagune_gid, '')
                wk_gid = str(leagune_gid)
                tu_gid = str(tou_gid)
                tu_id  = str(tou_id)
                tu_title = str(tou_title.encode('utf-8'))
                

                try:
                   cou = re.findall('(\d+).*?',  str(cou))[0]
                   dcou = cou
                except:
                   if cou != '':
                       cou = cou.split('(')[0].strip()
                   else:
                       cou = 0

                   dcou = cou

                tou_gid ='<font color="brown">%s</font>' % str(tou_gid)
                tou_id = '<font color="black">%s</font>' % str(tou_id)
                tou_title = '<font color="brown">%s</font>' % str(tou_title.encode('utf-8'))
                headers = ('Id', 'Gid', 'Title', 'Short_title', 'Display_title', 'Aka')

                count =  len(value)
                if len(value) ==1:
                    count = 0
                if count == cou == 0:
                    self.countzero.write('%s %s %s %s Count: %s/%s\n' % (wk_gid, tu_gid, tu_id, tu_title, count, cou))
                zerocount = count 
                
                if title in self.north_america:
                    if zerocount == 0:
                        naz = naz + 1
                    if zerocount == 0:
                        cnorthamericazero = cnorthamericazero + 1
                    self.northamerica_country_zero.update({title: cnorthamericazero})
                    if zerocount != 0:
                        cnorthamericanonzero = cnorthamericanonzero + 1
                    self.northamerica_country_nonzero.update({title: cnorthamericanonzero})
                    if str(len(value)) == cou:
                        cou = '<font color="green">%s</font>' % cou
                        db_count = '<font color="green">%s</font>' % str(count)
                        nagc = nagc + 1 
                        if tou_title:
                            naleagc1 = naleagc1 + 1
                        self.northamericacount.update({('North America',(nagc,naleagc1,naz))})
                    else:
                        cou = '<font color="red">%s</font>' % cou
                        db_count = '<font color="red">%s</font>' % str(count)
                        if tou_title:
                            naleagc1 = naleagc1 + 1
                            self.northamericacount.update({('North America',(nagc,naleagc1,naz))})
                if title in self.south_america:
                    if zerocount == 0:
                        saz = saz = saz + 1
                    if zerocount == 0:
                        csouthamericazero = csouthamericazero + 1
                    self.southamerica_country_zero.update({title: csouthamericazero})
                    if zerocount != 0:
                        csouthamericanonzero = csouthamericanonzero + 1
                    self.southamerica_country_nonzero.update({title: csouthamericanonzero})
                    if str(len(value)) == cou:
                        cou = '<font color="green">%s</font>' % cou
                        db_count = '<font color="green">%s</font>' % str(count)
                        sagc = sagc + 1
                        if tou_title:
                            saleagc1 = saleagc1 + 1
                        self.southamericacount.update({('South America',(sagc,saleagc1,saz))})
                    else:
                        cou = '<font color="red">%s</font>' % cou
                        db_count = '<font color="red">%s</font>' % str(count)
                        if tou_title:
                            saleagc1 = saleagc1 + 1
                            self.southamericacount.update({('South America',(sagc,saleagc1,saz))})
                if title in self.africa:
                    if zerocount == 0:
                        afz = afz + 1
                    if zerocount == 0:
                        cafricazero = cafricazero + 1
                    self.africa_country_zero.update({title: cafricazero})
                    if zerocount != 0:
                        cafricanonzero = cafricanonzero + 1
                    self.africa_country_nonzero.update({title: cafricanonzero})
                    if str(len(value)) == cou:
                        cou = '<font color="green">%s</font>' % cou
                        db_count = '<font color="green">%s</font>' % str(count)
                        afgc = afgc + 1
                        if tou_title:
                            afleagc1 = afleagc1 + 1
                        self.africacount.update({('Africa',(afgc,afleagc1,afz))})
                    else:
                        cou = '<font color="red">%s</font>' % cou
                        db_count = '<font color="red">%s</font>' % str(count)
                        if tou_title:
                            afleagc1 = afleagc1 + 1
                            self.africacount.update({('Africa',(afgc,afleagc1,afz))})
                if title in self.asia:
                    if zerocount == 0:
                        asz = asz + 1
                    if zerocount == 0:
                        casiazero = casiazero + 1
                    self.asia_country_zero.update({title: casiazero})
                    if zerocount != 0:
                        casianonzero = casianonzero + 1
                    self.asia_country_nonzero.update({title: casianonzero})
                    if str(len(value)) == cou:
                        cou = '<font color="green">%s</font>' % cou
                        db_count = '<font color="green">%s</font>' % str(count)
                        asgc = asgc + 1
                        if tou_title:
                            asleagc1 = asleagc1 + 1
                        self.asiacount.update({('Asia',(asgc,asleagc1,asz))})
                    else:
                        cou = '<font color="red">%s</font>' % cou
                        db_count = '<font color="red">%s</font>' % str(count)
                        if tou_title:
                            asleagc1 = asleagc1 + 1
                            self.asiacount.update({('Asia',(asgc,asleagc1,asz))})

                if title in self.europe:
                    if zerocount == 0:
                        euz = euz + 1
                    if zerocount == 0:
                        ceuropezero = ceuropezero + 1
                    self.europe_country_zero.update({title: ceuropezero})
                    if zerocount != 0:
                        ceuropenonzero = ceuropenonzero + 1
                    self.europe_country_nonzero.update({title: ceuropenonzero})
                    if str(len(value)) == cou:
                        cou = '<font color="green">%s</font>' % cou
                        db_count = '<font color="green">%s</font>' % str(count)
                        eugc = eugc + 1
                        if tou_title:
                            euleagc1 = euleagc1 + 1
                        self.europecount.update({('Europe',(eugc,euleagc1,euz))})
                    else:
                        cou = '<font color="red">%s</font>' % cou
                        db_count = '<font color="red">%s</font>' % str(count)
                        if tou_title:
                            euleagc1 = euleagc1 + 1
                            self.europecount.update({('Europe',(eugc,euleagc1,euz))})

                if title in self.oceania:
                    if zerocount == 0:
                        ocz = ocz + 1
                    if zerocount == 0:
                        coceaniazero = coceaniazero + 1
                    self.oceania_country_zero.update({title: coceaniazero})
                    if zerocount != 0:
                        coceanianonzero = coceanianonzero + 1
                    self.oceania_country_nonzero.update({title: coceanianonzero})
                    if str(len(value)) == cou:
                        cou = '<font color="green">%s</font>' % cou
                        db_count = '<font color="green">%s</font>' % str(count)
                        ocgc = ocgc + 1
                        if tou_title:
                            ocleagc1 = ocleagc1 + 1
                        self.oceaniacount.update({('Oceania',(ocgc,ocleagc1,ocz))})
                    else:
                        cou = '<font color="red">%s</font>' % cou
                        db_count = '<font color="red">%s</font>' % str(count)
                        if tou_title:
                            ocleagc1 = ocleagc1 + 1
                            self.oceaniacount.update({('Oceania',(ocgc,ocleagc1,ocz))})    
                page.li('<h4>%s %s %s %s Count: %s/%s</h4>' %(wiki_gid, tou_gid, tou_id, tou_title, db_count, cou.encode('utf-8')))
                all_count_page.li('<h4>%s %s %s %s Count: %s/%s</h4>' %(wiki_gid, tou_gid, tou_id, tou_title, db_count, cou.encode('utf-8')))
                if count > 0:
                    t_str = html_obj.get_html_table_with_rows(headers, value)
                    at_str = html_obj.get_html_table_with_rows(headers, value)
                    page.add(t_str)
                    all_count_page.add(at_str)
                    page.br()
                    all_count_page.br()
                    self.obj_map_dict.update({title: m_html_obj})
                if title in self.south_america:
                    self.samerica_continent.update({title: html_obj})
                elif title in self.north_america:
                    self.namerica_continent.update({title: html_obj})
                elif title in self.africa:
                    self.africa_continent.update({title: html_obj})
                elif title in self.asia:
                    self.asia_continent.update({title: html_obj})
                elif title in self.oceania:
                    self.oceania_continent.update({title: html_obj})
                elif title in self.europe:
                    self.europe_continent.update({title: html_obj})
                if str(len(value)) != dcou :
                    d_count_page.add('<h4>%s %s %s %s Count: %s/%s</h4>' %(wiki_gid, tou_gid, tou_id, tou_title, len(value), cou.encode('utf-8')))
                    dt_str = d_html_obj.get_html_table_with_rows(headers, value)
                    d_count_page.add(dt_str)
                    d_count_page.br()
                    self.diff_map_dict.update({title: d_html_obj})
                    if title in self.south_america:
                        self.samerica_continent1.update({title: d_html_obj})
                    elif title in self.north_america:
                        self.namerica_continent1.update({title: d_html_obj})
                    elif title in self.africa:
                        self.africa_continent1.update({title: d_html_obj})
                    elif title in self.asia:
                        self.asia_continent1.update({title: d_html_obj})
                    elif title in self.oceania:
                        self.oceania_continent1.update({title: d_html_obj})
                    elif title in self.europe:
                        self.europe_continent1.update({title: d_html_obj})
                    main_html_obj = VtvHtml()
                    main_page = main_html_obj.create_page('Improper %s Tournaments Mapping Report: %s' % (spt, datetime.datetime.now().date()))
                    main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
                    main_html_obj.container_open()
                    report_file_format = 'improper_%s_tournaments_mapping_%s.html'
                    for key, obj in self.diff_map_dict.iteritems():
                        page_str = obj.get_page_str()
                        main_html_obj.page.add(page_str)

                    report_file_name = report_file_format % (spt, self.today_str)
                    report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)
                    page_str = str(main_page)
                    open(report_file_name,'w').write(page_str)
                    copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (spt,'latest')), self.logger)
            page.ol.close()
            all_count_page.ol.close()
        nonzerocount = 0
        zerocount = 0
        totalcount = 0
        nonempty = 0 
        eucont = self.europe_continent.keys()
        afcont = self.africa_continent.keys()
        ascont = self.asia_continent.keys()
        nacont = self.namerica_continent.keys()
        sacont = self.samerica_continent.keys()
        occont = self.oceania_continent.keys()

        for euco in eucont:
            nonzerocount =  self.europe_country_nonzero.get(euco)
            zerocount = self.europe_country_zero.get(euco)
            totalcount = nonzerocount + zerocount
            if zerocount != totalcount:
                nonempty = nonempty + 1
        self.europecount.update({('Europe',(eugc,euleagc1,euz,nonempty))})
        nonempty = 0
        for afco in afcont:
            nonzerocount =  self.africa_country_nonzero.get(afco)
            zerocount = self.africa_country_zero.get(afco)
            totalcount = nonzerocount + zerocount
            if zerocount != totalcount:
                nonempty = nonempty + 1
        self.africacount.update({('Africa',(afgc,afleagc1,afz,nonempty))})
        nonempty = 0
        for asco in ascont:
            nonzerocount =  self.asia_country_nonzero.get(asco)
            zerocount = self.asia_country_zero.get(asco)
            totalcount = nonzerocount + zerocount
            if zerocount != totalcount:
                nonempty = nonempty + 1
        self.asiacount.update({('Asia',(asgc,asleagc1,asz,nonempty))})
        nonempty = 0
        for naco in nacont:
            nonzerocount =  self.northamerica_country_nonzero.get(naco)
            zerocount = self.northamerica_country_zero.get(naco)
            totalcount = nonzerocount + zerocount
            if zerocount != totalcount:
                nonempty = nonempty + 1
        self.northamericacount.update({('North America',(nagc,naleagc1,naz,nonempty))})
        nonempty = 0
        for saco in sacont:
            nonzerocount =  self.southamerica_country_nonzero.get(saco)
            zerocount = self.southamerica_country_zero.get(saco)
            totalcount = nonzerocount + zerocount
            if zerocount != totalcount:
                nonempty = nonempty + 1
        self.southamericacount.update({('South America',(sagc,saleagc1,saz,nonempty))})
        nonempty = 0
        for occo in occont:
            nonzerocount =  self.oceania_country_nonzero.get(occo)
            zerocount = self.oceania_country_zero.get(occo)
            totalcount = nonzerocount + zerocount
            if zerocount != totalcount:
                nonempty = nonempty + 1
        self.oceaniacount.update({('Oceania',(ocgc,ocleagc1,ocz,nonempty))})


        main_html_obj = VtvHtml()
        main_page = main_html_obj.create_page('%s Tournaments Mapping Report: %s' % (spt, datetime.datetime.now().date()))
        main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
        main_html_obj.container_open()
        report_file_format = '%s_tournaments_mapping_%s.html'
        for key, obj in self.obj_map_dict.iteritems():
            page_str = obj.get_page_str()
            main_html_obj.page.add(page_str)

        report_file_name = report_file_format % (spt, self.today_str)
        report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)
        page_str = str(main_page)
        open(report_file_name,'w').write(page_str)
        copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (spt, 'latest')), self.logger)

    def get_continent_report(self, title):
        continents = [self.africa_continent, self.asia_continent, self.europe_continent, self.namerica_continent, self.oceania_continent, self.samerica_continent]
        continents_name = ['South America', 'North America', 'Africa', 'Asia', 'Oceania', 'Europe']
        continents_name = sorted(continents_name)
        self.rep = {}
        rep = []
        dif_rep = []
        for conti, conti_name in zip(continents, continents_name):
            if not conti:
                continue
            main_html_obj = VtvHtml()
            main_page = main_html_obj.create_page('%s %s Tournaments Mapping Report with %s Countries: %s' % (conti_name, title, len(conti), datetime.datetime.now().date()))
            main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
            main_html_obj.container_open()
            for key, obj in sorted(conti.iteritems()):
                page_str = obj.get_page_str()
                main_html_obj.page.add(page_str)

            report_file_name = '%s_%s_tournaments_mapping_%s.html' % (conti_name.lower(), title, self.today_str)
            report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)
            page_str = str(main_page)
            open(report_file_name,'w').write(page_str)
            report_file_format = '%s_%s_tournaments_mapping_%s.html'
            copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (conti_name.lower(), title, 'latest')), self.logger)
            self.rep.setdefault(conti_name, (report_file_name, len(conti)))

        continents = [self.africa_continent1, self.asia_continent1, self.europe_continent1, self.namerica_continent1, self.oceania_continent1, self.samerica_continent1]
        continents_name = ['South America', 'North America', 'Africa', 'Asia', 'Oceania', 'Europe']
        continents_name = sorted(continents_name)
        for conti, conti_name in zip(continents, continents_name):
            main_html_obj = VtvHtml()
            main_page = main_html_obj.create_page('%s Difference report %s Tournaments Mapping Report with %s Countries: %s' % (conti_name, title, len(conti), datetime.datetime.now().date()))
            main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
            main_html_obj.container_open()
            for key, obj in sorted(conti.iteritems()):
                page_str = obj.get_page_str()
                main_html_obj.page.add(page_str)

            report_file_name = '%s_%s_tournaments_difference_mapping_%s.html' % (conti_name.lower(), title, self.today_str)
            report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)
            page_str = str(main_page)
            open(report_file_name,'w').write(page_str)
            report_file_format = '%s_%s_tournaments_difference_mapping_%s.html'
            copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (conti_name.lower(), title, 'latest')), self.logger)
            dif_rep.append(report_file_name)
        return (rep, dif_rep)
        

    def get_summary_report(self, title):
        self.files = {}
        main_html_obj = VtvHtml()
        main_page = main_html_obj.create_page('Tournaments Mapping Summary Report for 6 Continents: %s' % datetime.datetime.now().date())
        main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
        main_html_obj.container_open()
        report_file = 'summary_report_%s_%s.html' % (title, self.today_str)
        report_file = os.path.join('/data/REPORTS/SPORTS/', report_file)
        continents_name = ['South America', 'North America', 'Africa', 'Asia', 'Oceania', 'Europe']
        continents_name = sorted(continents_name)
        main_html_obj.page.add('<b>Summary for the Continents are below:<b><br>')
        main_html_obj.page.ol()
        for c_name in continents_name:
            record = self.rep.get(c_name,'')
            
            if not record:
                continue
            report_file_name,count = record
            report_file_name = report_file_name.split('/')[-1]
            report_file_name = "<a href='%s'>%s</a>" % (report_file_name, c_name)
            #report_file_name= "<a href='%s'>%s</a>" % (report_file_name.replace('/data',''),c_name)
            main_html_obj.page.li(report_file_name)
            page_str = str(main_page)
            open(report_file,'w').write(page_str)
        report_file_format = 'summary_report_%s_%s.html'
        copy_file(report_file, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (title, 'latest')), self.logger)
        report_file = 'summary_report_%s_%s.html' % (title, 'latest') 
        self.files.setdefault(report_file)

    def main_summary_report(self, title):
        counts = [self.europecount, self.northamericacount, self.southamericacount, self.africacount, self.asiacount, self.oceaniacount]
        headers = ['Sports', 'Africa', 'Asia', 'Europe', 'North America', 'South America', 'Oceania']
        self.files = self.files.keys()
        self.files = self.files[0].replace('/data', '')
        title = "<a href='%s'>%s</a>" % ((self.files), title)
        row = [''] * 7
        passed = 0
        total = 0
        mis = 0 
        nonzero = 0
        for c_name in headers:
            record = self.rep.get(c_name,'')
            if not record:
                continue
            row[0] = title
            for count in counts:
                if count == {}:
                    continue
                cont = count.keys()[0]
                if cont == c_name:
                    total = count.values()[0][1] 
                    passed = count.values()[0][0]
                    zeroes = count.values()[0][2]
                    nonzero = count.values()[0][3]
                    mis = (total)-(passed + zeroes )
                    if mis < 0:
                        mis = 0 
                    total = '<font color="red">%s</font>' % total
                    if passed > 0:
                        passed = '<font color="green">%s</font>' % passed
                    else:
                        passed = '<font color="grey">%s</font>' % passed
            report_file_name,count = record
            report_file_name = report_file_name.split('/')[-1]
            report_file_name = "<a href='%s'>C:%s &nbsp; M:%s/%s &nbsp;  Z:%s/%s &nbsp; Mis: %s/%s &nbsp; NonZero: %s/%s</a>" % (report_file_name, count, passed, total, zeroes, total, mis, total, nonzero, count)
            
                
            row[headers.index(c_name)] = str(report_file_name)
        row_length = [x for x in row if x]
        if len(row_length) != 0:
            
            self.final_team_list.append(row)
        self.generate_main()

      
 
    def get_diff_summary_report(self, dif_rep, title):
        main_html_obj = VtvHtml()
        main_page = main_html_obj.create_page('Difference Tournaments Mapping Summary Report for 6 Continents: %s' % datetime.datetime.now().date())
        main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
        main_html_obj.container_open()
        report_file = 'difference_summary_report_%s_%s.html' % (title, self.today_str)
        report_file = os.path.join('/data/REPORTS/SPORTS/', report_file)
        continents_name = ['South America', 'North America', 'Africa', 'Asia', 'Oceania', 'Europe']
        continents_name = sorted(continents_name)
        main_html_obj.page.add('<b>Difference Summary for the Continents are below:<b><br>')
        main_html_obj.page.ol()
        for c_name, report_file_name in zip(continents_name, dif_rep):
            report_file_name = report_file_name.split('/')[-1]
            report_file_name = "<a href='%s'>%s</a>" % (report_file_name, c_name)
            main_html_obj.page.li(report_file_name)
            page_str = str(main_page)
            open(report_file,'w').write(page_str)
        report_file_format = 'difference_summary_report_%s_%s.html'
        copy_file(report_file, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (title, 'latest')), self.logger)



    def get_tournaments(self, sport_id, sp_title):
        sel_qry = 'select id, title, affiliation, type, status, location_ids, created_at from sports_tournaments where sport_id=%s'
        self.cursor.execute(sel_qry, sport_id)
        data = self.cursor.fetchall()
        for data_ in data:
            tou_id, tou_title, aff, type_, status, loc_id, created_at = data_
            loc_id = str(loc_id).split('<>')[0]
            sel_t_qry = 'select country from sports_locations where id = %s'
            t_values = (loc_id)
            self.cursor.execute(sel_t_qry, t_values)
            t_data = self.cursor.fetchone()
            if t_data:
                title = t_data[0]
            else:
                title = 'NO Country'

            self.tou_dict.setdefault(title, []).append([tou_id, tou_title.decode('u8'), aff, type_, status])
        for key, value in self.tou_dict.iteritems():
            value = self.tou_dict.get(key, [])
            headers = ('Tou_id', 'Tournament', 'affiliation', 'Type', 'Status')
            html_obj = VtvHtml()
            title = key
            page = html_obj.create_page(title)
            t_str = html_obj.get_html_table_with_rows(headers, value)
            page.add(t_str)
            page.br()
            self.obj_dict.update({title: html_obj})

        main_html_obj = VtvHtml()
        main_page = main_html_obj.create_page('Basketball Tournaments Report: %s' % datetime.datetime.now())
        main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
        main_html_obj.container_open()
        report_file_format = '%s_tournaments_%s.html'
        for key, obj in self.obj_dict.iteritems():
            page_str = obj.get_page_str()
            main_html_obj.page.add(page_str)

        report_file_name = report_file_format % (title, self.today_str)
        report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)
        page_str = str(main_page)
        open(report_file_name,'w').write(page_str)
        copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (title,'latest')), self.logger) 


    def get_teams(self, sport_id, sp_title):
        teams_dict = {}
        all_teams = []
        sel_qry = 'select SP.id, SP.gid, SP.title, SP.aka, T.display_title, T.short_title, T.category, T.status, SP.created_at from sports_participants SP, sports_teams T where T.participant_id=SP.id and SP.sport_id=%s and SP.participant_type="team" order by SP.id'
        sport_id = sport_id
        self.cursor.execute(sel_qry, sport_id)
        data = self.cursor.fetchall()
        for data_ in data:
            team_id, team_gid, team_title, aka, dt, st, cat, team_status, created_at = data_
            title = "%s Teams List" % sp_title.title()
            teams_dict.setdefault(title, []).append([team_id, team_gid, team_title, aka, dt, st, cat, team_status, str(created_at)])
            all_teams.append((team_id, team_gid, team_title.decode('u8'), aka.decode('u8'), dt.decode('u8'), st.decode('    u8'), cat.decode('u8'), team_status, str(created_at)))
        self.generate_report(sp_title, all_teams, teams_dict)

    def generate_report(self, sp_title, all_teams, teams_dict):
        main_html_obj = VtvHtml()
        main_page = main_html_obj.create_page('%s Teams Report: %s' % (sp_title.title(), datetime.datetime.now()))
        main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
        main_html_obj.container_open()
        main_html_obj.page.h2('%s Teams List' %(sp_title.title()))
        self.all_teams.sort(key=lambda x: x[1])
        headers = ['Team_id', 'Team_gid', 'Team', 'Aka', 'Display_title', 'Short_title', 'Category', 'Status', 'Created_at']

        t_str = main_html_obj.get_html_table_with_rows(headers, all_teams)
        main_page.add(t_str)
        report_file_format = '%s_%s_%%s.html' % (sp_title.replace(' ','_'), 'teams_report')
        report_file_name = report_file_format % self.today_str
        report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)

        page_str = str(main_page)
        open(report_file_name,'w').write(page_str)
        copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % 'latest'), self.logger)



    def generate_main(self):
        main_html_obj = VtvHtml()
        main_page = main_html_obj.create_page('All Sports Report: %s' % datetime.datetime.now())
        main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
        main_html_obj.container_open()
        main_html_obj.page.h2('All Sports')
        main_html_obj.page.h4('C: Countries , M: Matched, Z: Zeroes , Mis: Mismatch')
        self.final_team_list.sort(key=lambda x: x[0])
        headers = ['Sports', 'Africa', 'Asia', 'Europe', 'North America', 'South America', 'Oceania']

        t_str = main_html_obj.get_html_table_with_rows(headers, self.final_team_list)
        main_page.add(t_str)
        report_file_format = 'all_sports_summary_report_%s.html'
        report_file_name = report_file_format % self.today_str
        report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)

        page_str = str(main_page)
        open(report_file_name,'w').write(page_str)
        copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % 'latest'), self.logger)

    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.', 'tournaments_report*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


    def run_main(self):
        self.sport_wise_stats()


if __name__ == '__main__':
    vtv_task_main(TournamentsTeamsReport)



