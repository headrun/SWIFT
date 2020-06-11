import datetime
# coding=utf-8
import sys
import jinja2
import codecs
import os
import re
import json
import datetime
import itertools

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
        self.ignlist = {} 
        self.all_teams = []
        self.final_team_list = []
        self.obj_dict = {}
        reload(sys)
        sys.setdefaultencoding('utf-8')
        self.wiki_dict = {}
        self.rep = {}
        self.total  = []
        self.obj_map_dict = OrderedDict()
        self.diff_map_dict = OrderedDict()
        self.tour_details = OrderedDict()
        self.stats = [] 
        self.sport_id =  {}
        self.missing_toulist = {}
        self.ignore_toulist = {}
        self.south_america = ['Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Paraguay', 'Peru', 'Uruguay', 'Venezuela']
        self.africa = ['Algeria','Angola','Burkina Faso','Burundi','Cameroon','Cape Verde','Central African Republic','Chad','Comoros','Congo','Democratic Republic of the Congo','Republic of the Congo','Djibouti','Egypt','Equatorial Guinea','Eritrea','Ethiopia','Gabon','Gambia','Ghana','Guinea','Guinea-Bissau','Ivory Coast','Kenya','Lesotho','Liberia','Libya','Madagascar','Malawi','Mali','Mauritania','Mauritius','Morocco','Mozambique','Namibia','Niger','Nigeria','Reunion','Rwanda','Sao Tome and Principe','Senegal','Seychelles','Sierra Leone','Somalia','South Africa','South Sudan','Sudan','Swaziland','Tanzania','Togo','Tunisia','Uganda','Zambia','Zanzibar','Zimbabwe','Benin','Botswana']
        self.north_america = ['Anguilla','Antigua and Barbuda','Bahamas','Barbados','Belize','Bermuda','Canada','Costa Rica','Cuba','Dominica','Dominican Republic','El Salvador','Grenada','Guatemala','Haiti','Honduras','Jamaica','Mexico','Nicaragua','Panama','Saint Kitts and Nevis','Saint Lucia','Saint Vincent and the Grenadines','Trinidad and Tobago','United States','West Indies']
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
        self.leh = [] 

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
     
    def update_ignore_tournaments(self,i_count_page, title, i_html_obj, dict_name, headers, ignore_details):
        t_str = i_html_obj.get_html_table_with_rows(headers,ignore_details)
        i_count_page.add(t_str)
        dict_name.update({title: i_html_obj})

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
            '''ignore_list = ['tennis' , 'golf', 'cycling', 'curling', 'swimming', 'table tennis', 'ski jumping', 'aquatics', 'netball', 'athletics', 'multi sport', 'water polo','basketball','american football','baseball','cricket','soccer','field hockey','rugby union','rugby sevens','volleyball','softball','rugby league','canadian football','rules football','handball','futsal','gaelic football','beach soccer','kabaddi']'''
            ignore_list = ['tennis' , 'golf', 'cycling', 'curling', 'swimming', 'table tennis', 'ski jumping', 'aquatics', 'netball', 'athletics', 'multi sport', 'water polo']
            if sports_types[0][1] in ignore_list:
                continue
            record = sports_types
            sport_id, title = record[0]
            sport_id = int(sport_id)
            #self.get_details(sport_id, title)
            self.get_temp_details(sport_id, title)
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
        self.ign_map_dict = {}
        self.roster_dict = OrderedDict()
        self.tour_details = OrderedDict()
        self.ignlist = {}
        self.ignore_toulist = {}
        
        _info = open('tournament_counts.json').read()
        key_list = []



        sel_qry = 'select id, gid, title, affiliation, type, status, location_ids, attribute, created_at from sports_tournaments where sport_id=%s and type= "tournament" and status = "" and attribute in ("major","major<>professional")'
        self.cursor.execute(sel_qry, sport_id)
        data1 = self.cursor.fetchall()
        sel_qry = 'select id, gid, title, affiliation, type, status, location_ids, attribute, created_at from sports_tournaments where sport_id=%s and type= "tournament" and status = "" and attribute not in ("junior","domestic","major","youth","Minor","Junior<>Division 1","League<>Youth","Domestic<>Youth","major<>professional")'
        self.cursor.execute(sel_qry, sport_id)
        data2 = self.cursor.fetchall()
        data = data1+data2
        for data_ in data:
            
            tou_id, tou_gid, tou_title, aff, type_, status, loc_id, attri,created_at = data_
            loc_id = str(loc_id).split('<>')[0]
            sel_t_qry = 'select country from sports_locations where id = %s'
            t_values = (loc_id)
            self.cursor.execute(sel_t_qry, t_values)
            t_data = self.cursor.fetchone()
            if t_data:
                title = t_data[0]
            else:
                title = 'NO Country'
            if title == "USA":
                title = "United States"
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

                
            key_list.append(title)
            leagune_nm = tou_title
            sel_location = 'select id from sports_locations where country=%s and city="" and state=""'
            loc_values = (title.replace('United States', 'USA'))
            self.cursor.execute(sel_location, loc_values)
            loc_data = self.cursor.fetchone()
            gid_qry = 'select exposed_gid from GUIDMERGE.sports_wiki_merge where child_gid=%s'
            vals = (tou_gid)
            self.cursor.execute(gid_qry, vals)
            leagune_gid = self.cursor.fetchone()
            if not leagune_gid:
                leagune_gid = vals
            else:
                leagune_gid = leagune_gid[0]
            if not tou_id:
                league_gid = leagune_gid
                link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (leagune_gid)
                leagune_gid = '<a href=%s>%s</a>' % (link, leagune_gid)
                leagune_gid = str(leagune_gid)
                try:
                    if isinstance(leagune_nm, unicode):
                        leagune_nm = leagune_nm.decode('u8')
                        self.missing_toulist.setdefault(title, set()).add((leagune_gid, '', '', leagune_nm.decode('u8')))
                    else:
                        self.missing_toulist.setdefault(title, set()).add((leagune_gid, '', '', leagune_nm.encode('u8').decode('u8')))
                        leagune_nm = leagune_nm.encode('u8').decode('u8')
                except:
                    self.missing_toulist.setdefault(title, set()).add((leagune_gid, '', '', leagune_nm))
                    leagune_nm = leagune_nm
                self.roster_dict.setdefault(title, OrderedDict()).setdefault(('', '', league_gid, leagune_nm), []).append(['', '', '', '', '', '']) 
                self.tour_details.setdefault(title, []).append(['', '', league_gid, leagune_nm])

            if tou_id:
                if tou_id == 3301L or tou_id == 6938L or tou_id == 6805L or tou_id == 3306L or tou_id == 3308L or tou_id == 5476L or tou_id == 4780L or tou_id == 5473L or tou_id == 4779L or tou_id == 6703L or tou_id == 6495L or tou_id == 6957L or tou_id == 5570L or tou_id == 5547L or tou_id == 5502L or tou_id == 6596L or tou_id == 6586L or tou_id == 5542L or tou_id == 5538L or tou_id == 6601L or tou_id == 5501L or tou_id == 6599L or tou_id == 1901L or tou_id == 5886L or tou_id == 6130L or tou_id == 7352L or tou_id == 5880L or tou_id == 5897L or tou_id == 5113L or tou_id == 7017L or tou_id == 5449L or tou_id == 6206L or tou_id == 5873L or tou_id == 5453L or tou_id == 4596L or tou_id == 5032L or tou_id == 6117L or tou_id == 6397L or tou_id == 6957L or tou_id == 5685L or tou_id == 7403L or tou_id == 5113L or tou_id == 5748L or tou_id == 3515L or tou_id == 5295L or tou_id == 3750L or tou_id == 6119L or tou_id == 6006L or tou_id == 6114L or tou_id == 6505L or tou_id == 6125L or tou_id == 6098L or tou_id == 7508L or tou_id == 6059L or tou_id == 7348L or tou_id == 5982L or tou_id == 6229L or tou_id == 6260L or tou_id == 6144L or tou_id == 4993L or tou_id == 4971L or tou_id == 5569L or tou_id == 5527L or tou_id == 5513L or tou_id == 5565L or tou_id == 5545L or tou_id == 5540L or tou_id == 5541L or tou_id == 4903L or tou_id == 5548L or tou_id == 5561L or tou_id == 5531L or tou_id == 5520L or tou_id == 4920L or tou_id == 5553L or tou_id == 5524L or tou_id == 4869L or tou_id == 4424L :
                    data_qry = 'select P.id, P.gid, T.title, P.title, L.short_title, L.display_title, P.aka, P.created_at, T.created_at from sports_participants P, sports_teams L, sports_tournaments T, sports_tournaments_participants TP where P.id = L.participant_id and TP.tournament_id = T.id and TP.tournament_id = %s and TP.participant_id=L.participant_id and P.id=TP.participant_id order by T.title'
                else:
                    data_qry = 'select P.id, P.gid, T.title, P.title, L.short_title, L.display_title, P.aka, P.created_at, T.created_at from sports_participants P, sports_teams L, sports_tournaments T where P.id = L.participant_id and L.tournament_id = T.id and L.tournament_id = %s order by T.title'
                values = (tou_id)
                self.cursor.execute(data_qry, values)
                tou_data = self.cursor.fetchall()
                if tou_data:
                    firstnode = tou_data[0]
                    last_node = tou_data[-1]
                if not tou_data or (firstnode == last_node):
        
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
                            self.missing_toulist.setdefault(title, set()).add((leagune_gid, tou_gid, tou_id, leagune_nm.decode('u8')))
                            leagune_nm = leagune_nm.decode('u8')
                        else:
                            self.missing_toulist.setdefault(title, set()).add((leagune_gid, tou_gid, tou_id, leagune_nm.encode('u8').decode('u8')))
                            leagune_nm = leagune_nm.encode('u8').decode('u8')
                    except:
                        self.missing_toulist.setdefault(title, set()).add((leagune_gid, tou_gid, tou_id, leagune_nm))
                        leagune_nm = leagune_nm
                    self.roster_dict.setdefault(title, OrderedDict()).setdefault((tou_id, tou_gid, league_gid, leagune_nm), []).append(['', '', '', '', '', ''])
                    self.tour_details.setdefault(title, []).append([tou_id, tou_gid, league_gid, leagune_nm])


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

                    self.roster_dict.setdefault(title, OrderedDict()).setdefault((tou_id, tou_gid, leagune_gid, Tou_title.decode('u8')), []).append([Team_id, Team_gid, Team_title.decode('u8'), Team_st.decode('u8'), Team_dt.decode('u8'), aka.decode('u8')])
                    self.tour_details.setdefault(title, []).append([tou_id, tou_gid, leagune_gid, Tou_title.decode('u8')])
        key_list = sorted(set(key_list))
        self.ignlist = {}
        sa , na, af, asi, oce, eur = 0, 0, 0, 0, 0, 0
        summary_header = ('Wikigid', 'Gid', 'Id', 'Title')
        dbc = 0
        ignlist = []
        rdbc = 0
        naleagc1, saleagc1, afleagc1, euleagc1, ocleagc1, asleagc1, titlecount = 0, 0 ,0, 0, 0, 0, 0
        self.europecount, self.northamericacount, self.southamericacount, self.africacount, self.asiacount, self.oceaniacount = {}, {}, {}, {}, {}, {}
        nagc, narc, sagc, sarc, afgc, afrc, asgc, asrc, eugc, eurc, ocgc, ocrc = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        naz, saz, afz, asz, euz, ocz = 0, 0, 0, 0, 0, 0
        sac, nac, afc, asic, ocec, eurc, totigc = 0, 0, 0, 0, 0, 0, 0
        ct = 0
        self.asia_country_nonzero , self.africa_country_nonzero, self.europe_country_nonzero, self.northamerica_country_nonzero, self.southamerica_country_nonzero, self.oceania_country_nonzero= {}, {}, {}, {}, {}, {}
        self.europe_country_zero, self.asia_country_zero, self.africa_country_zero, self.northamerica_country_zero, self.southamerica_country_zero, self.oceania_country_zero = {}, {}, {}, {}, {}, {}
        self.igfile = {}
        ignore_details = []
        for index, key in enumerate(key_list):
            casiazero , ceuropezero, cafricazero, cnorthamericazero, csouthamericazero, coceaniazero= 0, 0 ,0 ,0 ,0 ,0
            casianonzero, ceuropenonzero, cafricanonzero, csouthamericanonzero, cnorthamericanonzero, coceanianonzero = 0, 0, 0, 0, 0, 0
            each_tour_dict = self.roster_dict.get(key, {}) 
            each_template_tour_dict = self.roster_template_dict.get(key, {})
            each_template_domestic_tour_dict = self.roster_template_domestic_dict.get(key, {})
            tour_details = self.tour_details.get(key, {}) 
            title_wiki = self.wiki_dict.get(key, '')
            if not title_wiki:
                if title == 'NO COUNTRY':
                    continue
                else:
                    title_wiki = key
            html_obj = VtvHtml()
            m_html_obj = VtvHtml()
            d_html_obj = VtvHtml()
            i_html_obj = VtvHtml()
            title = key
            missing_tou_details = self.missing_toulist.get(title, [])
            missing_details =  [ tuple(x) for x in missing_tou_details]
            all_count_page = m_html_obj.create_page('%s. %s' % (index + 1, title))
            d_count_page = d_html_obj.create_page('%s' % title)
            self.dict1 = OrderedDict()
            ignct = 0
            coutitle = title
            dict_name = self.ign_map_dict
            title1 = ''
            #self.igfile = {}
            i_count_page = i_html_obj.create_page('%s' % title1)
            for tour_info, value in each_template_tour_dict.iteritems():
                if tour_info not in self.dict1.keys():
                    b_set = set(tuple(x) for x in value)
                    value = [ list(x) for x in b_set ]
                    self.dict1.setdefault(tour_info, value)
            for tour_info, value in each_tour_dict.iteritems():
                if tour_info in each_template_domestic_tour_dict.keys():
                    continue
                if tour_info[3] in self.leh:
                    continue
                if tour_info not in self.dict1.keys():

                    self.dict1.setdefault(tour_info, value)


            
            for tour_info, value in self.dict1.iteritems():
                
                headers = ['Country','Gid','Title']
                tournaments_ignore_list = ['A2 Ethniki Volleyball',"Slovakia Men's Volleyball League",'Azerbaijan Volleyball League','Regionalliga','Ecuadorian Segunda Categoria','Northern Football League (Australia)','Australian rules football in South East Queensland','Campeonato Baiano',"Portuguese Volleyball Second Division","Czechoslovak First Ice Hockey League","2nd Czech Republic Hockey League","Karjala Tournament","Regionalliga (ice hockey)","Lithuania Hockey League","Russian Hockey Second League","Sweden Hockey Games","TV-pucken","Ukrainian Hockey Extra League","Ukrainian Men's Volleyball Super League",'Australian rules football leagues in regional Queensland','Fortescue National Football League','West Australian State Premiership','Adelaide Footy League','AFL Queensland','Campeonato Paraense','Campeonato Paranaense','Campeonato Pernambucano','Campeonato Cearense','Campeonato Goiano','Campeonato Paulista','Campeonato Piauiense','Campeonato Potiguar','Campeonato Catarinense','Campeonato Mato-Grossense','Campeonato Maranhense','Campeonato Mineiro','ACTRU Premier Division','Dewar Shield','Campeonato Nacional de Clubes (Venezuela)','Campeonato Paraense Second Division','Recopa Sul-Brasileira','Tercera B','Tercera B (Chile)','Gridiron Victoria','ACT Gridiron','Czech League of American Football','American football Regionalliga','German Junior Bowl','German Bowl','Rocky Mountain Lacrosse League','Prairie Gold Lacrosse League','Thompson Okanagan Senior Lacrosse League','OLA Senior B Lacrosse League','Western Lacrosse Association','West Coast Senior Lacrosse Association','Salibandyliiga','National League A (Floorball)','Flemish American Football League','Finnish 1st Division (American football)','Vaahteraliiga','Central European Football League','American Football Wellington','AFBN Division One','BritishUniversities American Football League','Australian Schools Championship','Claxton Shield','Australian Schools Championship (baseball)','Kansai Independent Baseball League','Eastern League (Japanese baseball)','Western League (Japanese baseball)','Taiwan Baseball Summer League','Nova Scotia Senior Baseball League','Grand Forks International','Dominican Summer League','Gulf Coast League','Prairie Gold Lacrosse League','T20 Global League','Stanbic Bank 20 Series','Major League Cricket','American Twenty20 Championship','NYPD Cricket League','Friends Provident Trophy',"Greek Men's Cricket Championship",'Scottish National Cricket League','Mini Indian Premier League','Rohinton Baria Trophy','South Australian Grade Cricket League','Premier Limited Overs Tournament','ACC Premier League',"NCAA Men's Water Polo Championship",'Mongolia Hockey League','Turkmenistan Championship (ice hockey)','Minas Gerais Volleyball Championship','Bartercard Premiership','NBA Summer League','NBA Development League','Liga de Desenvolvimento de Basquete ','Nigeria Basketball Federation','Libyan Arab Basketball Federation','United States Premier Hockey League','Philippine Collegiate Champions League','Division 1-A Rugby','USA Rugby Club 7s','Nestea Beach Volleyball','Philippine Super Liga','PVF Beach Volleyball tournament','Beach Volleyball Republic','Volleyball Thai-Denmark Super League','Dominican Republic Volleyball League','Dominican Republic National Beach Volleyball Tour','USAFL Western Regionals','Ligue Nationale de Handball','Portuguese Handball Fourth Division','Eric Shirley Shield','Blackrock Rugby Festival',"East Men's League","Yorkshire Men's League",'New Caledonia Super Ligue','New Caledonia Second Level','Lotto Sport Italia NRFL Division 1','Lotto Sport Italia NRFL Division 2','Campeonato Carioca',"U Sports men's ice hockey",'Championnat de France (water polo)',"Serie A2 (men's water polo)","Serie B (men's water polo)",'Adriatic Water Polo League','Kuwaiti Futsal League','Kyrgyzstan Futsal League','DBKL Youth Futsal League','IPT Futsal League','Red Bull Futsal League','Vietnam Futsal League',"Milo President's Trophy Knockout Tournament",'COBRA Rugby Tens','ARFU Asian Rugby Series','Inter-District Championship','Kiribati National Championship','New Zealand Football Championship','Papua New Guinea Overall Championship','Papua New Guinea National Club Championship','Nationale 1','A2 Ethniki Water Polo' ,"Northern Premier League","Southern Football League","Combined Counties Football League","East Midlands Counties Football League","Eastern Counties Football League","Essex Senior Football League","Hellenic Football League","Midland Football League" ,"Northern Counties East Football League" ,"Northern Football League" ,"North West Counties Football League","Southern Counties East Football League","South West Peninsula League","Spartan South Midlands Football League","Southern Combination Football League","United Counties League","Wessex Football League","Western Football League","West Midlands (Regional) League","FA WSL 1","Sheffield & Hallamshire County Senior Football League","RFL Reserve Championship","Mercian Regional Football League","Northern Premier League Division One North","Northern Premier League Division One South","Northern Premier League Premier Division","Bristol and Avon Association Football League","Teesside Football League","FA WSL 2" ,"FA WSL","Gloucestershire County Football League","North Bucks & District Football League" ,"West Lancashire Football League","Mid-Somerset Football League","Bedfordshire County Football League","Hope Valley Amateur League","Craven and District Football League","Basingstoke and District Football League","Kent County League","West Yorkshire Association Football League","Coventry Alliance Football League","Surrey Elite Intermediate Football League","West Cheshire Association Football League","Perry Street and District League","Cheltenham Association Football League","St. Edmundsbury Football League","Wearside Football League" ,"Cirencester and District League","Portsmouth Saturday Football League","Duchy League","Middlesex County Football League" ,"Nottinghamshire Senior League","Arthurian League","Plymouth and West Devon Football League","Aldershot & District Football League","Midlands Regional Alliance","Anglian Combination","Banbury District and Lord Jersey FA","Trelawny League","Liverpool Old Boys' Amateur Football League","Hampshire Premier League" ,"East Cornwall League" ,"Southampton Saturday Football League","Central and South Norfolk League","Witney and District League","North Devon Football League","Brighton, Worthing & District Football League","Central Midlands Football League","East Berkshire Football League","Huddersfield and District Association Football League","Halifax and District Association Football League","North Lancashire and District Football League","Yeovil and District League","Norwich and District Saturday Football League","Southern Amateur Football League","Great Yarmouth and District Football League","Herefordshire Football League","North East Norfolk League","Andover and District Football League","Bristol Downs Association Football League","Bromley and District Football League","South London Football Alliance","Portuguese Volleyball First Division","National Junior Hockey League","Vysshaya Liga (ice hockey)","Scottish Premier Hockey League","Slovak 1. Liga","Hockeyettan","J20 SuperElit","J20 Elit","Oberliga (ice hockey)","Swiss 1. Liga (ice hockey)","Austrian Landesliga","Austrian 2. Landesliga","Austrian Basketball Supercup","A1 Liga","Alain Gilles Trophy","2. Basketball Bundesliga","Greek C Basket League","Hellenic Basketball Clubs Association","HEBA Greek All Star Game","Second Division Men (Icelandic basketball)","Liga Artzit (basketball)","Lega Basket All Star Game","Regional Basketball League","Lithuanian Students Basketball League","Regional Basketball League (Lithuania)","NBB-Beker","Istanbul Basketball League","Turkish Wheelchair Basketball Super League","Turkish Basketball Super League","Championnat LNB","Basketball League of Serbia B","Serie B Basket","I Liga (basketball)","Russian Professional Basketball League","USSR Premier Basketball League","Serie A2 Basket","Euskal Kopa","Liga EBA","English Basketball League","Campeonato Brasileiro Série D","Argentine División Intermedia","Paraguayan Tercera División","Paraguayan Cuarta División","Taça Brasil","Campeonato Brasileiro de Seleções Estaduais","Taça da Prefeitura do Distrito Federal","Ecuadorian Segunda Categoría","Venezuelan Segunda División B","Venezuelan Tercera División","Campeonato Carioca (lower levels)","Campeonato Paulista Série A2","Campeonato Paulista Segunda Divisão","Campeonato Sergipano Série A2","Campeonato da Cidade de Campos","Campeonato Fluminense","Primera División de Baloncesto","Lietuvos moksleivių krepšinio lyga","Divisão de Elite","Kulcsár Anita-emléktorna","Federaci ón Argentina de Football","Asociación Amateurs de Football","Liga Argentina de Football","Liga Rosarina de Football","Taça Victorino Cunha","António Pratas Trophy","Supercopa de España de Balonmano Femenino","Campeonato Argentino de Básquet","Ligue Régional II","Ligue de Football de la Wilaya","Angolan Provincial Stage","Botswana First Division North","Botswana First Division South","Egyptian Fourth Division","Canal Zone League","Kenyan Regional Leagues","Kenyan County Champions League","Kenyan Sub-County Leagues","GNFA 1","SAFA Regional League","South African Soccer League","NSL Second Division","Khartoum League","Zimbabwe Division 1","Federación Argentina de Football","Somali Second Division","Somali Third Division","Tanzanian First Division League","Tanzanian Second Division League","Nigeria National League","Nigeria Nationwide League","Nigeria Amateur League Division Two","Rwandan Second Division","Inter-Régions Division","Ligue Régional I","Gira Angola","Elite Two","Santiago Island League (South)","FKF Division One","Libyan Second Division","Zambian Division One","Kenyan National Super League","Libyan Third Division","Botola 2","South African Premier Division","National First Division","SAFA Second Division","Tunisian Ligue Professionnelle 2","Tunisian Ligue Professionnelle 3","FUFA Big League","Togolese Championnat National","Grand Bahama Football League","New Providence Football League","Belize Premier Football League","U Sports men's soccer","Ontario Soccer League","Croatian-North American Soccer Tournament","Tercera Division de Costa Rica","Segunda División de El Salvador","Tercera Division de Fútbol Salvadoreño","GFA First Division","Primera División de Ascenso","Segunda División de Ascenso","Honduran Liga Nacional de Ascenso","South Central Confederation Super League","Eastern Confederation Super League","Western Confederation Super League","KSAFA Major League","Segunda División de México","Tercera División de México","Liga Premier de Ascenso","Premier Arena Soccer League","Tercera División de Nicaragua","Saint Kitts and Nevis Division 1","SKNFA Super League","Saint Lucia Silver Division","United States Adult Soccer Association","Premier Development League","National Premier Soccer League","American Premier Soccer League","United Premier Soccer League","Major Arena Soccer League","Premier Arena Soccer League","Professional Futsal League","Major League Futsal","NCAA Division II Men's Soccer Championship","NCAA Division II Men's Soccer Championship","United Soccer Association","National Women's Soccer League","Champions Soccer League USA","Barbados First Division","Super League of Belize","Super-20 League","League1 Ontario","Ligue de Soccer Elite Quebec","Canadian Premier League","Pacific Coast Soccer League","U Sports men's soccer championship","Soccer at the Canada Games","Alberta Major Soccer League","Canadian Soccer League championship final","Kitchener and District Soccer League","Vancouver Metro Soccer League","Segunda División de Costa Rica","Primera División de Republica Dominicana","Dominican Football Federation","Primera División Reserves (El Salvador)","Liga Nacional de Fútbol de Honduras Reserves","Liga Mayor de Futbol de Honduras","Honduras Primera Division","Segunda División de Nicaragua","KSAFA Super League","Liga Nacional de Ascenso","Saint Kitts Premier Division","NLA First Division Club Championship","National Super League","North American Soccer League","United Soccer League","Premier League of America","NCAA Division III Men's Soccer Championship","NAIA Men's Soccer Championship","Albanian Second Division","Albanian Third Division","Segona Division","Austrian Regional League","Austrian Regional League East","Austrian Regional League Central","Austrian Regional League West","ÖFB-Frauenliga","AFFA Amateur League","Belarusian First League","Belarusian Second League","Belarusian Premier League Reserves Championship","Belgian First Amateur Division","Belgian Third Amateur Division","Belgian Provincial Leagues","Belgian Fourth Division","Belgian Third Division","Second League of the Federation of Bosnia and Herzegovina","Regional Amateur Football Groups","Bulgarian Republic Football Championship","Bulgarian State Football Championship","Regional Amateur Football Groups (Bulgaria)","Croatian Third Football League","First County Football League","First County Football League (Croatia)","First League of Primorje-Gorski Kotar County","Inter-county League Rijeka","Croatian Women's First Football League","Cypriot Third Division","STOK Elite Division","Bohemian Football League","Czech Fourth Division","Prague Championship","Moravian–Silesian Football League","Danish 2nd Division","Denmark Series","Elitedivisionen","Esiliiga B","II liiga","III liiga","IV liiga","Estonian Football Winter Tournament","Naiste Meistriliiga","2. deild","3. deild","1. deild kvinnur","Ykkonen","Kakkonen","Kolmonen","Nelonen","Vitonen","Kutonen","Seiska","Seiska (football)","Nelonen (football)","Naisten Liiga","Division d'Honneur","Martinique Championnat National","Ligue des Antilles","Réunion Premier League","Division 1 Féminine"]
                wlist = ["NCAA Women's Division I Hockey","NCAA Women's Division III Hockey","Women's National Basketball Association","NCAA Men's Division II Hockey","NCAA Division III Men's Lacrosse Championship","NCAA Division I Men's Lacrosse Championship","NCAA Division II Men's Lacrosse Championship","NCAA Men's Division II Volleyball"]
                if tour_info[3] in wlist:
                    continue
                self.ignore_toulist = {}
                if tour_info[3] in tournaments_ignore_list:
                    ct = ct + 1
                    i_html_obj = VtvHtml()
                    values = [tour_info[3]]
                    wiki = tour_info[2]
                    link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (wiki) 
                    wikilink = '<a href="%s">%s</a>' % (link, wiki) 
                    self.ignore_toulist.setdefault(title, set()).add((title,wikilink,tour_info[3]))
                    #import pdb;pdb.set_trace()
                    title1 = '' 
                    i_count_page = i_html_obj.create_page('%s' % title1)
                    ignore_tou_details = self.ignore_toulist.get(title, [])
                    ignore_details =  [tuple(x) for x in ignore_tou_details]
                    if coutitle in ignore_details[0][0]:
                        coutitle = '' 
                        

                    self.ignlist.setdefault(title,[]).append([ignore_details[0]])
                    #self.update_ignore_tournaments(i_count_page, title, i_html_obj, self.ign_map_dict, headers, ignore_details)

            conf_list = []
            igcount = 0
            for coun, teamss in self.ignlist.iteritems():
                for teams in teamss:
                    igcount = igcount + 1
                    igcount, dt, st, p_id = igcount,teams[0][0],teams[0][1],teams[0][2]

                    html_obj = VtvHtml()
                    conf_list.append((igcount,dt,st,p_id))
            conf_list.sort(key=lambda x: x[0])
            main_html_obj = VtvHtml()
            main_page = main_html_obj.create_page('Ignore Report for %s leagues: %s' % (igcount,datetime.datetime.now().date()))
            main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
            main_html_obj.container_open()
            report_file_format = '%s_ignore_report_%s.html'
            headers = ['Count','Country','Wiki','Title']
            t_str = main_html_obj.get_html_table_with_rows(headers, conf_list)
            main_page.add(t_str)
            report_file_name = report_file_format % (spt, self.today_str)
            report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)
            page_str = str(main_page)
            open(report_file_name,'w').write(page_str)
            copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (spt,'latest')), self.logger)
            self.igfile.setdefault(report_file_name)


        
            for tour_info, value in self.dict1.iteritems():
                if tour_info[3] in tournaments_ignore_list:    
                    ignct = ignct + 1
                    continue
            if title in self.north_america:
                if len(self.dict1.keys()) == ignct:
                    continue
                nac = nac + ignct
                na = na + 1
                page = html_obj.create_page('%s. %s' % (na, title_wiki))
                #if not each_tour_dict:
                 #   self.update_missing_tournaments(page, title, html_obj, self.namerica_continent, summary_header, missing_details)
            elif title in self.south_america:
                if len(self.dict1.keys()) == ignct:
                    continue
                sac = sac + ignct
                sa = sa + 1
                page = html_obj.create_page('%s. %s' % (sa, title_wiki))
                #if not each_tour_dict:
                 #   self.update_missing_tournaments(page, title, html_obj, self.samerica_continent, summary_header, missing_details)
            elif title in self.africa:
                if len(self.dict1.keys()) == ignct:
                    continue
                afc = afc + ignct
                af = af + 1
                page = html_obj.create_page('%s. %s' % (af, title_wiki))
                #if not each_tour_dict:
                 #   self.update_missing_tournaments(page, title, html_obj, self.africa_continent, summary_header, missing_details)

            elif title in self.asia:
                if len(self.dict1.keys()) == ignct:
                    continue
                asic= asic + ignct
                asi = asi + 1
                page = html_obj.create_page('%s. %s' % (asi, title_wiki))
                #if not each_tour_dict:
                 #   self.update_missing_tournaments(page, title, html_obj, self.asia_continent, summary_header, missing_details)
            elif title in self.oceania:
                if len(self.dict1.keys()) == ignct:
                    continue
                ocec = ocec + ignct
                oce = oce + 1
                page = html_obj.create_page('%s. %s' % (oce, title_wiki))
                #if not each_tour_dict:
                  #  self.update_missing_tournaments(page, title, html_obj, self.oceania_continent, summary_header, missing_details)
            elif title in self.europe:
                if len(self.dict1.keys()) == ignct:
                    continue
                eurc = eurc + ignct
                eur = eur + 1
                page = html_obj.create_page('%s. %s' % (eur, title_wiki))
                #if not each_tour_dict:
                 #   self.update_missing_tournaments(page, title, html_obj, self.europe_continent, summary_header, missing_details)
            else:
                continue
            page.ol()
            all_count_page.ol()
            for tour_info, value in self.dict1.iteritems():
                values = []
                for index ,val in enumerate(value):
                    valu = [index+1] + val
                    values = values + [valu]
                if 'Cup' in tour_info[3] :
                    continue
                if 'Copa' in tour_info[3] :
                    continue 
                if 'Coupe' in tour_info[3] :
                    continue
                if 'Ligas' in tour_info[3] :
                    continue
                if 'Torneo' in tour_info[3] :
                    continue
                if 'Torneos' in tour_info[3] :
                    continue
                if 'Torneio' in tour_info[3] :
                    continue
                if 'Sevens' in tour_info[3] :
                    continue
                if 'World' in tour_info[3] :
                    continue
                if 'High School' in tour_info[3] :
                    continue
                if 'Conference' in tour_info[3] :
                    continue
                if 'Under 19' in tour_info[3]:
                    continue
                if 'U-19' in tour_info[3]:
                    continue
                if 'U-20' in tour_info[3]:
                    continue
                if sport_id != 7: 
                    if 'Championship' in tour_info[3]:
                        continue
                if "NCAA Men's Division II Volleyball" in tour_info[3]:
                    continue



                

                '''lists = ['Cup','Copa','Coupe','Ligas','Torneo','Torneos','Torneio','World','High School','Conference','Under 19','U-19','U-20']
                for ls in lists:
                    if ls in tour_info[3] :
                        continue'''

                
                tou_id, tou_gid, leagune_gid, tou_title = tour_info
                link = 'http://10.28.218.80/tools/guid_page.py?gid=%s' % (leagune_gid)
                wiki_gid = '<a href=%s>%s</a>' % (link, leagune_gid)
                wiki_gid = str(wiki_gid)
                cou = json.loads(_info).get(leagune_gid, '')
                wk_gid = str(leagune_gid)
                tu_gid = str(tou_gid)
                tu_id  = str(tou_id)
                if cou == u'Division 1: 6 Division 2: 10' :
                    cou = int(cou.split(':')[2])+int(cou.split(':')[1][1])
                if tou_title == "NCAA Division II Football":
                    cou = 163
                if tou_title == "Hong Kong Premier League":
                    cou = 5
                if tou_title == "NCAA Men's Division I Hockey":
                    cou = 83
                if tou_title == "NCAA Men's Division III Hockey":
                    cou = 56
                if tou_title == "NCAA Men's Division I Volleyball":
                    cou = 22
                if tou_title == "NCAA Men's Division III Volleyball":
                    cou = 102
                if tou_title == "Minor League Baseball Triple-A":
                    cou = 47
                if tou_title == "Minor League Baseball Rookie":
                    cou = 30
                if tou_title == "Minor League Baseball Class A Short Season":
                    cou = 22
                if tou_title == "Minor League Baseball Class A-Advanced":
                    cou = 30
                if tou_title == "Minor League Baseball Double-A":
                    cou = 30
                if tou_title == "Minor League Baseball Class A":
                    cou = 30
                if tou_title == "Minor League Baseball Rookie Advanced":
                    cou = 18
                if tou_title == "Premier Division":
                    cou = 24
                if tou_title == "Division One North":
                    cou = 24
                if tou_title == "Division One South":
                    cou = 24
                if tou_title == "NCAA Men's Division I Soccer":
                    cou = 179
                if tou_title == "NCAA Men's Division II Soccer":
                    cou = 194
                
                tournaments_ignore_list = ['A2 Ethniki Volleyball',"Slovakia Men's Volleyball League",'Azerbaijan Volleyball League','Regionalliga','Northern Football League (Australia)',"Czechoslovak First Ice Hockey League","2nd Czech Republic Hockey League","Karjala Tournament","Regionalliga (ice hockey)","Lithuania Hockey League","Russian Hockey Second League","Sweden Hockey Games","TV-pucken","Ukrainian Hockey Extra League",'Ecuadorian Segunda Categoria','Campeonato Baiano',"Portuguese Volleyball Second Division","Ukrainian Men's Volleyball Super League",'Fortescue National Football League','West Australian State Premiership','Adelaide Footy League','AFL Queensland','Australian rules football in South East Queensland','Australian rules football leagues in regional Queensland','Campeonato Cearense','Campeonato Goiano','Campeonato Paulista','Campeonato Piauiense','Campeonato Potiguar','Campeonato Catarinense','Campeonato Mato-Grossense','Campeonato Mineiro','Campeonato Maranhense','Campeonato Paraense','Campeonato Paranaense','Campeonato Pernambucano','ACTRU Premier Division','Dewar Shield','Campeonato Nacional de Clubes (Venezuela)','Campeonato Paraense Second Division','Recopa Sul-Brasileira','Tercera B','Tercera B (Chile)','Gridiron Victoria','ACT Gridiron','Czech League of American Football','American football Regionalliga','German Junior Bowl','German Bowl','Rocky Mountain Lacrosse League','Prairie Gold LacrosseLeague','Thompson Okanagan Senior Lacrosse League','OLA Senior B Lacrosse League','Western Lacrosse Association','West Coast Senior Lacrosse Association','Salibandyliiga','National League A (Floorball)','Flemish American Football League','Finnish 1st Division (American football)','Vaahteraliiga','Central European Football League','American Football Wellington','AFBN Division One','BritishUniversities American Football League','Australian Schools Championship','Claxton Shield','Australian Schools Championship (baseball)','Kansai Independent Baseball League','Eastern League (Japanese baseball)','Western League (Japanese baseball)','Taiwan Baseball Summer League','Nova Scotia Senior Baseball League','Grand Forks International','Dominican Summer League','Gulf Coast League','Prairie Gold Lacrosse League','T20 Global League','Stanbic Bank 20 Series','Major League Cricket','American Twenty20 Championship','NYPD Cricket League','Friends Provident Trophy',"Greek Men's Cricket Championship",'Scottish National Cricket League','Mini Indian Premier League','Rohinton Baria Trophy','South Australian Grade Cricket League','Premier Limited Overs Tournament','ACC Premier League',"NCAA Men's Water Polo Championship",'Mongolia Hockey League','Turkmenistan Championship (ice hockey)',"NCAA Women's Division I Hockey","NCAA Women's Division III Hockey",'Minas Gerais Volleyball Championship','Bartercard Premiership','NBA Summer League','NBA Development League',"Women's National Basketball Association",'Liga de Desenvolvimento de Basquete','Nigeria Basketball Federation','Libyan Arab Basketball Federation',"NCAA Men's Division II Hockey",'United States Premier Hockey League',"NCAA Division II Men's Lacrosse Championship","NCAA Division III Men's Lacrosse Championship",'Philippine Collegiate Champions League','Division 1-A Rugby','USA Rugby Club 7s','Nestea Beach Volleyball','Philippine Super Liga','PVF Beach Volleyball tournament','Beach Volleyball Republic','Volleyball Thai-Denmark Super League','Dominican Republic Volleyball League','Dominican Republic National Beach Volleyball Tour','USAFL Western Regionals','Ligue Nationale de Handball','Portuguese Handball Fourth Division','Eric Shirley Shield','Blackrock Rugby Festival',"East Men's League","Yorkshire Men's League",'New Caledonia Super Ligue','New Caledonia Second Level','Lotto Sport Italia NRFL Division 1','Lotto Sport Italia NRFL Division 2','Campeonato Carioca',"U Sports men's ice hockey",'Championnat de France (water polo)',"Serie A2 (men's water polo)","Serie B (men's water polo)",'Adriatic Water Polo League','Kuwaiti Futsal League',"Kyrgyzstan Futsal League","DBKL Youth Futsal League","IPT Futsal League","Red Bull Futsal League","Vietnam Futsal League","Milo President's Trophy Knockout Tournament",'COBRA Rugby Tens','ARFU Asian Rugby Series','Inter-District Championship','Kiribati National Championship','New Zealand Football Championship','Papua New Guinea Overall Championship','Papua New Guinea National Club Championship','Nationale 1','A2 Ethniki Water Polo',"Northern Premier League","Southern Football League","Combined Counties Football League","East Midlands Counties Football League","Eastern Counties Football League","Essex Senior Football League","Hellenic Football League","Midland Football League" ,"Northern Counties East Football League" ,"Northern Football League" ,"North West Counties Football League","Southern Counties East Football League","South West Peninsula League","Spartan South Midlands Football League","Southern Combination Football League","United Counties League","Wessex Football League","Western Football League","West Midlands (Regional) League","FA WSL 1","Sheffield & Hallamshire County Senior Football League","RFL Reserve Championship","Mercian Regional Football League","Northern Premier League Division One North","Northern Premier League Division One South","Northern Premier League Premier Division","Bristol and Avon Association Football League","Teesside Football League","FA WSL 2" ,"FA WSL","Gloucestershire County Football League","North Bucks & District Football League" ,"West Lancashire Football League","Mid-Somerset Football League","Bedfordshire County Football League","Hope Valley Amateur League","Craven and District Football League","Basingstoke and District Football League","Kent County League","West Yorkshire Association Football League","Coventry Alliance Football League","Surrey Elite Intermediate Football League","West Cheshire Association Football League","Perry Street and District League","Cheltenham Association Football League","St. Edmundsbury Football League","Wearside Football League" ,"Cirencester and District League","Portsmouth Saturday Football League","Duchy League","Middlesex County Football League" ,"Nottinghamshire Senior League","Arthurian League","Plymouth and West Devon Football League","Aldershot & District Football League","Midlands Regional Alliance","Anglian Combination","Banbury District and Lord Jersey FA","Trelawny League","Liverpool Old Boys' Amateur Football League","Hampshire Premier League" ,"East Cornwall League" ,"Southampton Saturday Football League","Central and South Norfolk League","Witney and District League","North Devon Football League","Brighton, Worthing & District Football League","Central Midlands Football League","East Berkshire Football League","Huddersfield and District Association Football League","Halifax and District Association Football League","North Lancashire and District Football League","Yeovil and District League","Norwich and District Saturday Football League","Southern Amateur Football League","Great Yarmouth and District Football League","Herefordshire Football League","North East Norfolk League","Andover and District Football League","Bristol Downs Association Football League","Bromley and District Football League","South London Football Alliance","Portuguese Volleyball First Division","National Junior Hockey League","Vysshaya Liga (ice hockey)","Scottish Premier Hockey League","Slovak 1. Liga","Hockeyettan","J20 SuperElit","J20 Elit","Oberliga (ice hockey)","Swiss 1. Liga (ice hockey)","Austrian Landesliga","Austrian 2. Landesliga","Austrian Basketball Supercup","A1 Liga","Alain Gilles Trophy","2. Basketball Bundesliga","Greek C Basket League","Hellenic Basketball Clubs Association","HEBA Greek All Star Game","Second Division Men (Icelandic basketball)","Liga Artzit (basketball)","Lega Basket All Star Game","Regional Basketball League","Lithuanian Students Basketball League","Regional Basketball League (Lithuania)","NBB-Beker","Istanbul Basketball League","Turkish Wheelchair Basketball Super League","Turkish Basketball Super League","Championnat LNB","Serbian First League (basketball)","Serbian First League (basketball)","Basketball League of Serbia B","Serie B Basket","I Liga (basketball)","Russian Professional Basketball League","USSR Premier Basketball League","Serie A2 Basket","Euskal Kopa","Liga EBA","English Basketball League","Campeonato Brasileiro Série D","Argentine División Intermedia","Paraguayan Tercera División","Paraguayan Cuarta División","Taça Brasil","Campeonato Brasileiro de Seleções Estaduais","Taça da Prefeitura do Distrito Federal","Ecuadorian Segunda Categoría","Venezuelan Segunda División B","Venezuelan Tercera División","Campeonato Carioca (lower levels)","Campeonato Paulista Série A2","Campeonato Paulista Segunda Divisão","Campeonato Sergipano Série A2","Campeonato da Cidade de Campos","Campeonato Fluminense","Primera División de Baloncesto","Lietuvos moksleivių krepšinio lyga","Divisão de Elite","Kulcsár Anita-emléktorna","Federaci ón Argentina de Football","Asociación Amateurs de Football","Liga Argentina de Football","Liga Rosarina de Football","Taça Victorino Cunha","António Pratas Trophy","Supercopa de España de Balonmano Femenino","Campeonato Argentino de Básquet","Ligue Régional II","Ligue de Football de la Wilaya","Angolan Provincial Stage","Botswana First Division North","Botswana First Division South","Egyptian Fourth Division","Canal Zone League","Kenyan Regional Leagues","Kenyan County Champions League","Kenyan Sub-County Leagues","GNFA 1","SAFA Regional League","South African Soccer League","NSL Second Division","Khartoum League","Zimbabwe Division 1","Federación Argentina de Football","Somali Second Division","Somali Third Division","Tanzanian First Division League","Tanzanian Second Division League","Nigeria National League","Nigeria Nationwide League","Nigeria Amateur League Division Two","Rwandan Second Division","Inter-Régions Division","Ligue Régional I","Gira Angola","Elite Two","Santiago Island League (South)","FKF Division One","Libyan Second Division","Zambian Division One","Kenyan National Super League","Libyan Third Division","Botola 2","South African Premier Division","National First Division","SAFA Second Division","Tunisian Ligue Professionnelle 2","Tunisian Ligue Professionnelle 3","FUFA Big League","Togolese Championnat National","Grand Bahama Football League","New Providence Football League","Belize Premier Football League","U Sports men's soccer","Ontario Soccer League","Croatian-North American Soccer Tournament","Tercera Division de Costa Rica","Segunda División de El Salvador","Tercera Division de Fútbol Salvadoreño","GFA First Division","Primera División de Ascenso","Segunda División de Ascenso","Honduran Liga Nacional de Ascenso","South Central Confederation Super League","Eastern Confederation Super League","Western Confederation Super League","KSAFA Major League","Segunda División de México","Tercera División de México","Liga Premier de Ascenso","Premier Arena Soccer League","Tercera División de Nicaragua","Saint Kitts and Nevis Division 1","SKNFA Super League","Saint Lucia Silver Division","United States Adult Soccer Association","Premier Development League","National Premier Soccer League","American Premier Soccer League","United Premier Soccer League","Major Arena Soccer League","Premier Arena Soccer League","Professional Futsal League","Major League Futsal","NCAA Division II Men's Soccer Championship","NCAA Division II Men's Soccer Championship","United Soccer Association","National Women's Soccer League","Champions Soccer League USA","Barbados First Division","Super League of Belize","Super-20 League","League1 Ontario","Ligue de Soccer Elite Quebec","Canadian Premier League","Pacific Coast Soccer League","U Sports men's soccer championship","Soccer at the Canada Games","Alberta Major Soccer League","Canadian Soccer League championship final","Kitchener and District Soccer League","Vancouver Metro Soccer League","Segunda División de Costa Rica","Primera División de Republica Dominicana","Dominican Football Federation","Primera División Reserves (El Salvador)","Liga Nacional de Fútbol de Honduras Reserves","Liga Mayor de Futbol de Honduras","Honduras Primera Division","Segunda División de Nicaragua","KSAFA Super League","Liga Nacional de Ascenso","Saint Kitts Premier Division","NLA First Division Club Championship","National Super League","North American Soccer League","United Soccer League","Premier League of America","NCAA Division III Men's Soccer Championship","NAIA Men's Soccer Championship","Albanian Second Division","Albanian Third Division","Segona Division","Austrian Regional League","Austrian Regional League East","Austrian Regional League Central","Austrian Regional League West","ÖFB-Frauenliga","AFFA Amateur League","Belarusian First League","Belarusian Second League","Belarusian Premier League Reserves Championship","Belgian First Amateur Division","Belgian Third Amateur Division","Belgian Provincial Leagues","Belgian Fourth Division","Belgian Third Division","Second League of the Federation of Bosnia and Herzegovina","Regional Amateur Football Groups","Bulgarian Republic Football Championship","Bulgarian State Football Championship","Regional Amateur Football Groups (Bulgaria)","Croatian Third Football League","First County Football League","First County Football League (Croatia)","First League of Primorje-Gorski Kotar County","Inter-county League Rijeka","Croatian Women's First Football League","Cypriot Third Division","STOK Elite Division","Bohemian Football League","Czech Fourth Division","Prague Championship","Moravian–Silesian Football League","Danish 2nd Division","Denmark Series","Elitedivisionen","Esiliiga B","II liiga","III liiga","IV liiga","Estonian Football Winter Tournament","Naiste Meistriliiga","2. deild","3. deild","1. deild kvinnur","Ykkonen","Kakkonen","Kolmonen","Nelonen","Vitonen","Kutonen","Seiska","Seiska (football)","Nelonen (football)","Naisten Liiga","Division d'Honneur","Martinique Championnat National","Ligue des Antilles","Réunion Premier League","Division 1 Féminine"]
                if tou_title in tournaments_ignore_list:
                    continue

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

                if isinstance(tou_title, unicode):
                    tou_title = '<font color="brown">%s</font>' % str(tou_title.encode('utf-8'))
                else:
                    try:
                        tou_title = '<font color="brown">%s</font>' % str(tou_title.decode('utf-8').encode('u8'))
                    except:
                        tou_title = '<font color="brown">%s</font>' % str(tou_title.encode('u8'))
            

                #tou_title = '<font color="brown">%s</font>' % str(tou_title.encode('utf-8'))
            
                headers = ('Index','Id', 'Gid', 'Title', 'Short_title', 'Display_title', 'Aka')

                count =  len(values)
                if len(values) ==1:
                    count = 0
                if count == cou == 0:
                    self.countzero.write('%s %s %s Count: %s/%s\n' % (wk_gid, tu_gid, tu_id, count, cou))
                zerocount = count
                if title in self.north_america:
                    if count > 200:
                        cou = str(count)
                    if zerocount == 0:
                        naz = naz + 1
                    if zerocount == 0:
                        cnorthamericazero = cnorthamericazero + 1
                    self.northamerica_country_zero.update({title: cnorthamericazero})
                    if zerocount != 0:
                        cnorthamericanonzero = cnorthamericanonzero + 1
                    self.northamerica_country_nonzero.update({title: cnorthamericanonzero})
                    if str(len(values)) == cou:
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
                    if count > 200:
                        cou = str(count)
                    if zerocount == 0:
                        saz = saz = saz + 1
                    if zerocount == 0:
                        csouthamericazero = csouthamericazero + 1
                    self.southamerica_country_zero.update({title: csouthamericazero})
                    if zerocount != 0:
                        csouthamericanonzero = csouthamericanonzero + 1
                    self.southamerica_country_nonzero.update({title: csouthamericanonzero})
                    if str(len(values)) == cou:
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
                    if count > 200:
                        cou = str(count)
                    if zerocount == 0:
                        afz = afz + 1
                    if zerocount == 0:
                        cafricazero = cafricazero + 1
                    self.africa_country_zero.update({title: cafricazero})
                    if zerocount != 0:
                        cafricanonzero = cafricanonzero + 1
                    self.africa_country_nonzero.update({title: cafricanonzero})
                    if str(len(values)) == cou:
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
                    if count > 200:
                        cou = str(count)
                    if zerocount == 0:
                        asz = asz + 1
                    if zerocount == 0:
                        casiazero = casiazero + 1
                    self.asia_country_zero.update({title: casiazero})
                    if zerocount != 0:
                        casianonzero = casianonzero + 1
                    self.asia_country_nonzero.update({title: casianonzero})
                    if str(len(values)) == cou:
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
                    if count > 200:
                        cou = str(count)
                    if zerocount == 0:
                        euz = euz + 1
                    if zerocount == 0:
                        ceuropezero = ceuropezero + 1
                    self.europe_country_zero.update({title: ceuropezero})
                    if zerocount != 0:
                        ceuropenonzero = ceuropenonzero + 1
                    self.europe_country_nonzero.update({title: ceuropenonzero})
                    if str(len(values)) == cou:
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
                    if count > 200:
                        cou = str(count)
                    if zerocount == 0:
                        ocz = ocz + 1
                    if zerocount == 0:
                        coceaniazero = coceaniazero + 1
                    self.oceania_country_zero.update({title: coceaniazero})
                    if zerocount != 0:
                        coceanianonzero = coceanianonzero + 1
                    self.oceania_country_nonzero.update({title: coceanianonzero})
                    if str(len(values)) == cou:
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
                    t_str = html_obj.get_html_table_with_rows(headers, values)
                    at_str = html_obj.get_html_table_with_rows(headers, values)
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
                if str(len(values)) != dcou :
                    d_count_page.add('<h4>%s %s %s %s Count: %s/%s</h4>' %(wiki_gid, tou_gid, tou_id, tou_title, len(values), cou.encode('utf-8')))
                    dt_str = d_html_obj.get_html_table_with_rows(headers, values)
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
        total = 0
        matched = 0
        self.total = []
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
        totigc = nac + sac + afc + asic + ocec + eurc
        total = euleagc1 + afleagc1 + asleagc1 + naleagc1 + saleagc1 + ocleagc1
        matched = eugc + afgc + asgc + nagc + sagc + ocgc
        self.total.append((matched,total,ct))
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

        '''self.igfile = {}
        main_html_obj = VtvHtml()
        main_page = main_html_obj.create_page('Ignore Report: %s' % (datetime.datetime.now().date()))
        main_html_obj.page.add('<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/></head>')
        main_html_obj.container_open()
        report_file_format = '%s_ignore_report_%s.html'
        for key, obj in self.ignlist.iteritems():
            page_str = obj.get_page_str()
            main_html_obj.page.add(page_str)
        headers = ['Country','Wiki','Title']
        t_str = main_html_obj.get_html_table_with_rows(headers,'')
        main_page.add(t_str)
        for val in self.ignlist:
             
            t_str = main_html_obj.add(val)
            main_page.add(t_str)
        report_file_name = report_file_format % (spt, self.today_str)
        report_file_name = os.path.join('/data/REPORTS/SPORTS/', report_file_name)
        page_str = str(main_page)
        open(report_file_name,'w').write(page_str)
        copy_file(report_file_name, os.path.join('/data/REPORTS/SPORTS/', report_file_format % (spt,'latest')), self.logger)
        self.igfile.setdefault(report_file_name)'''



    def get_temp_details(self, sport_id, spt):
        self.samerica_continent = {}
        self.africa_continent = {}
        self.namerica_continent = {}
        self.asia_continent = {}
        self.oceania_continent = {}
        self.europe_continent = {}
        self.roster_template_dict = OrderedDict()
        self.roster_template_domestic_dict = OrderedDict()
        self.tour_details = OrderedDict()
        self.titlesdomestic = []
        self.wiki_dict = {}

        _data = open('wiki_sports_tournaments_from_templates.json', 'r+')
        _info = open('tournament_counts.json').read()
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

            count  = 0
            leagues_list = ['league','domestic']
            major_leaguesall = []
            for league in leagues_list[::1]:
                if league == 'league':
                    major_leagues =  _data.values()[0].get('tournament', '').get(league, '').get('men', '')
                else:
                    major_leagues =  _data.values()[0].get('tournament', '').get(league, '').values()
                    legs = major_leagues[1]
                    for leg in legs:
                        leg = leg.split('{')[0].strip()
                        self.leh.append(leg) 
                         
                    '''
                    major_leagues =  _data.values()[0].get('tournament', '').get(league, '').get('men', '')
                    major_leagues1 = major_leagues1.values()
                    for major_league in major_leagues1:
                        if major_league in major_leagues:
                            continue
                        else:
                            major_leaguesall.append(major_league)'''
                for mg_leagues in major_leagues[::1]:
                    league_name = mg_leagues
                    if league_name == []:
                        continue
                    try:
                        leagune_nm = league_name.split('{')[0].strip()
                    except:
                        leagune_nm = league_name[0].split('{')[0].strip()
                        self.titlesdomestic.append(leagune_nm)
                    try:
                        leagune_gid = league_name.split('{')[-1].strip().replace('}', '')
                    except:
                        leagune_gid = league_name[0].split('{')[-1].strip().replace('}', '')
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
                        if league != 'domestic':
                            self.roster_template_dict.setdefault(con_name, OrderedDict()).setdefault(('', '', league_gid, leagune_nm), []).append(['', '', '', '', '', ''])
                            self.tour_details.setdefault(con_name, []).append(['', '', league_gid, leagune_nm])
                        else:
                            self.roster_template_domestic_dict.setdefault(con_name, OrderedDict()).setdefault(('', '', league_gid, leagune_nm), []).append(['', '', '', '', '', ''])
                            self.tour_details.setdefault(con_name, []).append(['', '', league_gid, leagune_nm])

                    if tou_id:
                        typ_qry = 'select type from sports_tournaments where id = %s'
                        values = (tou_id)
                        self.cursor.execute(typ_qry, values)
                        typ_data = self.cursor.fetchall()
                        if typ_data[0][0] != "tournament":
                            continue
                        data_qry = 'select P.id, P.gid, T.title, P.title, L.short_title, L.display_title, P.aka, P.created_at, T.created_at from sports_participants P, sports_teams L, sports_tournaments T where P.id = L.participant_id and L.tournament_id = T.id and L.tournament_id = %s and T.type = "tournament" order by T.title'
                        values = (tou_id)
                        self.cursor.execute(data_qry, values)
                        tou_data = self.cursor.fetchall()
                        if tou_data:
                            firstnode = tou_data[0]
                            last_node = tou_data[-1]
                        if not tou_data or (firstnode == last_node):
                            data_qry = 'select P.id, P.gid, T.title, P.title, L.short_title, L.display_title, P.aka, P.created_at, T.created_at from sports_participants P, sports_teams L, sports_tournaments T, sports_tournaments_participants TP where P.id = L.participant_id and TP.tournament_id = T.id and TP.tournament_id = %s and TP.participant_id=L.participant_id and P.id=TP.participant_id and T.type = "tournament" order by T.title'
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
                            if league != 'domestic':
                                self.roster_template_dict.setdefault(con_name, OrderedDict()).setdefault((tou_id, tou_gid, league_gid, leagune_nm), []).append(['', '', '', '', '', ''])
                                self.tour_details.setdefault(con_name, []).append([tou_id, tou_gid, league_gid, leagune_nm])
                            else:
                                self.roster_template_domestic_dict.setdefault(con_name, OrderedDict()).setdefault((tou_id, tou_gid, league_gid, leagune_nm), []).append(['', '', '', '', '', ''])
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
                            if league != 'domestic':
                                self.roster_template_dict.setdefault(con_name, OrderedDict()).setdefault((tou_id, tou_gid, leagune_gid, Tou_title.decode('u8')), []).append([Team_id, Team_gid, Team_title.decode('u8'), Team_st.decode('u8'), Team_dt.decode('u8'), aka.decode('u8')])
                                self.tour_details.setdefault(con_name, []).append([tou_id, tou_gid, leagune_gid, Tou_title.decode('u8')])
                            else:
                                self.roster_template_domestic_dict.setdefault(con_name, OrderedDict()).setdefault((tou_id, tou_gid, leagune_gid, Tou_title.decode('u8')), []).append([Team_id, Team_gid, Team_title.decode('u8'), Team_st.decode('u8')
, Team_dt.decode('u8'), aka.decode('u8')])
                                self.tour_details.setdefault(con_name, []).append([tou_id, tou_gid, leagune_gid, Tou_title.decode('u8')])
    
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
        conticount = 0 
        counts = [self.europecount, self.northamericacount, self.southamericacount, self.africacount, self.asiacount, self.oceaniacount]
        for co in counts:
            if co.values()[0][0] == co.values()[0][1]:
                conticount = conticount + 1
        headers = ['Sports', 'Africa', 'Asia', 'Europe', 'North America', 'South America', 'Oceania']
        self.files = self.files.keys()
        self.files = self.files[0].replace('/data', '')
        title = "<a href='%s'>%s</a>" % ((self.files), title)
        row = [''] * 7
        passed = 0
        total = 0
        mis = 0 
        nonzero = 0
        count = 0
        zeroes, z = 0, 0
        toti ,counti = 0, 0
        totii = 0
        totiii = 0
        for c_name in headers:
            record = self.rep.get(c_name,'')
            #report_file_name,count = record
            if not record:
                continue
            row[0] = title
            for count in counts:
                if count == {}:
                    continue
                cont = count.keys()[0]
                if cont == c_name:
                    #if self.total[0][0] ==  self.total[0][1]:
                    matchedleague = self.total[0][0]
                    matchedleague1 = self.total[0][0]
                    totalleague1 = self.total[0][1]
                    totalleague = self.total[0][1]
                    totalignore = self.total[0][2]
                    total = count.values()[0][1] 
                    passed = count.values()[0][0]
                    zeroes = count.values()[0][2]
                    nonzero = count.values()[0][3]
                    mis = (total)-(passed + zeroes )
                    if mis < 0:
                        mis = 0
                    nonzero = "<font color='grey'>%s</font>" % str(nonzero)
                    #passed = "<font color='green'>%s</font>" % str(passed)
                    toti = "<font color='green'>%s</font>" % str(total)
                    pas = passed
                    if passed == total:
                        passed = self.get_status_color('Success')
                    elif passed == 0:
                        passed = self.get_status_color('Fail')
                    elif passed != total:   
                        passed = self.get_status_color('Incomplete')
                    zeroes = "<font color='red'>%s</font>" % str(zeroes)
                    totii = "<font color='red'>%s</font>" % str(total)
                    mis = "<font color='orange'>%s</font>" % str(mis)
                    totiii = "<font color='orange'>%s</font>" % str(total)
                     
                    
            report_file_name,count = record
            report_file_name = report_file_name.split('/')[-1]
            counti = "<font color='grey'>%s</font>" % str(count)
            c = "<a href='%s'>%s</a>" %(report_file_name, count)
            nz = "<a href='%s'>%s/%s</a>" %(report_file_name, nonzero, counti)
            strii =  passed.split('>')[1].split('<')[0]
            if strii == 'Incomplete':
                pas = '%s/%s' %(pas,total) 
                passed = passed.replace(strii,str(pas))
            elif strii == 'Success':
                pas = '%s/%s' %(pas,total)
                passed = passed.replace(strii,str(pas))
            elif strii == 'Fail':
                pas = '%s/%s' %(pas,total)
                passed = passed.replace(strii,str(pas))
            
            m = "<a href='%s'>%s</a>" %(report_file_name, passed)
            if zeroes != "<font color='red'>0</font>":
                z = "<a href='%s'>%s/%s</a>" %(report_file_name, zeroes, totii)
            else:
                z = ''
            if mis != "<font color='orange'>0</font>":
                mis = "<a href='%s'>%s/%s</a>" %(report_file_name, mis, totiii)
            else:
                mis = ''
            #report_file_name = ["<a href='%s'>%s</a>" %(report_file_name, count), "<a href='%s'>%s/%s</a>" %(report_file_name, nonzero, counti), "<a href='%s'>%s/%s</a>" %(report_file_name, passed, toti), "<a href='%s'>%s/%s</a>" %(report_file_name, zeroes, totii), "<a href='%s'>%s/%s</a>" %(report_file_name, mis, totiii)]
            #report_file_name = [c,nz,m,z,mis]
            report_file_name = [nz,m,mis]
            leaguecount = ['%s/%s' %(matchedleague, totalleague)]
            if matchedleague == totalleague:
                matchedleague = self.get_status_color('Success')
            elif (totalleague-matchedleague) < 5:
                matchedleague = self.get_status_color('Incomplete')
            else:
                matchedleague = self.get_status_color('Fail')
            striii = matchedleague.split('>')[1].split('<')[0]
            if striii == 'Incomplete':
                leaguecount = matchedleague.replace(striii,str(leaguecount[0]))
            elif striii == 'Success':
                leaguecount = matchedleague.replace(striii,str(leaguecount[0]))
            elif striii == 'Fail':
                leaguecount = matchedleague.replace(striii,str(leaguecount[0]))
            row[headers.index(c_name)] = report_file_name
        row_length = [x for x in row if x]
        if len(row_length) != 0:
            if matchedleague1 == totalleague1:
                status = 'Success'
            elif matchedleague1 == 0:
                status = 'Fail'
            else:
                status = 'Incomplete'
            statscolor = self.get_status_color(status)   
            sub_list = [[''] * 3 if not x else x for x in row]
            final_team_list = [ l for li in sub_list[1:] for l in li ]
            totalignore = ['%s' %self.total[0][2]]
            igfile = self.igfile.keys()[0].replace('/data','')
            totalignore = "<a href='%s'>%s</a>" % ((igfile), totalignore[0])
            totalconti = 6
            if conticount == 6:
                conticountflag = self.get_status_color('Success')
            elif conticount == 5:
                conticountflag = self.get_status_color('Incomplete')
            else:
                conticountflag = self.get_status_color('Fail')
            con1 = conticountflag.split('>')[1].split('<')[0]
            if con1 == 'Fail':
                conticounts = ['%s/%s' %(conticount,totalconti)]
                conticount = conticountflag.replace(con1,str(conticounts[0]))
            elif con1 == 'Success':
                conticounts = ['%s/%s' %(conticount,totalconti)]
                conticount = conticountflag.replace(con1,str(conticounts[0]))
            elif con1 == 'Incomplete':
                conticounts = ['%s/%s' %(conticount,totalconti)]
                conticount = conticountflag.replace(con1,str(conticounts[0]))
            
            final_team_list = [sub_list[0]] + [leaguecount] + [conticount] + [totalignore]+ [statscolor] + [ l for li in sub_list[1:] for l in li ]
            self.final_team_list.append(final_team_list)
         
 
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
        main_html_obj.page.h2(' All Sports')
        main_html_obj.page.h4('C: Countries , M: Matched, Z: Zeroes , Mis: Mismatch, NZ: Non Zero Countries')
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
        
    def get_status_color(self, status):
        COLOR_DICT = { 'Success' : 'badge-success', 'Fail' : 'badge-important', 'Incomplete' : 'badge-warning' }

        return '<span class="badge %s">%s</span>' % (COLOR_DICT.get(status, ''), status)

    def cleanup(self):
        self.move_logs(DATA_LOGS, [ ('.', 'tournaments_report*.log') ])
        self.remove_old_dirs(DATA_LOGS, self.logs_dir_prefix, self.log_dirs_to_keep, check_for_success=False)


    def run_main(self):
        self.sport_wise_stats()
        self.final_team_list = sorted(self.final_team_list)
        jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader(os.getcwd()))
        table_html = jinja_environment.get_template('tournaments_report.jinja').render(today_date = datetime.datetime.now(), big_lists=self.final_team_list)
        codecs.open(os.path.join('/data/REPORTS/SPORTS/', 'all_sports_summary_report_latest.html'), 'w', 'utf8').write(table_html)
        codecs.open(os.path.join('/data/REPORTS/SPORTS/', 'all_sports_summary_report_%s.html' % self.today_str), 'w', 'utf8').write(table_html)


if __name__ == '__main__':
    vtv_task_main(TournamentsTeamsReport)



