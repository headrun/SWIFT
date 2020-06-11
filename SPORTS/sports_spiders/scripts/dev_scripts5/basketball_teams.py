from vtvspider import VTVSpider, extract_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
import MySQLdb
import urllib


GAME = "hockey"

PL_NAME_QUERY = 'select id from sports_participants where \
title like "%s" and game="%s" and participant_type="team"'

INSERT_SK = 'insert into sports_source_keys (entity_id, entity_type, \
source, source_key, created_at, modified_at) \
values("%s", "%s", "%s", "%s", now(), now())'

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="ncaa_nch" and source_key= "%s"'

TEAM_URL = """http://10.4.18.34/cgi-bin/add_teams.py?name=%s&aka=%s&game=%s&participant_type=%s&img=%s&bp=%s&ref=%s&loc=5166&short=%s&callsign=%s&category=%s&kws=%s&tou_name=%s&division=%s&gender=%s&formed=%s&tz=%s&logo=%s&vtap=%s&yt=%s&std=0&src=%s&sk=%s&action=submit"""

def create_cursor():
    con = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP")
    cursor = con.cursor()
    return con, cursor

def get_team_name(name):

    team_name = name
    for key, value in TEAM_DICT.iteritems():
        if key == name:
            team_name = value
    return team_name

def check_title(name):
    con, cursor = create_cursor()
    team_name = "%" + name + "%"
    cursor.execute(PL_NAME_QUERY % (team_name, GAME))
    pl_id = cursor.fetchall()
    con.close()
    return pl_id

def check_sk(pl_sk):
    con, cursor = create_cursor()
    cursor.execute(SK_QUERY % pl_sk)
    entity_id = cursor.fetchone()
    if entity_id:
        pl_exists = True
    else:
        pl_exists = False
    con.close()
    return pl_exists

aka, bp = "", "-150"
source = "ncaa_nch"
#source = "ncaa_ncw"
tou = "NCAA College Hockey"
#tou = "NCAA Women's Division I Basketball"
category = "ncaa-nch"
#category = "ncaa-ncw"
par_type = "team"
game = "hockey"
gender = "men"
#gender = "women"
formed = "="
vtap = ""
you_tube = ""
logo_url = ""
tz = ""


class NCAATeamss(VTVSpider):
    name = "ncaa_teams"

    start_urls = ['http://www.ncaa.com/schools/princeton']
    allowed_domains = ['www.ncaa.com']
    urls = []
    teams = open("missed_teams", "a+")


    def parse(self, response):
        hxs = Selector(response)
        name = extract_data(hxs, '//span[@class="school-name"]/text()')
        location = extract_data(hxs, '//li[span[contains(text(), "Location")]]/text()')
        nickname = extract_data(hxs, '//li[span[contains(text(), "Nickname")]]/text()')
        image = extract_data(hxs, '//span[@class="school-logo"]//@src')
        team_sk = response.url.split('/')[-1]
        con, cursor = create_cursor()
        pl_exists = check_sk(team_sk)
        if pl_exists == False:
            if nickname:
                if name.startswith('University of'):
                    t_name = name.replace('University of ', '') + " " + nickname + " men's ice hockey"

                    short_title = name.split('University of ')[1]
                elif name.startswith('University'):
                    short_title = name.split('University')[1]
                    t_name = name.replace('University ', '') + " " + nickname+ " men's ice hockey"

                else:
                    short_title = name.split('University')[0]
                    t_name = name.replace(' University', '') + " " + nickname
                    t_name = name.replace(' University', '') + " " + nickname + " men's ice hockey"
                pt_ids = check_title(t_name)
                if not pt_ids:
                    pt_ids = check_title(t_name)
                if not pt_ids:
                    pt_ids = check_title(t_name.replace('(Pa.)', '(pa)').replace('(Mo.)', '(Mo)'))
                if len(pt_ids) == 1:
                    res = [str(i[0]) for i in pt_ids]
                    values = (res[0], 'participant', 'ncaa_nch', team_sk)
                    query = INSERT_SK % values
                    cursor.execute(query)
                    con.close()
                else:
                    if not pt_ids:
                        import pdb;pdb.set_trace()
                        #name_ = t_name + " men's basketball"
                        name_ = t_name
                        team_values = (name_, aka, game, par_type, image, bp, response.url, nickname, '', category, "", tou, "", gender, formed, tz, logo_url, vtap, you_tube, source, team_sk)
                        if name:
                            pl_add_url = (TEAM_URL % team_values).replace('\n', '')
                            urllib.urlopen(pl_add_url)
                            print  "Added team ", name
                        self.teams.write("%s\n" % t_name)
            else:
                self.false.write("%s\n" % response.url)
        else:
            if not name:
                name = response.url
                self.true.write("%s\n" % name)
