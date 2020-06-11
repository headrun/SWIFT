from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
import MySQLdb
import urllib

CURSOR = MySQLdb.connect(host="10.4.18.34", user="root",
db="SPORTSDB_BKP").cursor()

GAME = "basketball"

PL_NAME_QUERY = 'select id from sports_participants where \
title like "%s" and game="%s" and participant_type="team"'

INSERT_SK = 'insert into sports_source_keys (entity_id, entity_type, \
source, source_key, created_at, modified_at) \
values("%s", "%s", "%s", "%s", now(), now())'

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="ncaa_ncb" and source_key= "%s"'


def get_refined_name(name):
    if "State College" in name:
        team_name = name.replace('State College', 'State')
    else:
        team_name = name.replace('College of the ', '').replace('State College', 'State').\
                replace('College of ', '').replace('College ', '').\
                replace('State ', '').replace(' and ', " & ").replace('Univ. ', '').\
                replace("Fightin' ", '').replace('The ', '').\
                replace('School of ', '')
    return team_name

def get_team_name(name):
    team_name = name
    for key, value in TEAM_DICT.iteritems():
        if key == name:
            team_name = value
    return team_name

def check_title(name):
    team_name = "%" + name + "%"
    CURSOR.execute(PL_NAME_QUERY % (team_name, GAME))
    pl_id = CURSOR.fetchall()
    return pl_id

def check_sk(pl_sk):
    CURSOR.execute(SK_QUERY % pl_sk)
    entity_id = CURSOR.fetchone()
    if entity_id:
        pl_exists = True
    else:
        pl_exists = False
    return pl_exists


TEAM_URL = """http://10.4.18.34/cgi-bin/add_teams.py?name=%s&aka=%s&game=%s&participant_type=%s&img=%s&bp=%s&ref=%s&loc=5166&short=%s&callsign=%s&category=%s&kws=%s&tou_name=%s&division=%s&gender=%s&formed=%s&tz=%s&logo=%s&vtap=%s&yt=%s&std=0&src=%s&sk=%s&action=submit"""

aka, bp = "", "-150"
source = "ncaa_ncb"
tou = "NCAA Men's Division I Basketball"
category = "ncaa-ncb"
par_type = "team"
game = "basketball"
gender = "men"
formed = ""
vtap = ""
you_tube = ""
logo_url = ""
tz = ""


class NCAATeamsAdd(VTVSpider):
    name = "ncaa_schools"
    start_urls = ['http://www.ncaa.com']
    allowed_domains = ['www.ncaa.com']
    domain_url = "http://www.ncaa.com"
    teams = open("missed_teams", "a+")
    false = open("filter_urls", "a+")
    true = open("true_teams", "a+")
    urls = []

    def parse(self, response):
        hxs = Selector(response)
        schools = extract_data(hxs, '//div//a[span[contains(text(), "Browse Schools")]]/@href')
        if not "http" in schools:
            link = self.domain_url + schools
            yield Request(link, callback=self.parse_one)

    def parse_one(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@id="school-list"]//li')
        for node in nodes:
            url = extract_data(node, './/@href')
            if not "http" in url:
                req = self.domain_url + url
                yield Request(req, callback=self.parse_teams)

        pages = get_nodes(hxs, '//div[@class="item-list"]//li[a[@class="active"]]/following-sibling::li/a')

        for page in pages:
            school_link = extract_data(page, './/@href')
            if not "http" in school_link:
                link = self.domain_url + school_link
                yield Request(link, callback=self.parse_one)




    def parse_teams(self, response):
        hxs = Selector(response)
        name = extract_data(hxs, '//span[@class="school-name"]/text()')
        location = extract_data(hxs, '//li[span[contains(text(), "Location")]]/text()')
        nickname = extract_data(hxs, '//li[span[contains(text(), "Nickname")]]/text()')
        image = extract_data(hxs, '//span[@class="school-logo"]//@src')
        team_sk = response.url.split('/')[-1]
        pl_exists = check_sk(team_sk)
        if pl_exists == False:
            if nickname:
                if name.startswith('University of'):
                    t_name = name.replace('University of ', '')+ " " + nickname + " men's basketball"
                    short_title = name.split('University of ')[1]
                elif name.startswith('University'):
                    short_title = name.split('University')[1]
                    t_name = name.replace('University ', '') + " " + nickname + " men's basketball"
                else:
                    short_title = name.split('University')[0]
                    t_name = name.replace(' University', '') + " " + nickname + " men's basketball"
                team_name = get_team_name(t_name.replace('Univ. ', ''))
                pt_ids = check_title(team_name)
                if not pt_ids:
                    team_name = get_refined_name(team_name)
                    pt_ids = check_title(team_name)
                if not pt_ids:
                    pt_ids = check_title(team_name.replace('(Pa.)', '(pa)').replace('(Mo.)', '(Mo)'))
                if len(pt_ids) == 1:
                    res = [str(i[0]) for i in pt_ids]
                    values = (res[0], 'participant', 'ncaa_ncb', team_sk)
                    query = INSERT_SK % values
                    #CURSOR.execute(query)
                else:
                    if not pt_ids:
                        name_ = team_name
                        team_values = (name_, aka, game, par_type, image, bp, response.url, short_title, '', category, "", tou, "", gender, formed, tz, logo_url, vtap, you_tube, source, team_sk)
                        if name:
                            pl_add_url = (TEAM_URL % team_values).replace('\n', '')
                            #urllib.urlopen(pl_add_url)
                        self.teams.write("%s\n" % team_name)
            else:
                self.false.write("%s\n" % response.url)
        else:
            if not name:
                name = response.url
                self.true.write("%s\n" % name)
