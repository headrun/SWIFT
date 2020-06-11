from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime
import MySQLdb



def create_cursor():
    con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB")
    cursor = con.cursor()
    return con, cursor


def check_player(player_id):
    con, cursor = create_cursor()
    cursor.execute(SK_QUERY % player_id)
    entity_id = cursor.fetchone()
    if entity_id:
        pl_exists = True
        pl_id = entity_id
    else:
        pl_exists = False
        pl_id = ''
    con.close()
    return pl_exists, pl_id

def add_source_key(entity_id, _id):
    if _id and entity_id:
        con, cursor = create_cursor()
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'uciworldtour_cycling', _id)

        cursor.execute(query, values)
        con.close()

def check_title(name):
    con, cursor = create_cursor()
    cursor.execute(PL_NAME_QUERY % (name, GAME))
    pl_id = cursor.fetchone()
    con.close()
    return pl_id
PAR_QUERY = "insert into sports_participants (id, gid, title, aka, game, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at, display_title) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, now(), now(), %s) on duplicate key update modified_at = now();"
MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'


PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"';

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="uciworldtour_cycling" and source_key= "%s"'


GAME = 'cycling'
PAR_TYPE = 'player'
BASE_POP = "200"
LOC = '0'
DEBUT = "0000-00-00"
ROLES = ''
GENDER = "male"
SAL_POP = ''
RATING_POP = ''
MARITAL_STATUS = ''
PAR_SINCE = COMP_SINCE = ''
WEIGHT_CLASS = AKA = ''


class UCICyclingPlayer(VTVSpider):
    name = "cyclinguci_player"
    start_urls = ['http://www.uci.infostradasports.com/asp/lib/TheASP.asp?PageID=19006&TaalCode=2&StyleID=0&SportID=102&CompetitionID=21608&EditionID=998575&SeasonID=488&EventID=12146&GenderID=1&ClassID=1&PhaseStatusCode=262280&EventPhaseID=998576&Phase1ID=1131853&Phase2ID=0&Phase3ID=0&PhaseClassificationID=1205997&Detail=0&Ranking=0&DerivedEventPhaseID=-1&S00=1&S01=2&S02=3&PageNr0=-1&Cache=8', \
    'http://www.uci.infostradasports.com/asp/lib/TheASP.asp?PageID=19006&TaalCode=2&StyleID=0&SportID=102&CompetitionID=21608&EditionID=998575&SeasonID=488&EventID=12146&GenderID=1&ClassID=1&PhaseStatusCode=262280&EventPhaseID=998576&Phase1ID=1131858&Phase2ID=0&Phase3ID=0&PhaseClassificationID=1214335&Detail=1&Ranking=0&DerivedEventPhaseID=-1&S00=1&S01=2&S02=3&PageNr0=-1&Cache=8', \
    'http://www.uci.infostradasports.com/asp/lib/TheASP.asp?PageID=19006&TaalCode=2&StyleID=0&SportID=102&CompetitionID=21608&EditionID=998575&SeasonID=488&EventID=12146&GenderID=1&ClassID=1&PhaseStatusCode=262280&EventPhaseID=998576&Phase1ID=1131857&Phase2ID=0&Phase3ID=0&PhaseClassificationID=1214334&Detail=1&Ranking=0&DerivedEventPhaseID=-1&S00=1&S01=2&S02=3&PageNr0=-1&Cache=8', \
    'http://www.uci.infostradasports.com/asp/lib/TheASP.asp?PageID=19006&TaalCode=2&StyleID=0&SportID=102&CompetitionID=21608&EditionID=998575&SeasonID=488&EventID=12146&GenderID=1&ClassID=1&PhaseStatusCode=262280&EventPhaseID=998576&Phase1ID=1131856&Phase2ID=0&Phase3ID=0&PhaseClassificationID=1214333&Detail=1&Ranking=0&DerivedEventPhaseID=-1&S00=1&S01=2&S02=3&PageNr0=-1&Cache=8', \
    'http://www.uci.infostradasports.com/asp/lib/TheASP.asp?PageID=19006&TaalCode=2&StyleID=0&SportID=102&CompetitionID=21608&EditionID=998575&SeasonID=488&EventID=12146&GenderID=1&ClassID=1&PhaseStatusCode=262280&EventPhaseID=998576&Phase1ID=1131855&Phase2ID=0&Phase3ID=0&PhaseClassificationID=1214332&Detail=1&Ranking=0&DerivedEventPhaseID=-1&S00=1&S01=2&S02=3&PageNr0=-1&Cache=8', \
    'http://www.uci.infostradasports.com/asp/lib/TheASP.asp?PageID=19006&TaalCode=2&StyleID=0&SportID=102&CompetitionID=21608&EditionID=998575&SeasonID=488&EventID=12146&GenderID=1&ClassID=1&PhaseStatusCode=262280&EventPhaseID=998576&Phase1ID=1131854&Phase2ID=0&Phase3ID=0&PhaseClassificationID=1206002&Detail=1&Ranking=0&DerivedEventPhaseID=-1&S00=1&S01=2&S02=3&PageNr0=-1&Cache=8']
    start_urls = ['http://www.uci.infostradasports.com/asp/lib/TheASP.asp?PageID=19006&SportID=102&CompetitionID=20471&EditionID=998579&SeasonID=488&ClassID=1&GenderID=1&EventID=12146&EventPhaseID=0&Phase1ID=1133101&Phase2ID=0&Phase3ID=0&PhaseClassificationID=-1&Detail=0&Ranking=0&All=0&TaalCode=2&StyleID=0&Cache=8']
    start_urls = ['http://www.uci.infostradasports.com/asp/lib/TheASP.asp?PageID=19006&SportID=102&CompetitionID=25150&EditionID=1002629&SeasonID=488&ClassID=1&GenderID=1&EventID=12146&EventPhaseID=0&Phase1ID=1370764&Phase2ID=0&Phase3ID=0&PhaseClassificationID=-1&Detail=0&Ranking=0&All=0&TaalCode=2&StyleID=0&Cache=8']
    participants = {}


    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//table[@class="datatable"]//tr[@valign="top"]')
        for node in nodes:
            player_name = extract_data(node, './/td[2]/text()').title()
            player_age = extract_data(node, './/td[@align="center"]/text()')
            player_img = ''
            display_title = ''
            player_role = ''
            player_id = player_name.lower().replace(' ', '-')
            con, cursor = create_cursor()
            pl_exists, pl_id = check_player(player_id)
            if pl_exists == False:
                pts_id = check_title(player_name)
                if pts_id:
                    add_source_key(str(pts_id[0]), player_id)
                    print "add source key", player_name
                '''else:
                    img = ''
                    cursor.execute(MAX_ID_QUERY)
                    pl_data = cursor.fetchall()
                    max_id, max_gid = pl_data[0]
                    next_id = max_id + 1
                    next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                            replace('PL', '')) + 1)

                    values = (next_id, next_gid, player_name, AKA, GAME, PAR_TYPE, player_img, \
                          BASE_POP, response.url, LOC)
                    cursor.execute(PAR_QUERY, values)
                    values = (next_id, DEBUT, player_role, ROLES, GENDER, \
                          player_age, '', '', '', '', SAL_POP, RATING_POP, \
                          WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE, display_title)

                    cursor.execute(PL_QUERY, values)
                    add_source_key(next_id, player_id)
                    print "add player", player_name
                    con.close()'''

