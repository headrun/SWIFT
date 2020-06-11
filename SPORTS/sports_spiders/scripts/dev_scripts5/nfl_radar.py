from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider import VTVSpider, get_nodes, extract_data, extract_list_data
import re
import datetime
import MySQLdb
import genericFileInterfaces

PL_QRY = 'select id from sports_participants where title = "%s" and game = "football"'
PL_DOB_QRY = 'select birth_date from sports_players where participant_id = %s'


def add_source_key(entity_id, _id):
    if _id and entity_id:
        conn, cursor = mysql_connection()
        query = "insert into sports_radar_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now())"
        values = (entity_id, 'participant', 'radar', _id)

        cursor.execute(query, values)
        conn.close()



class NFLRadar(VTVSpider):
    name = "nfl_radar"
    start_urls = ['https://api.sportradar.us/nfl-ot1/teams/97354895-8c77-4fd4-a860-32e62ea7382a/profile.xml?api_key=ru43ezr53p9jdsbg9r2tmvyr']

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_RADAR")
        self.cursor = self.conn.cursor()
        self.player_exists_file = open('player_exist', 'w')
        self.player_not_exists_file = open('birtdate_not_matched', 'w')

        self.schema = { 'pl_name'    : 0, 'jersey'   : 1, 'last_name' : 2, \
                        'first_name' : 3, 'abbr_name': 4, 'preferred_name' : 5, \
                        'birth_date' : 6, 'weight'   : 7, 'height' : 8, \
                        'pl_id'      : 9, 'position' : 10, 'birth_place': 11, \
                        'high_school': 12, 'college' : 13, 'college_conf': 14, \
                        'rookie_year': 15
                            }

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        nodes = get_nodes(sel, '//players/player')
        for node in nodes:
            pl_name = extract_data(node, './@name')
            abbr_name = extract_data(node, './@abbr_name')
            last_name = extract_data(node, './@last_name')
            first_name = extract_data(node, './@first_name')
            preferred_name = extract_data(node, './@preferred_name')
            position = extract_data(node, './@position')
            birth_date = extract_data(node, './@birth_date')
            weight = extract_data(node, './@weight')
            height = extract_data(node, './@height')
            pl_id = extract_data(node, './@id')
            birth_place = extract_data(node, './@birth_place')
            jersey  = extract_data(node, './@jersey')
            rookie_year = extract_data(node, './@rookie_year')
            college_conf = extract_data(node, './@college_conf')
            college = extract_data(node, './@college')
            high_school = extract_data(node, './@high_school')

            site_birt_date = str(datetime.datetime.strptime(birth_date, "%Y-%m-%d"))

            record = [ pl_name, jersey, last_name, first_name, abbr_name, \
                       preferred_name, birth_date, weight, height, pl_id, position, \
                       birth_place, high_school, college, college_conf, rookie_year ]

            entity_id = get_plid(self, pl_name, site_birt_date, preferred_name, last_name)

            if entity_id:
                add_source_key(entity_id, pl_id)
                #genericFileInterfaces.writeSingleRecord(self.player_exists_file, record, self.schema, 'utf-8')

            else:
                genericFileInterfaces.writeSingleRecord(self.player_not_exists_file, record, self.schema, 'utf-8')




def get_plid(self, pl_name, site_birt_date, preferred_name, last_name):
    self.cursor.execute(PL_QRY %(pl_name))
    data = self.cursor.fetchone()
    if not data:
        pl_name = preferred_name.replace("Tim'", 'Tim') + " " + last_name
        self.cursor.execute(PL_QRY %(pl_name))
        data = self.cursor.fetchone()
    if data:
        pl_id = data[0]
        self.cursor.execute(PL_DOB_QRY %(pl_id))
        birth_date = self.cursor.fetchone()
        if birth_date:
            db_birth_date = str(birth_date[0])
        if db_birth_date == site_birt_date:
            pl_id = pl_id
        else:
            pl_id = ''
    else:
        pl_id = ''
    return pl_id

