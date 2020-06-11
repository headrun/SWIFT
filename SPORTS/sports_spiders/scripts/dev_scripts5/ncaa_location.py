from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
import MySQLdb
import urllib

CURSOR = MySQLdb.connect(host="10.4.18.34", user="root",
db="SPORTSDB_BKP").cursor()

SK_QUERY = 'select source_key from sports_source_keys where \
entity_type="participant" and source="ncaa_ncb" and entity_id=%s'

TOU_PAR = 'select participant_id from sports_tournaments_participants where tournament_id =213'

UPDATE_LOC = 'update sports_participants set location_id=%s where id=%s limit 1'

REPLACE_STATE_DICT = {'TN' : 'Tennessee', 'OH' : 'Ohio', 'VA' : 'Virginia', \
'TX' : 'Texas', 'OK' : 'Oklahoma', 'NY' : 'New York', \
'NJ' : 'New Jersey', 'IL' : 'Illinois', 'AL' : 'Alabama', 'Ala' : 'Alabama', 'NC' : 'North Carolina', \
'SC' : 'South Carolina', 'GA' : 'Georgia', 'Kan': 'Kansas', 'Ind': 'Indiana', \
'OR' : 'Oregon', 'Ore': 'Oregon', 'DE' : 'Delaware', 'IA' : 'Iowa', 'WV' : 'West Virgina', \
'FL' : 'Florida', 'KS' : 'Kansas', 'TN' : 'Tennessee', \
'LA' : 'Louisiana', 'MO' : 'Missouri', 'AR' : 'Arkansas', 'SD' : 'South Dakota', 'MS' : 'Mississippi', 'MI' : 'Michigan', \
'UT' : 'Utah', 'MT' : 'Montana', 'NE' : 'Nebraska', 'ID' : 'Idaho', 'RI' : 'Rhode Island', \
'NM' : 'New Mexico', 'MN' : 'Minnesota', 'PA' : 'Pennsylvania', 'MD' : 'Maryland', 'IN' : 'Indiana', \
'CA': 'California', 'WI': 'Wisconsin', 'KY' : 'Kentucky', 'MA' : 'Massachusetts', 'CT' : 'Connecticut', 'CO': 'Colorado' }


class NCAALocation(VTVSpider):
    name = "ncaa_location"
    allowd_domains = []
    start_urls = []

    def start_requests(self):
        CURSOR.execute(TOU_PAR)
        data = CURSOR.fetchall()
        for data_ in data:
            en_id = str(data_[0])
            CURSOR.execute(SK_QUERY %(en_id))
            data = CURSOR.fetchall()
            sk = data[-1][0]
            if sk:
                url = "http://www.ncaa.com/schools/%s" %(sk) 
                yield Request(url, callback=self.parse_location, meta = {'id_': en_id})

    def parse_location(self, response):
        sel = Selector(response)
        pl_id = response.meta['id_']
        location = extract_data(sel, '//li[span[contains(text(), "Location")]]/text()')
        if len(location.split(','))==2:
            city = location.split(',')[0]
            state = location.split(',')[1].strip()
            state = REPLACE_STATE_DICT.get(state)
            loc_id = 'select id from sports_locations where city ="%s" and state = "%s" and country = "USA" limit 1' %(city, state)
            CURSOR.execute(loc_id)
            loc_id = CURSOR.fetchall()

            if not loc_id:
                loc_id = 'select id from sports_locations where city ="%s", state = "%s" limit 1' %(city, state)
                CURSOR.execute(loc_id)
                loc_id = CURSOR.fetchall()

            if loc_id and loc_id !=():
                loc_id = str(loc_id[0][0])
                import pdb;pdb.set_trace()
                CURSOR.execute(UPDATE_LOC %(loc_id, pl_id))

            else:
                response.url
            


