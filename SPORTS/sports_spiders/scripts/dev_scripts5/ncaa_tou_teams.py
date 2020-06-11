from vtvspider_dev import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
import MySQLdb
import urllib



class NCAATouTeams(VTVSpider):
    name = "ncaa_team_tou"
    allowed_damains = []
    start_urls = []

    def __init__(self):
        self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        #self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def start_requests(self):
        sl_query = 'select id, participant_id from sports_teams where participant_id  not in (select participant_id from sports_tournaments_participants where tournament_id =213) and tournament_id=213'
        self.cursor.execute(sl_query)
        data = self.cursor.fetchall()
        for data_ in data:
            id_ = str(data_[0])
            pr_id = str(data_[1])
            sk_qurey = 'select source_key from sports_source_keys where source="ncaa_ncb" and entity_type = "participant" and entity_id=%s' %(pr_id)
            self.cursor.execute(sk_qurey)
            data = self.cursor.fetchone()
            if data:
                sk = str(data[0])
                url = "http://www.ncaa.com/schools/" + sk
                yield Request(url, callback=self.parse, meta = {'pr_id':pr_id})


    def parse(self, response):
        sel = Selector(response)
        div = extract_data(sel, '//div[@class="school-meta"]//h2//text()').strip()
        if div:
            if div == "Div III":
                tou_id = "3154"
            elif div == "Div II":
                tou_id ="3153"
            else:
                import pdb;pdb.set_trace()
            if tou_id:
                up_query = 'update sports_teams set tournament_id=%s where participant_id=%s'
                values = (tou_id, response.meta['pr_id'])
                self.cursor.execute(up_query, values)
