import json
import MySQLdb
import genericFileInterfaces
from vtvspider import VTVSpider, get_nodes, extract_data
from scrapy.selector import Selector
from scrapy.http import Request


SK_QUERY = 'select entity_id from sports_source_keys where source="MLB" and entity_type="participant" and source_key=%s'

INSERT_SK = 'insert ignore into sports_radar_source_keys(entity_id, entity_type, source, source_key, created_at, modified_at) values (%s, %s, %s, %s, now(), now())'

API_DICT = {'TOR': "1d678440-b4b1-4954-9b39-70afb3ebbcfa",
            'TB' : "bdc11650-6f74-49c4-875e-778aeb7632d9",
            'BAl': "75729d34-bca7-4a0f-b3df-6f26c6ad3719",
            'BOS': "93941372-eb4c-4c40-aced-fe3267174393",
            'NYY': "a09ec676-f887-43dc-bbb3-cf4bbaee9a18",
            'KC' : "833a51a9-0d84-410f-bd77-da08c3e5e26e",
            'CLE': "80715d0d-0d2a-450f-a970-1b9a3b18c7e7",
            'CWS': "47f490cd-2f58-4ef7-9dfd-2ad6ba6c1ae8",
            'DET': "575c19b7-4052-41c2-9f0a-1c5813d02f99",
            'MIN': "aa34e0ed-f342-4ec6-b774-c79b47b60e2d",
            'SEA': "43a39081-52b4-4f93-ad29-da7f329ea960",
            'LAA': "4f735188-37c8-473d-ae32-1f7e34ccf892",
            'OAK': "27a59d3b-ff7c-48ea-b016-4798f560f5e1",
            'TEX': "d99f919b-1534-4516-8e8a-9cd106c6d8cd",
            'HOU': "eb21dadd-8f10-4095-8bf3-dfb3b779f107",
            'PHI': "2142e1ba-3b40-445c-b8bb-f1f8b1054220",
            'NY' : "f246a5e5-afdb-479c-9aaa-c68beeda7af6",
            'MIA': "03556285-bdbb-4576-a06d-42f71f46ddc5",
            'ATL': "12079497-e414-450a-8bf2-29f91de646bf",
            'WSH': "d89bed32-3aee-4407-99e3-4103641b999a",
            'LA' : "ef64da7f-cfaf-4300-87b0-9313386b977c",
            'ARI': "25507be1-6a68-4267-bd82-e097d94b359b",
            'SF' : "a7723160-10b7-4277-a309-d8dd95a8ae65",
            'COL': "29dd9a87-5bcc-4774-80c3-7f50d985068b",
            'SD' : "d52d5339-cbdd-43f3-9dfa-a42fd588b9a3",
            'CIN': "c874a065-c115-4e7d-b0f0-235584fb0e6f",
            'PIT': "481dfe7e-5dab-46ab-a49f-9dcc2b6e2cfd",
            'CHI': "55714da8-fcaf-4574-8443-59bfb511a524",
            'STL': "44671792-dc02-4fdd-a5ad-f5f17edaa9d7",
            'MIL': "dcfd5266-00ce-442c-bc09-264cd20cf455"}

class MLBRadar(VTVSpider):
    name = 'radar_mlb'
    url = 'https://api.sportradar.us/mlb-t5/teams/%s/profile.json?api_key=xadjhp2uxg342yvjhxvv5z8b'
    p_url = 'https://api.sportradar.us/mlb-p5/teams/%s/profile.json?api_key=e7gv4ntqzhyrrs5fn4qje6vr'

    def __init__(self):
        self.conn        = MySQLdb.connect(host='10.4.18.34', user='root', db= 'SPORTSDB_RADAR')
        self.cursor      = self.conn.cursor()
        self.exists_file = open('pl_exists_file', 'w')
        self.missed_file = open('pl_missed_file', 'w')
        self.schema      = {'mlb_sk'        : 0,       'radar_sk'      : 1,
                            'full_name'     : 2,       'position'      : 3,
                            'status'        : 4,       'short_title'   : 5,
                            'jersey_number' : 6,       'height'        : 7,
                            'height'        : 7,       'weight'        : 8,
                            'birthdate'     : 9,       'birthcity'     : 10,
                            'birthstate'    : 11,      'birthcountry'  : 12,
                            'pro_debut'     : 13}

    def start_requests(self):
        for api_key in API_DICT.values():
            top_url = self.p_url % api_key
            yield Request(top_url, self.parse)

    def parse(self, response):
        data = json.loads(response.body)
        players = data.get('players', '')
        for node in players:
            mlb_sk        = node.get('mlbam_id', '')
            radar_sk      = node.get('id', '')
            full_name     = node.get('full_name', '')
            position      = node.get('position', '')
            status        = node.get('status', '')
            jersey_number = node.get('jersey_number', '')
            height        = node.get('height', '')
            weight        = node.get('weight', '')
            birthdate     = node.get('birthdate', '')
            birthcity     = node.get('birthcity', '')
            birthstate    = node.get('birthstate', '')
            birthcountry  = node.get('birthcountry', '')
            pro_debut     = node.get('pro_debut', '')
            pl_id         = self.get_sk(mlb_sk, radar_sk)
            if not pl_id:
                print response.url
                record = [mlb_sk, radar_sk, full_name, position, status, jersey_number, height, weight, birthdate, birthcity, birthstate, birthcountry, pro_debut]
                self.missed_file.write('%s\n' % record)

    def get_sk(self, mlb_sk, radar_sk):
        self.cursor.execute(SK_QUERY, mlb_sk)
        data = self.cursor.fetchone()
        if data:
            data = str(data[0])
            values = (data, 'participant', 'radar', radar_sk)
            self.cursor.execute(INSERT_SK, values)
        return data
