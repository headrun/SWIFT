from vtvspider import VTVSpider
from scrapy.http import Request
from datetime import datetime
from scrapy.conf import settings
import json
import MySQLdb
from getty_utils import *

TOU_DICT = {'Ligue 1': [32, 7]}

class Gettygames(VTVSpider):
    name        = "getty_games_meta"
    start_urls  = []

    def __init__(self, *args, **kwargs):
        self.conn   = MySQLdb.connect(host = HOST, user = USER, passwd  = passwd, db = DB_NAME, charset = 'utf8', use_unicode = True)
        self.cursor = self.conn.cursor()
        settings.overrides['DOWNLOADER_CLIENTCONTEXTFACTORY'] = 'sports_spiders.context_factory.MyClientContextFactory'

    def start_requests(self):
        seasons = {'Ligue 1':{'ligue+1' : '7'},
                    'Premier League':{'epl':'7'},
                    'UEFA Europa League':{'UEFA Europa League':'7'},
                    'UEFA Champions League':{'UEFA Champions League':'7'},
                    'La Liga':{'La Liga':'7'},
                    'Bundesliga':{'Bundesliga':'7'},
                    'Serie A':{'Serie A italy':'7'},
                    'Liga MX':{'liga+mx':'7'},
                    'Campeonato Brasileiro Serie A':{'Campeonato Brasileiro Serie A':'7'},
                    'Argentina Primera Division':{'Argentina Primera Division':'7'},
                    'Chile Primera Division':{'Chile Primera Division':'7'},
                    'Major League Baseball':{'Major League Baseball':'1'},
                    'National Basketball Association':{'National Basketball Association':'2'},
                    'National Hockey League':{'National Hockey League':'3'},
                    'National Football League':{'National Football League':'4'},
                    'Major League Soccer':{'Major League Soccer':'7'},
                    'NASCAR':{'NASCAR':'10'},
                    'NCAA Division I Football':{'NCAA Division I Football':'4'},
                    "NCAA Men's Division I Basketball":{"NCAA Men's Division I Basketball":'2'}
                    }
        seasons = {"National Basketball Association":{"National Basketball Association":'2'}}
        top_urls = ['http://www.gettyimages.in/search/facets/events?assettype=image&excludenudity=true&family=editorial&phrase=%s&sort=mostpopular&editorialproducts=sport', 'http://www.gettyimages.in/search/facets/specificpeople?assettype=image&excludenudity=true&family=editorial&phrase=%s&sort=best']
        top_urls = ['http://www.gettyimages.in/search/facets/specificpeople?assettype=image&excludenudity=true&family=editorial&phrase=%s&sort=best']
        for k, words in seasons.iteritems():
            for leag,spid in words.iteritems():
                for each in top_urls:
                    top_url = each%leag
                    yield Request(top_url, self.parse, meta={'league':k, 'sport_id':spid})

    def parse(self, response):
        print response.url
        data = json.loads(response.body)
        years = [str(datetime.now().year), str(datetime.now().year - 1)]
        events = data.get('refinements','')
        league = response.meta['league']
        sport_id = response.meta['sport_id']
        for each in events:
            year = each.get('year', '')
            game_datetime = each.get('date', '')
            sub_header  = each.get('subHeader', '')
            game_note   = each.get('name', '')
            game_id     = each.get('id', '')
            if '/facets/events' in response.url:
                if "women's" in game_note.lower() or " v " not in game_note:
                    continue

                if year not in years:
                    continue

                values      = (game_id, game_note, game_datetime, sub_header, league, '0')
                self.cursor.execute(INS_QUERY, values)
                entity_id = self.get_game_id(game_note, game_datetime, league)
                if entity_id:
                    values = (entity_id, 'game', game_id, '1')
                    self.cursor.execute(GM_MAP_QRY, values)

            elif '/facets/specificpeople' in response.url:
                pl_values = (game_id, game_note.split(' - ')[0], sub_header, league, '0')
                self.cursor.execute(PL_INS_QUERY, pl_values)
                entity_id = self.get_player_id(game_note, league, sport_id)
                if entity_id:
                    values = (entity_id, 'player', game_id, '1')
                    self.cursor.execute(PL_MAP_QRY, values)


    def get_player_id(self, pl_name, league, sport_id):
        pl_name = pl_name.split(' - ')[0].strip()
        values = (sport_id, pl_name)
        self.cursor.execute(pl_po_sel_qry1 % values)
        data = self.cursor.fetchall()

        if not data:
            pl_name = '%' + pl_name + '%'
            values = (sport_id, pl_name)
            self.cursor.execute(pl_po_sel_qry1 % values)
            data = self.cursor.fetchall()

        if not data:
            values = (sport_id, pl_name.replace("-", ' '))
            self.cursor.execute(pl_po_sel_qry1 % values)
            data = self.cursor.fetchall()

        if not data:
            values = (sport_id, pl_name)
            self.cursor.execute(pl_po_sel_qry2 % values)
            data = self.cursor.fetchall()

        entity_id = ''
        if len(data) == 1:
            entity_id = data[0][0]
        else:
            for data_ in data:
                pl_id = data_[0]
                values = (league, pl_id)
                self.cursor.execute(pl_po_sel_qry3, values)
                pl_data = self.cursor.fetchall()
                if pl_data:
                    entity_id = pl_data[0][0]
            print data, pl_name
        return entity_id

    def get_game_id(self, game_note, game_datetime, league):
        gn = game_note
        game_datetime = datetime.strftime(datetime.strptime(game_datetime, "%d %B, %Y"), '%Y-%m-%d')
        game_note = game_note.split(':')[-1].split(' - ')[0].strip().replace(' v ', ' vs. ')

        tou_id, sport_id = TOU_DICT[league]
        par_ids = []
        participants = game_note.split(' vs. ')
        for participant in participants:
            check_titles = [item for item in participant.split(' ') if len(item) > 3]
            for check_title in check_titles:
                values = ('%'+check_title+'%', tou_id, sport_id)
                self.cursor.execute(GM_PAR_QRY1, values)
                _id = self.cursor.fetchall()
                if _id and len(_id) ==1:
                    par_ids.append(_id[0][0])
                    break

                if not _id:
                    values = ('%'+check_title+'%', tou_id, sport_id)
                    self.cursor.execute(GM_PAR_QRY2, values)
                    _id = self.cursor.fetchall()
                    if _id and len(_id) ==1:
                        par_ids.append(_id[0][0])
                        break

                if not _id:
                    values = ('%' + participant.replace(' ', '%'), tou_id, sport_id)
                    self.cursor.execute(GM_PAR_QRY3, values)
                    if _id and len(_id) ==1:
                        par_ids.append(_id[0][0])
                        break

                if not _id:
                    self.cursor.execute(GM_PAR_QRY4, '%' + participant.replace(' ', '%') + '%')
                    _id = self.cursor.fetchone()
                    if _id:
                        _id = _id[0]
                        par_ids.append(_id)
                        break

        game_values = tuple(par_ids + ['%'+game_datetime+'%', tou_id])
        try:
            self.cursor.execute(GAME_QRY, game_values)
            game_id = self.cursor.fetchone()
            if game_id:
                game_id = game_id[0]
        except:
            game_id = ''
        '''
        game_ids = self.cursor.fetchall()
        SEL_QRY         = "select id from SPORTSDB.sports_games where game_datetime like %s and game_note like %s and status!='Hole'"
        values          = ('%'+game_datetime+'%', '%'+game_note+'%')
        self.cursor.execute(SEL_QRY, values)
        data            = self.cursor.fetchall()
        if data:
        return str(data[0][0])
        '''
        return game_id
