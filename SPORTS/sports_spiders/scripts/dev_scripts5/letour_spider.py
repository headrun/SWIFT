import re
import datetime
from vtvspider_new import VTVSpider
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import HtmlXPathSelector
from vtvspider_new import VTVSpider, extract_data, extract_list_data, \
                              get_nodes, get_utc_time

domain = 'http://www.letour.fr/us'

def get_sk(sk):
    sk = sk.split('/')[-1].split('.htm')[0].strip()
    sk = sk.strip()
    return sk

class LetourGames(VTVSpider):
    name            = 'letour_games'
    start_urls      = []

    def start_requests(self):
        top_url = 'http://www.letour.fr/us/'
        yield Request(top_url, callback=self.parse, meta= {})

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        link = extract_data(hxs, '//div[contains(@class, "logo")]/a/@href')
        if not link:
            link = extract_data(hxs, '//p[contains(@class, "logo")]/a/@href')
        season = ''.join(re.findall('le-tour\/(\d+)\/.*\/', link))
        if not season:
            #season = link.split('/')[-3].strip()
            season = "2015"
        tou_name = extract_data(hxs, '//div[@class="description"]/h3/text()').replace(season, '').strip()
        if not tou_name:
            tou_name = extract_data(hxs, '//p[contains(@class, "logo")]/a/text()')
        url = domain.replace('/us', link)+'parcours-general.html'#'classifications.html'

        yield Request(url, callback=self.parse_details, meta= {'season': season, \
                                                        'tou_name': tou_name})

    def parse_details(self, response):
        hxs = HtmlXPathSelector(response)
        record = SportsSetupItem()
        season = response.meta['season']
        tou_name = response.meta['tou_name']
        #today_date = datetime.datetime.now()
        today_date = datetime.datetime.utcnow().date()
        nodes = get_nodes(hxs, '//table[@class="table-etape"]/tbody//tr')
        for node in nodes:
            game_datetime = extract_list_data(node, './/td[@class="date"]/text()')[0]. \
            split(',')[1].strip()
            game_datetime = game_datetime.replace('th', ''). \
            replace('rd', '').replace('st', '').replace('nd', '').strip()
            tou_date = game_datetime.strip()+ ' ' + season
            game_datetime = game_datetime.strip() + ' ' + season
            pattern = "%B %d %Y"
            game_time = get_utc_time(game_datetime, pattern, 'US/Eastern')
            distance  = extract_data(node, './/td[@class="distance"]//text()')
            stage     = extract_data(node, './/td[@class="etape"]//text()')
            game_note  = extract_data(node, './/td[@class="parcours"]/text()'). \
            replace('>', '/').replace('&gt;', '>').strip() + " -" + " " + distance
            game_note = game_note.replace( ' / ', ' to ').strip()
            if stage:
                stage = "Stage "+stage
            game_note  = stage +  " "+ game_note
            if "Stage P" in game_note:
                continue
            game_type  = extract_data(node, './td[contains(@class, "type")]/text()')
            if "Rest day" in game_type:
                continue
            link = extract_data(node, './/td[@class="details"]/a/@href')
            if "rest-day" in link: continue
            if "http" not in link:
                link = "http://www.letour.fr" + link
            game_date = game_time.split(' ')[0]
            today_date = datetime.datetime.now().date()
            if  game_date < str(today_date):
                status = "completed"
            elif game_date == str(today_date):
                status = 'ongoing'
            else:
                status = 'scheduled'


            final_link = link.split('.html')[0]+'/classifications.html'
            gt = datetime.datetime.strptime(game_time, '%Y-%m-%d %H:%M:%S' )
            game_sk = final_link.split('/')[-2].strip() + '_' + str(gt.year) + str(gt.month) + str(gt.day)
            event_name = tou_name + " "+final_link.split('/')[-2].strip().replace('-', ' ').title()
            game_data = {'game_dt': str(game_time), 'tou_name': tou_name, \
                'event_name' : event_name, 'reference': link, \
                'status' : status, 'game_sk' : game_sk, \
                'tou_date' : tou_date, 'game_note': game_note, \
                'season': season}

            if status == "completed" or status == "ongoing":
                yield Request(final_link, callback=self.parse_scores, meta= {'game_data': game_data, \
                    'game_dt': str(game_time), 'tou_name': tou_name, \
                    'event_name' : event_name, 'reference': link, \
                    'status' : status, 'game_sk' : game_sk, \
                    'tou_date' : tou_date, 'game_note': game_note, \
                    'season': season})
            elif status == "scheduled":
                record['game_datetime'] = game_time
                record['source_key'] = game_sk
                record['game_status'] = status
                record['reference_url'] = link
                record['tournament'] = tou_name
                record['event'] = event_name
                record['rich_data'] =  {'game_note': game_note, 'location': {}}
                record['affiliation'] = "uci"
                record['source'] = "tourdefrance_cycling"
                record['game'] = "cycling"
                record['participants'] = {}
                record['participant_type'] = "player"
                record['result'] =  {}
                record['time_unknown'] = 1
                yield record


    def parse_scores(self, response):
        hxs = HtmlXPathSelector(response)
        game_data = response.meta['game_data']
        game_num  = response.url.split('/')[-2].strip()
        stage_num  = game_num.split('-')[-1].strip()
        nodes  = get_nodes(hxs, '//div[@id="main"]//div[@id="tableau_honneur"]/div[@class="porteurMaillot"]')
        stage_data = extract_data(hxs, '//div[@id="tableau_honneur"]//h2/text()').strip()
        if stage_data:
            stage_data = stage_data.split('after the stage')[-1].strip()
            if int(stage_data)!= int(stage_num):
                status = "scheduled"
            else:
                status = "completed"
        else:
            if not nodes:
                status = "scheduled"
            else:
                status = "completed"

        results = {}
        if status == "completed":
            for node in nodes:
                jersey_type = [i.strip() for i in extract_list_data(node, './/div[@class="jersey"]//h5/text()')]
                if jersey_type[0] == "team":
                    continue
                if jersey_type[0] == "combative":
                    continue
                final_jersey = " ".join(jersey_type)
                #rider = [i.split('/')[-1].split('.html')[0].strip() for i in extract_data(node, './/div[@class="rider"]//div[@class="buttons"]/a/@href')]
                rider = [i.split('/')[-1].replace('.html', '').strip() for i in extract_list_data(node, './/div[@class="rider"]//div[@class="buttons"]/a/@href')]
                if len(jersey_type)==2:
                    results.setdefault('0', {}).update({jersey_type[0]: rider[0]})
                    results.setdefault('0', {}).update({final_jersey: rider[1]})
                else:
                    if "winner" in jersey_type[0]:
                        jersey_type = ["winner"]
                    results.setdefault('0', {}).update({jersey_type[0]: rider[0]})
        score_link = "http://www.letour.fr/le-tour/%s/us/%s00/classement/bloc-classement-page/ITE.html" % (response.meta['season'], stage_num)
        yield Request(score_link, callback=self.parse_final, meta= {'status' : status, \
                                    'results': results, 'game_data': game_data, \
                                    'stage_num': stage_num})


    def parse_final(self, response):
        import pdb;pdb.set_trace()
        hxs = HtmlXPathSelector(response)
        record = SportsSetupItem()
        results = response.meta['results']
        status = response.meta['status']
        game_note = response.meta['game_data']['game_note']
        #_note = _note = extract_data(hxs, '//span[contains(text(), "Total distance covered")]/following-sibling::span/text()')
        riders = {}
        if "scheduled" not in status:
            rider_nodes =  get_nodes(hxs, '//table[@itequality]//tbody//tr')
            pos = extract_list_data(hxs, '//table[@itequality]//tbody//tr//td[1]//text()')
            if pos:
                if pos[0].replace('.', '').strip() == "1":
                    status = "completed"
            else:
                status = "scheduled"
                results = {}
            for node in rider_nodes:
                rank = extract_data(node, './/td[1]/text()').replace('.', '').strip()
                rider_link = extract_data(node,  './/td//a[contains(@href, "riders")]/@href')
                rider_sk = get_sk(rider_link)
                rider_team = extract_data(node, './/td/a[contains(@href, "/teams/")]/text()')
                final_time = extract_data(node, './/td[5]/text()')
                gap = extract_data(node, './/td[6]/text()').strip().replace('+ ','')
                rider_num = extract_data(node, './/td[3]/text()').replace('.', '').strip()
                results[rider_sk] =  {'position': rank, \
                                    'final_time': final_time, 'time_gap': gap, \
                                        'rider_number': rider_num}
                riders.update({rider_sk:(0, '')})
        else:
            riders = {}
        record['source_key'] = response.meta['game_data']['game_sk']
        record['game_status'] = status
        record['reference_url'] = response.meta['game_data']['reference']
        record['tournament'] = response.meta['game_data']['tou_name']
        record['event'] = response.meta['game_data']['event_name']
        record['game_datetime'] = response.meta['game_data']['game_dt']
        record['rich_data'] =  {'game_note': game_note, 'location': {}}
        record['affiliation'] = "uci"
        record['source'] = "tourdefrance_cycling"
        record['game'] = "cycling"
        record['participant_type'] = "player"
        record['time_unknown'] = 1
        record['participants'] = riders
        record['result'] = results
        yield record
