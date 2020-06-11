# -*- coding: utf-8 -*-

"""Crawler for UEFA Association Cup games """
import re
import time
import unicodedata
import datetime
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data, log, extract_list_data
from scrapy.selector import HtmlXPathSelector
from scrapy_spiders_dev.items import SportsSetupItem

TEAM_DICT   = {'Nottm Forest'       : '52681',      'Sheff. United'     : '68215',      'Wigan'     : '89696' ,
               'Sheff. Wednesday'   : '52922',      'Charlton'          : '69598',      'Brighton'  : '2601105',
               'Blackburn'          : '53344',      'MK Dons'           : '52924',      'QPR'       : '52282',
               'Bristol City'       : '2601134',    'Bolton'            : '53346',      'Blackpool' : '2601125',
               'Leicester'          : '53577',      'Middlesbrough'     : '69600',      'Barnsley'  : '53339',
               'Coventry'           : '52915',      'Huddersfield'      : '2601106',    'Watford'   : '52744',
               'Yeovil'             : '2601135',    'Leyton Orient'     : '94021',      'Stevenage' : '2601373',
               'Kidderminster'      : '2601371',    'Peterborough'      : '2600631',    'Reading'   : '93338',
               'Birmingham'         : '53342',      'Bournemouth'       : '2601124',    'Bristol Rovers':'2601129',
               'Burton'             : '2601140',    'Crystal Palace'    : '52916',      'Ipswich'   : '62301',
               'Leeds'              : '53361',      'Millwall'          : '86796',      'Oxford'    : '90864',
               'Plymouth'           : '2601118',    'Port Vale'         : '2601109',    'Rochdale'  : '2603517',
               'Stoke'              : '53356',      'Kaiserslautern'    : '3695',       'Spezia'    : '2601009',
               'Cannes'             : '52930',      'LOSC'              : '75797',      'Caen'      : '52933',
               'Auxerre'            : '52269',      'Angers'            : '64162',      'Moulins'   : '2605330',
               "L'Île-Rousse"       : '2606147',    'Lens'              : '52277',      'Preston'   : '394',
               'CA Bastia'          : '61567',      'Troyes'            : '73556',      'Cornella'  : '20000898'}


STADIUMS_DICT = {'5035' : 'Wembley Stadium', '5135' : 'Wembley Stadium',
                 '5138' : 'Stade de France', '5053' : 'Estadi de Mestalla',
                 '5042' : 'Stadio Olimpico'}

tournament = {  'geo': 'Georgian Cup',      'and': 'Andorran cup', 'sco': 'Scottish Cup',
                'ser': 'Serbian Cup',       'swe': 'Swedish Cup',       'por': 'Portuguese Cup',
                'ita': 'Italian Cup',       'pol': 'Polish Cup',        'kaz': 'Kazakh Cup',
                'blr': 'Belarusian Cup',    'rou': 'Cupa Romaniei',     'cze': 'Czech CMFS Cup',
                'svk': 'Slovak Cup',        'est': 'Estonian Cup',      'ger': 'German DFB Cup',
                'esp': 'Spanish Cup',       'lux': 'Luxembourger Cup',  'ned': 'Dutch KNVB Cup',
                'fro': 'Faroese Cup',       'ukr': 'Ukrainian Cup',     'sui': 'Swiss Cup',
                'mal': 'Maltese Cup',       'gre': 'Greek Cup',         'irl': 'FAI Cup',
                'aut': 'Austrian Cup',      'fin': 'Finnish Cup',       'hun': 'Hungarian Cup',
                'cyp': 'Cypriot Cup',       'alb': 'Albanian Cup',      'bul': 'Bulgarian Cup',
                'tur': 'Turkish Cup',       'wal': 'Welsh Cup',         'bel': 'Belgian Cup',
                'mol': 'Moldovan Cup',      'nir': 'Irish Cup',         'rus': 'Russian Cup',
                'nor': 'Norwegian Cup',     'svn': 'Slovene Cup',       'smr': 'San Marinese Cup',
                'den': 'Danish DBU Cup',    'mne': 'Montenegrin Cup',   'aze': 'Azerbaijani Cup',
                'mkd': 'Macedonian Cup',    'lva': 'Latvian Cup',       'isl': 'Icelandic Cup',
                'mda': 'Moldovan Cup',      'srb' : 'Serbian Cup',      'cro': 'Croatian Cup',
                'arm': 'Armenian Independence Cup',         'ltu': 'Lithuanian LFF Cup',
                'bih': 'Bosnian-Herzegovinian Premier Cup', 'lie': 'Liechtensteiner Cup'}
tournament = { 'smr': 'San Marinese Cup', 'ita': 'Italian Cup'}

SPECIAL_LEAGUES =  {'5035' : 'English FA Cup' , '5135' : 'English League Cup',
                    '5038' : 'French Cup',      '5138' : 'French League Cup',
                    '5053' : 'Spanish Cup',     '5042' : 'Italian Cup'}
SK_LIST = ['03122014_52707_50102', '03122014_53064_50158', '03122014_2606502_62174', '03122014_65130_52723', '10122014_2600301_61616', '10122014_64470_64330', '10122014_2603039_64471', '10122014_92452_88347', '03012015_le-poire-sur-vie_plabennec']


class UefaCups(VTVSpider):
    '''Details to crawl cups,
     add the league and league_id'''
    start_urls = []
    name       = "uefa_cups"

    def start_requests(self):
        import pdb;pdb.set_trace()
        LEAGUES_ID      = { 'ita' : '5042'}

        d       = datetime.datetime.today()
        month   = d.month

        if self.spider_type == "scores":
            months = [month]
        else:
            months = [month + m  for m in range(0, 5)]

        for k, v in LEAGUES_ID.iteritems():
            if "#" in v:
                leagues = [i for i in v.split("#")]
            else:
                leagues = [v]
            for league in leagues:
                for m in months:
                    top_url  = 'http://www.uefa.com/memberassociations/association=%s/domesticcup/idcup=%s/matches/month=%s/_includematch.html' % (k, league, m)
                    yield Request(top_url, self.parse, meta = {'league_id' : league})

    @log
    def parse(self, response):
        hdoc    =  HtmlXPathSelector(response)
        games   =  get_nodes(hdoc, '//tr[contains(@class, "fs15 nbb npd-tb")]')

        if len(games) >= 1:
            for game in games:
                result      = {}
                status      = ''
                winner      = ''
                home_id     = extract_data(game, './/td[contains(@class, "r")]//a/@href')
                away_id     = extract_data(game, './/td[contains(@class, "l")]//a/@href')
                home_team   = extract_data(game, './/td[contains(@class, "r")]//a/text()')
                away_team   = extract_data(game, './/td[contains(@class, "l")]//a/text()')

                if type(home_team) == unicode:
                    home_team = unicodedata.normalize('NFKD', home_team).encode('ascii','ignore')
                if type(away_team) == unicode:
                    away_team = unicodedata.normalize('NFKD', away_team).encode('ascii','ignore')

                home_scores = extract_data(game, './/following::tr[contains(@class,"mtc-scorers")][1]//td[@class="r"]//text()')
                away_scores = extract_data(game, './/following::tr[contains(@class,"mtc-scorers")][1]//td[@class="l"]//text()')
                penalities  = extract_data(game, './/following::tr[@class="reasonwin"]/td[@class="nob"]/span[@class="wop"]/text()')

                home_half_time = 0
                home_half_time, away_half_time = 0, 0
                for scr in home_scores.split(","):
                    scr = re.findall('(\d+)', scr)
                    if scr:
                        if int(scr[0]) < 45:
                            home_half_time += 1


                away_half_time = 0
                for scr in away_scores.split(","):
                    scr = re.findall('(\d+)', scr)
                    if scr:
                        if int(scr[0]) < 45:
                            away_half_time += 1


                rname       = extract_data(game, './/preceding-sibling::tr//td[@class="headInfo"]//span[@class="rname"]//text()')
                score       = extract_data(game, './/td[@class="fs19 b nw"]//text()').strip()

                game_time   = extract_data(game, './/preceding::td[@class="headInfo"][1]//text()').replace("\n", "")
                if "-" in game_time:
                    game_note = game_time.split(' - ')[-1].strip()
                else:
                    game_note = ''
                venue       = extract_data(game, './/following::tr[@class="mtc-venue npd-tb"][1]//text()').strip().replace("Venue: ", "")
                if not venue:
                    venue_ = extract_list_data(game, './/following::tr[contains(@class, "mtc-venue npd-tb")]/td/text()')
                    if venue_:
                        venue = venue_[0].strip().replace("Venue: ", "")
                game_time   = re.findall("(\d+/\d+/\d+)", game_time)

                home_id         = re.findall('club=(\d+)', home_id)
                if not home_id:
                    home_id = TEAM_DICT.get(home_team, '')
                    if not home_id:
                        home_id = home_team.strip().replace(' ', '-').lower()

                else:
                    home_id = home_id[0]

                away_id         = re.findall('club=(\d+)', away_id)
                if not away_id:
                    away_id = TEAM_DICT.get(away_team, '')
                    if not away_id:
                        away_id = away_team.strip().replace(' ', '-').lower()
                else:
                    away_id = away_id[0]

                if home_id and away_id:
                    gm_time     = game_time[0].replace("/", "")
                    match_sk = gm_time + "_" + home_id + "_" + away_id
                if match_sk in SK_LIST:
                    continue

                if "-" in score and not score == '-':
                    status  = 'completed'
                    details = score.split("-")
                    if len(details) > 1:
                        result.setdefault(home_id, {}).update({'final' : details[0], 'H1' : home_half_time})
                        result.setdefault(away_id, {}).update({'final' : details[1], 'H1' : away_half_time})

                        if details[0] > details[1]:
                            winner = home_id
                        elif details[0] < details[1]:
                            winner = away_id
                        elif details[0] == details[1] and penalities:
                            if home_team in penalities:
                                winner = home_id
                            elif away_team in penalities:
                                winner = away_id
                        elif details[0] == details[1]:
                            winner = ''
                    else:
                        status = 'postponed'

                elif ":" in score:
                    status      = "scheduled"
                else:
                    status      = 'postponed'

                if status == "scheduled":
                    game_time       = game_time[0] + " " + score + ":00"
                    date_object     = datetime.datetime(*time.strptime(game_time, '%d/%m/%Y %H:%M:%S')[0:6])
                    game_date       = date_object.strftime('%Y-%m-%d %H:%M')
                    now             = datetime.datetime.utcnow()

                    game_dt_time    = datetime.datetime(*time.strptime(game_date, '%Y-%m-%d %H:%M')[0:6])
                    utc_dt_time     = game_dt_time - datetime.timedelta(hours=1)
                    utc_game_date   = time.strftime("%Y-%m-%d %H:%M:%S", utc_dt_time.timetuple())
                    score = ''

                elif status  == 'completed' or status == 'postponed':
                    game_time       = game_time[0] + " " + "12:00:00"
                    date_object     = datetime.datetime(*time.strptime(game_time, '%d/%m/%Y %H:%M:%S')[0:6])
                    game_date       = date_object.strftime('%Y-%m-%d %H:%M')
                    now             = datetime.datetime.utcnow()

                    game_dt_time    = datetime.datetime(*time.strptime(game_date, '%Y-%m-%d %H:%M')[0:6])
                    utc_dt_time     = game_dt_time - datetime.timedelta(hours=1)
                    utc_game_date   = time.strftime("%Y-%m-%d %H:%M:%S", utc_dt_time.timetuple())


                tou             = re.findall('association=(\w+)', response.url)
                if tournament.has_key(tou[0]):
                    tou_name    = tournament[tou[0]]
                else:
                    tou_name    = SPECIAL_LEAGUES[response.meta['league_id']]

                match_url   = 'http://www.uefa.com/memberassociations/association=%s/domesticcup/index.html' % tou[0]
                stadium     = ''
                event_id    = ''

                if rname:
                    rname = rname.replace("-", "").strip()
                    if rname == 'Final':
                        event_id    = tou_name + ' ' + 'Final'
                        if STADIUMS_DICT.has_key(response.meta['league_id']):
                            stadium = STADIUMS_DICT[response.meta['league_id']]
                        else:
                            stadium = ''

                location    = {'stadium' : stadium, 'city' : venue}
                rich_data   = {'location' : location, 'game_id' : match_sk, 'game_note': game_note}
                result.setdefault('0', {}).update({'winner' : winner, 'score' : score})

                participants                        = {}
                participants[str(home_id)]          = (1, home_team)
                participants[str(away_id)]          = (0, away_team)

                sportssetupitem = SportsSetupItem()
                sportssetupitem['affiliation']      = 'club-football'
                sportssetupitem['event']            = event_id
                sportssetupitem['game']             = 'soccer'
                sportssetupitem['game_datetime']    = utc_game_date
                sportssetupitem['game_status']      = status
                sportssetupitem['participant_type'] = 'team'
                sportssetupitem['participants']     = participants
                sportssetupitem['reference_url']    = match_url
                sportssetupitem['result']           = result
                sportssetupitem['rich_data']        = rich_data
                sportssetupitem['source']           = "uefa_soccer"
                sportssetupitem['source_key']       = match_sk
                sportssetupitem['tournament']       = tou_name
                yield sportssetupitem
