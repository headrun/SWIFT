import time
import datetime
import re
import lxml.html
import urllib2
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from urlparse import urlparse
import copy
from scrapy_spiders_dev.items import SportsSetupItem

true = True
false = False
null = ''


def X(data):
    try:
        if isinstance(data, int):
            return data
        return ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
    except:
        try: return ''.join([chr(ord(x)) for x in data]).decode('cp1252').encode("utf-8")
        except: return data.encode('utf8')


def get_gmt_time(date_val, pattern):
    t =  datetime.datetime.utcnow()
    utc = datetime.datetime(t.year, t.month, t.day, t.hour, t.minute, t.second)
    t =  datetime.datetime.now()
    now = datetime.datetime(t.year, t.month, t.day, t.hour, t.minute, t.second)
    timedelta = utc - now
    date_val = urllib2.quote(date_val)
    date_val = urllib2.unquote(date_val)
    date_val = date_val.replace('%A0', ' ').replace('Noon', '12:00 PM')
    et = time.strptime(date_val, pattern)#'%Y %a, %b %d  %H:%M %p ET')
    et_dt = datetime.datetime(et.tm_year, et.tm_mon, et.tm_mday, et.tm_hour, et.tm_min,et.tm_sec)
    gmt = et_dt+timedelta
    gmt = gmt.strftime('%Y-%m-%d %H:%M:%S')
    return gmt

'''def get_str_end_dates(tou_date):
    #Feb 13-17 - Feb 27-Mar 3
    str_date, e_date = [i.strip() for i in tou_date.split('-')]
    if len(e_date.split()) < 2:
        str_month, str_day = str_date.split()
        e_date = '%s %s' % (str_month, e_date)
    #str_date = get_gmt_time(str_date)
    #e_date   = get_gmt_time(e_date)
    print "e_date>>", e_date
    print "str_date,", str_date
    return str_date, e_date
'''
def get_tou_dates(start_date, end_date, start_date_format, end_date_format):
    #format '%d %b %Y'
    print "start_date>>>", start_date
    print "end_date>>>", end_date
    mid_date = ''
    e_date    = (datetime.datetime(*time.strptime(end_date.strip(), end_date_format)[0:6])).date()
    str_date   = (datetime.datetime(*time.strptime(start_date.strip(), start_date_format)[0:6])).date()
    if str_date.month == e_date.month:
        #format '04 Apr 2014 06 Apr 2014'
        _date = e_date.day - 1
        end_month = "".join(end_date.split(' ')[1]).strip()
        mid_date = str(_date) + ' ' + end_month + ' ' + str(e_date.year)
        #mid_date = (datetime.datetime(*time.strptime(mid_date.strip(), '%d %b %Y')[0:6])).date()
        #mid_date = mid_date.strftime('%Y-%m-%d %H:%M:%S')
    elif str_date.month in [1, 3, 5, 7, 8, 10, 12]:
        #format '31 Jan 2014 02 Feb 2014'
        _date = e_date.day - 1
        end_month = "".join(end_date.split(' ')[1]).strip()
        mid_date = str(_date) + ' ' + end_month + ' ' + str(e_date.year)
        #mid_date = (datetime.datetime(*time.strptime(mid_date.strip(), '%d %b %Y')[0:6])).date()
        #mid_date = mid_date.strftime('%Y-%m-%d %H:%M:%S')
    else:
        #format '30 Jan 2014 01 Feb 2014'
        _date = str_date.day + 1
        start_month = "".join(start_date.split(' ')[1]).strip()
        mid_date = str(_date) + ' ' + start_month + ' ' + str(str_date.year)
        #mid_date = (datetime.datetime(*time.strptime(mid_date.strip(), '%d %b %Y')[0:6])).date()
        #mid_date = mid_date.strftime('%Y-%m-%d %H:%M:%S')
    e_date = datetime.datetime(*time.strptime(end_date.strip(), end_date_format)[0:6])
    #end_date = e_date.strftime('%Y-%m-%d %H:%M:%S')
    str_date = datetime.datetime(*time.strptime(start_date.strip(), start_date_format)[0:6])
    #start_date = str_date.strftime('%Y-%m-%d %H:%M:%S')
    #start_date = (datetime.datetime(*time.strptime(start_date.strip(), start_date_format)[0:6])).date()
    #tou_date = str_date.strftime('%b %-d') +' - '+ end_date.strftime('%b %-d')
    #str_date, end_date = get_str_end_dates(tou_date) 
    print "start_date>>", str_date
    print "end_date", e_date
    print "mid_date>>>", mid_date
    return mid_date, start_date, end_date



def create_doc(url):
    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8',
        'Connection': 'keep-alive'}
    req = urllib2.Request(url, headers=hdr)
    page = urllib2.urlopen(req)
    data = urllib2.urlopen(req).read()
    doc = lxml.html.fromstring(data)
    return doc

def get_std_loc(venue):
    loc             = {}
    if len(venue.split(',')) == 6:
        _details    = venue.split(',')[1:-2]
        stadium     = ",".join(_details).split(',')[0].strip()
        city        = ",".join(_details).split(',')[1].strip()
        country     = ",".join(_details).split(',')[-1].strip()
        state       = ''
    elif len(venue.split(',')) ==5 :
        _details    = venue.split(',')[1:-2]
        stadium     = ",".join(_details).split(',')[0].strip()
        city        = ",".join(_details).split(',')[1].strip()
        country     = ",".join(_details).split(',')[-1].strip()
        state       = ''
    elif len(venue.split(' ')) == 1:
        country     = venue
        city = state = stadium = ''
    elif len(venue.split(',')) == 3:
        country = venue.split(',')[-1].strip()
        stadium = venue.split(',')[0].strip().replace('"','')
        city    = state = venue.split(',')[1].strip()
    else:
        stadium = city = country = state = ''

    loc = {'city' : city, 'state' : state, 'country' : country, 'stadium' : stadium}

    return loc

class DavisCup(BaseSpider):
    name    = "ttennis_daviscup"
    allowed_domains = ['daviscup.com']
    start_urls = ['http://www.daviscup.com/en/draws-results/davis-cup-structure/diagram.aspx']

    def __init__(self, *args, **kwargs):
            super(DavisCup, self).__init__(*args, **kwargs)

            self.outfile    = file(kwargs.get('outfile', 'TEST_pro_schedules'), "w+")

    def parse(self,response):
        items = []
        hxs = HtmlXPathSelector(response)
        nodes = hxs.select('//div[@class="strWrap"]//a')
        for node in nodes:
            team_link       = "".join(node.select('./@href').extract())
            team_title      = "".join(node.select('./text()').extract())
            if 'http' not in team_link:
                team_link   = "http://www.daviscup.com" + team_link

            if '.aspx' in team_link:
                yield Request(team_link, callback = self.parse_games, meta = {'team_title' : team_title})


    def parse_games(self, response):
        print response.url
        hxs             = HtmlXPathSelector(response)
        game = {}
        grp_num_year    = "".join(hxs.select('//div[@id="titleBar"]/h1/text()').extract())
        if '2014' in grp_num_year:
            #nodes       = hxs.select('//ul[@class="clfx"]//h3/span/a')
            nodes     = hxs.select('//ul[@id="event"]/li[@id="liEventRound"]//ul[@id="roundTie"]//li[@class="tie clfx"]')
            count = 0
            for node in nodes:
                count += 1
                game_date = "".join(node.select('././/../li//../../ul/preceding-sibling::div[@id="divEventRoundDesc"]/p[@id="pHighlight"]/text()').extract()).strip()
                if not game_date:
                    continue
                if "/" not in game_date:
                    game_date = game_date.split(' - ')[0]+" "+game_date.split(' - ')[-1].split(' ')[-1]
                    _date = datetime.datetime.strptime(game_date, "%d %b %Y")
                    game_datetime = _date.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    game_date = game_date.split(' / ')[0]
                    game_date = game_date.split('-')[0]+" " + game_date.split('-')[-1].split( )[-1]+" "+"2014"
                _date = datetime.datetime.strptime(game_date, "%d %b %Y")
                #game_datetime = _date.strftime("%Y-%m-%d %H:%M:%S")
                _date_format = ("%d %b %Y")
                game_datetime  = get_gmt_time(game_date, _date_format)
                _game_sk = _date.strftime("%Y-%m-%d")
                print "game_datetiime", game_datetime
                now = datetime.datetime.now()
                now = now.strftime("%Y-%m-%d")
                if _game_sk <= now:
                    status = "cancelled"
                else:
                    status = "scheduled"
                game['status']= status
                game['game_datetime'] = game_datetime
                event_name = "".join(node.select('././/../li//../../ul/preceding-sibling::div[@id="divEventRoundDesc"]/h2[@id="h2Highlight"]/text()').extract()).strip()
                if event_name in ["First Round","1st Round"]:
                    event_name = "Davis cup First Round"
                elif event_name in ["Second Round", "2nd Round"]:
                    event_name = "Davis cup Second Round"
                elif event_name in ["3rd Round" ,"Third Round"]:
                    event_name = "Davis cup Third Round"
                elif event_name in ["1st Round Play-off","2nd Round Play-off", "Play-offs"]:
                    event_name = "Davis cup Playoffs"
                elif event_name in ["Quarterfinals"]:
                    event_name = "Davis cup Quarterfinals"
                elif event_name in ["Semifinals"]:
                    event_name = "Davis cup Semifinals"
                elif event_name in ["Final"]:
                    event_name = "Davis cup Finals"
                print "event_name", event_name
                game['event_name'] = event_name
                game['tou_name'] = "Davis cup"
                game['source'] ="daviscup_tennis"
                team_nodes = node.select('.//div[@class="panel-r"]//table[@class="dsTable"]//tr/td[@class="team"]/span/a')
                print "len", len(team_nodes)
                _team = ["".join(i.select('./@href').extract()) for i in team_nodes]
                game_note = "".join(node.select('.//h3/span//a/text()').extract())
                if game_note not in ['Results', 'Preview']:
                    if len(_team) == 1:
                        hm_tm = _team[0].rsplit('id=')[-1]
                        aw_tm = "TBD2"
                        game_sk = hm_tm+"_"+aw_tm+"_"+_game_sk.replace('-', '_')+"_"+str(count)
                        game['game_sk'] = game_sk
                        game['home_team_id'] = hm_tm
                        game['away_team_id'] = aw_tm
                    elif len(_team) == 0:
                        hm_tm = "TBD1"
                        aw_tm = "TBD2"
                        game_sk = hm_tm+"_"+aw_tm+"_"+_game_sk.replace('-', '_')+"_"+str(count)
                        game['home_team_id'] = hm_tm
                        game['away_team_id'] = aw_tm
                        game['game_sk'] = game_sk
                    print "game", game
                    #self.outfile.write('%s\n' %(repr(game)))
                else:
                    if len(_team) == 2:
                        hm_tm = _team[0].rsplit('id=')[-1]
                        aw_tm = _team[-1].rsplit('id=')[-1]
                        game['home_team_id'] = hm_tm
                        game['away_team_id'] = aw_tm
                        game_link   = "".join(node.select('.//h3/span//a/@href').extract())
                        game_id     = game_link.split('Id=')[-1]
                        game['game_sk'] = game_id
                        #print "game", game
                        #if game_link and 'http' not in game_link:
                        game_link = "http://www.daviscup.com" + game_link
                        #game_link = "http://www.daviscup.com/en/draws-results/tie/details.aspx?tieId=100021100"
                        #game_link = "http://www.daviscup.com/en/draws-results/tie/details.aspx?tieId=100021099"
                        yield Request(game_link, callback= self.parse_games_details, meta={'team_title' : response.meta.get('team_title'), 'game_id' : game_id, 'game': game, 'game_datetime': game_datetime, 'hm_tm': hm_tm, 'aw_tm': aw_tm, 'event_name': event_name})

    def parse_games_details(self, response):
        hxs         = HtmlXPathSelector(response)
        source = "daviscup_tennis"
        game_dict = response.meta['game']
        home_team_sk = response.meta['hm_tm']
        away_team_sk = response.meta['aw_tm']
        game_datetime= response.meta['game_datetime']
        event_name = response.meta['event_name']
        tou_name = "Davis Cup"
        data = {}
        print "response.url>>>>", response.url
        team_title  = response.meta.get('team_title', '')
        game_id     = response.url.split('Id=')[-1]
        nodes       = hxs.select('//div[@id="webletHome"]')

        for node in nodes:
            home_team_name  = "".join(node.select('.//div[@class="header"]/div[@class="lft"]//a/text()').extract())
            home_team_link  = "".join(node.select('.//div[@class="header"]/div[@class="lft"]//a/@href').extract())
            home_team_img   = "".join(node.select('.//div[@class="header"]/div[@class="lft"]//img/@src').extract())
            home_team_id    = home_team_link.split('id=')[-1].strip()

            if not home_team_img : home_team_img = ''
            if 'http' not in home_team_link: home_team_link = 'http://www.daviscup.com' + home_team_link
            if 'http' not in home_team_img:  home_team_img  = 'http://www.daviscup.com' + home_team_img

            away_team_name  = "".join(node.select('.//div[@class="header"]/div[@class="rht"]//a/text()').extract())
            away_team_link  = "".join(node.select('.//div[@class="header"]/div[@class="rht"]//a/@href').extract())
            away_team_img   = "".join(node.select('.//div[@class="header"]/div[@class="rht"]//img/@src').extract())
            away_team_id    = away_team_link.split('id=')[-1].strip()

            if not away_team_img : away_team_img = ''
            if 'http' not in away_team_link: away_team_link = 'http://www.daviscup.com' + away_team_link
            if 'http' not in away_team_img:  away_team_img  = 'http://www.daviscup.com' + away_team_img

            total_score         = "".join(node.select('.//div[@class="vs"]/text()').extract())
            home_team_score, away_team_score = '', ''
            if '0 : 0 ' not in total_score and 'V' not in total_score:
                home_team_score = total_score.split(':')[0].strip()
                away_team_score = total_score.split(':')[-1].strip()
                final_score     = home_team_score+" - "+away_team_score
                if int(home_team_score)>int(away_team_score):
                    home_win = 1
                    away_win = 0
                elif int(home_team_score)<int(away_team_score):
                    home_win = 0
                    away_win = 1
                elif int(home_team_score)==int(away_team_score):
                    home_win = 1
                    away_win = 1
                if home_win and away_win: team_winner = ''
                elif home_win: team_winner = home_team_id
                elif away_win: team_winner = away_team_id

                game_status     = 'completed'
            elif 'V' in total_score:
                game_status     = 'scheduled'
            elif '0 : 0 ' in total_score:
                game_status     = 'scheduled'
            #if game_status == 'completed':

            if '0 : 0' not in total_score and 'V' not in total_score:
                game_status     = 'completed'
                home_players, away_players  = {}, {}
                full_scores = []
                venue_details   = "".join(node.select('.//div[@class="groupVenue"]/p/text()').extract())
                if not venue_details:
                    venue_details   = "".join(node.select('.//ul[@class="tiedetails clfx"]//li[contains(text(), "Date")]/strong//text()').extract())
                    venue_details_ = "".join(node.select('.//ul[@class="tiedetails clfx"]//li[contains(text(), "Venue")]/strong//text()').extract())
                    tou_year = '2014'
                    str_date = venue_details.split('-')[0].strip()
                    str_date = str_date + ' ' +tou_year 
                    end_date = venue_details.split('-')[1].strip()
                    #end_date = end_date + ' ' +tou_year
                    e_format, s_format, m_format = '%d %b %Y', '%d %b %Y', '%d %b %Y'
                    mid_date, start_date, end_date = get_tou_dates(str_date, end_date, s_format, e_format)
                    start_date    = get_gmt_time(start_date, s_format)
                    end_date  = get_gmt_time(end_date, e_format)
                    mid_date = get_gmt_time(mid_date, m_format)
                    loc             = get_std_loc(venue_details_)
                tou_year        = " ".join(venue_details.split(',')[0].split(' ')[-1:]).strip()
                str_date        = venue_details.split(',')[0].split('-')[0].strip() + ' ' + tou_year
                end_date        = venue_details.split(',')[0].split('-')[-1].strip()
                e_format, s_format, m_format = '%d %b %Y', '%d %b %Y', '%d %b %Y'
                mid_date, start_date, end_date = get_tou_dates(str_date, end_date, s_format, e_format)
                print "mid_date,", mid_date
                print "start_date", start_date
                print "end_date", end_date
                start_date    = get_gmt_time(start_date, s_format)
                end_date  = get_gmt_time(end_date, e_format)
                mid_date = get_gmt_time(mid_date, m_format)

                loc             = get_std_loc(venue_details)
                print "loc>>", loc
                print "venue_details>>>", venue_details

                rounds          = node.select('.//ul[@class="tieList"]/li[contains(@id, "Result_rptTieRubberScores_liRubbers_")]')

                for r in rounds:
                    r_score                 = "".join(r.select('.//div[@class="listScore"]/text()').extract()).strip()
                    _score                  = r_score.split(' ')
                    print "_score", _score
                    if "retired" in r_score.lower():
                        pl_status = "retired"
                    elif "not"  in r_score.lower() or "played" in r_score.lower() or  "not played" in r_score.lower():
                        pl_status = "cancelled"
                    elif r_score == "v":
                        pl_status = "scheduled"
                    elif "walkover" in r_score.lower():
                        pl_status = "walkover"
                    else:
                        pl_status = "completed"
                    home_pl_score           = [i.split('-')[0].strip() for i in _score if i.lower() not in ["retired", "not", "played", "walkover"]]
                    away_pl_score           = [i.split('-')[-1].strip() for i in _score if i.lower() not in ["retired", "not", "played", "walkover"]]

                    round_num               = "".join(r.select('./h3/text()').extract())
                    if "R1" in round_num or "R2" in round_num:
                        pl_game_datetime = start_date
                    elif "R3"  in round_num:
                        pl_game_datetime = mid_date
                    elif "R4" in round_num or "R5" in round_num:
                        pl_game_datetime = end_date

                    round_sk                = round_num + '_' + game_id


                    winner_nodes = r.select('.//div[contains(@class, "winningSide")]/a')
                    winner_id = ["".join(winner.select('./@href').extract()).split('playerid=')[-1].strip() for winner in winner_nodes]

                    home_rounds             = r.select('.//div[contains(@class, "divSide1")]/a')
                    home_player_id = ["".join(home_player.select('./@href').extract()).split('playerid=')[-1].strip() for home_player in home_rounds]

                    away_rounds             = r.select('.//div[contains(@class, "divSide2")]/a')
                    away_player_id      = ["".join(away_player.select('./@href').extract()).split('playerid=')[-1].strip() for away_player in away_rounds]
                    players_game = {}
                    if away_player_id and  home_player_id:
                        #players_game = {'away_player_id' : away_player_id, 'home_player_id': home_player_id, 'winner': winner_id, 'pl_status': pl_status, 'game_sk': round_sk, 'game_datetime': game_datetime, 'reference_url' : response.url, 'home_pl_score': home_pl_score, 'away_pl_score': away_pl_score}
                        data  = {'away_player_id' : away_player_id, 'home_player_id': home_player_id, 'pl_winner': winner_id, 'pl_status': pl_status, 'pl_game_sk': round_sk, 'game_datetime': game_datetime, 'pl_game_datetime': pl_game_datetime, 'reference_url' : response.url, 'home_pl_score': home_pl_score, 'away_pl_score': away_pl_score,'status': game_status, 'game_sk': game_id,'home_team_id' : home_team_id, 'away_team_id' : away_team_id, 'reference_url' : response.url, 'home_final': home_team_score, 'away_final': away_team_score, 'final_score': final_score, 'winner': team_winner, 'loc': loc, 'event_name': event_name, 'source': source, 'tou_name': tou_name}

                        self.outfile.write('%s\n' % repr(data))
            elif '0 : 0' in total_score:
                tou_date = "".join(hxs.select('//ul[@class="tiedetails clfx"]//li[contains(text(), "Date")]/strong/text()').extract()).strip()
                str_date = tou_date.split('-')[0]+'2014'
                end_date =tou_date.split('-')[-1].split('2014')[0].strip()+' 2014'
                e_format, s_format, m_format = '%d %b %Y', '%d %b %Y', '%d %b %Y'
                mid_date, start_date, end_date = get_tou_dates(str_date, end_date, s_format, e_format)
                print "mid_date,", mid_date
                print "start_date", start_date
                print "end_date", end_date
                start_date    = get_gmt_time(start_date, s_format)
                end_date  = get_gmt_time(end_date, e_format)
                mid_date = get_gmt_time(mid_date, m_format)

                venue = "".join(hxs.select('//ul[@class="tiedetails clfx"]//li[contains(text(), "Venue:")]//text()').extract()[-1]).strip()
                loc             = get_std_loc(venue)
                _time = ",".join(hxs.select('//ul[@class="tiedetails clfx"]//li[contains(text(), "Start times")]//strong[contains(text(), "Day")]//text()').extract()).strip()
                rounds          = node.select('.//ul[@class="tieList"]/li[contains(@id, "Result_rptTieRubberScores_liRubbers_")]')

                for r in rounds:
                    r_score                 = "".join(r.select('.//div[@class="listScore"]/text()').extract()).strip()
                    _score                  = r_score.split(' ')
                    print "_score", _score
                    if "retired" in r_score.lower():
                        pl_status = "retired"
                    elif r_score == "v":
                        pl_status =  "scheduled"
                    elif "not"  in r_score.lower() or "played" in r_score.lower() or "not played" in r_score.lower():
                        pl_status = "cancelled"
                    elif "walkover" in r_score.lower():
                        pl_status = "walkover"
                    elif r_score =="":
                        pl_status = "scheduled"
                    else:
                        pl_status = "completed"

                    round_num               = "".join(r.select('./h3/text()').extract())
                    if "R1" in round_num or "R2" in round_num:
                        pl_game_datetime = start_date
                    elif "R3"  in round_num:
                        pl_game_datetime  = mid_date
                    elif "R4" in round_num or "R5" in round_num:
                        pl_game_datetime = end_date

                    round_sk                = round_num + '_' + game_id



                    home_rounds             = r.select('.//div[contains(@class, "divSide1")]/a')
                    home_player_id = ["".join(home_player.select('./@href').extract()).split('playerid=')[-1].strip() for home_player in home_rounds]

                    away_rounds             = r.select('.//div[contains(@class, "divSide2")]/a')
                    away_player_id      = ["".join(away_player.select('./@href').extract()).split('playerid=')[-1].strip() for away_player in away_rounds]
                    players_game = {}
                    if away_player_id and  home_player_id:
                        data  = {'away_player_id' : away_player_id, 'home_player_id': home_player_id, 'pl_status': pl_status, 'pl_game_sk': round_sk, 'game_datetime': game_datetime, 'pl_game_datetime': pl_game_datetime, 'reference_url' : response.url, 'status': game_status, 'game_sk': game_id,'home_team_id' : home_team_id, 'away_team_id' : away_team_id, 'reference_url' : response.url, 'loc': loc, 'event_name': event_name, 'source': source, 'tou_name': tou_name}

                        self.outfile.write('%s\n' % repr(data))

            else:
                #tou_date        = " ".join(node.select('.//div[@class="summary"]//td[@class="rht"]/ul/li[1]/strong/text()').extract())
                # if not tou_date:
                #    continue
                
                #str_date        = tou_date.split('-')[0].strip()
                #end_date        = tou_date.split('-')[-1].strip()
                #tou_year        = "".join(tou_date.split('-')[-1].split(' ')[-1:]).strip()
                #str_date        = str_date + ' ' + tou_year
                #e_format, s_format, m_format = '%d %b %Y', '%d %b %Y', '%d %b %Y'
                #format_e , format_s, format_m = '%Y-%m-%d %H:%M:S', '%Y-%m-%d %H:%M:S', '%Y-%m-%d %H:%M:S'
                #mid_date, start_date, end_date = get_tou_dates(str_date, end_date, s_format, e_format)
               
                venue           = "".join(node.select('.//div[@class="summary"]//td[@class="rht"]/ul/li[2]/strong/text()').extract())

                loc             = get_std_loc(venue)
                data = {'game_datetime': game_datetime, 'status': game_status, 'game_sk': game_id, 'home_team_id' : home_team_id, 'away_team_id' : away_team_id, 'reference_url' : response.url, 'event_name': event_name, 'source': source, 'tou_name': tou_name, 'loc': loc}
                self.outfile.write('%s\n' % repr(data))

