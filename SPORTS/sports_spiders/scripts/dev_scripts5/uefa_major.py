import re
import time
import datetime
import traceback

from vtvspider import VTVSpider, extract_data, extract_list_data, get_nodes 
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


class UefaMajorLegaues(VTVSpider):
    name            = "uefa_major_league"
    start_urls      = []

    tournament     = {'1': "UEFA Champions League", '14': 'UEFA Europa League'}
    outfile        = file('TEST_SCHE', "w+")
    country_dict   = { 'BEL' : 'Belgium', 'SCO' : 'Scotland', 'TUR' : 'Turkey',
                            'CYP' : 'Cyprus', 'GRE' : 'Greece', 'GER' : 'Germany',
                            'HUN' : 'Hungary', 'KAZ' : 'Kazakhstan', 'ESP' : 'Spain',
                            'ENG' : 'England', 'FRA' : 'France', 'CRO' : 'Croatia',
                            'NED' : 'Netherlands', 'MLT' : 'Malta', 'ALB' : 'Albania',
                            'ROU' : 'Romania', 'ITA' : 'Italy', 'RUS' : 'Russia',
                            'CZE' : 'Czech Republic', 'SVK' : 'Slovakia',
                            'NIR' : 'Nigeria', 'ISL' : 'Iceland', 'AUT' : 'Austria',
                            'SUI' : 'Switzerland', 'UKR' : 'Ukraine', 'MDA' : 'Moldova',
                            'POL' : 'Poland', 'GEO' : 'Georgia', 'ISR' : 'Israel',
                            'SUI' : 'Switzerland', 'SWE' : 'Sweden', 'LUX' : 'Luxembourg',
                            'NOR' : 'Norway', 'MNE' : 'Montenegro',
                            'SRB' : 'Serbia', 'SVN' : 'Slovenia', 'FOR' : 'Faroe Islands',
                            'LIT' : 'Lithuania', 'FIN' : 'Finland'}

    def start_requests(self):
        uefa_leagues    = {'1': "UEFA Champions League", '14': 'UEFA Europa League'}
        days            = ['0', '-1', '-2', '-3', '-4', '-5', '-6', '-7', '-8',
                           '-9', '-10', '-11', '-12', '-13', '-14', '1', '2',
                           '3', '4', '5', '6', '7', '8', '9', '10', '11', '12',
                           '13', '14']
        url             = 'http://www.uefa.com/library/matches/idcup=%s/season=2014/fixtures/day=%s/session=%s/_matchesbydate.html'
        start_urls      = [ url %(id, day,i) for i in [1,2] for day in days for id, league in uefa_leagues.iteritems()]

        for i in [1,2]:
            for day in days:
                for id, league in uefa_leagues.iteritems():
                    #r = url %(id, day,i)
                    r = url %('1', '0', '1')
                    yield Request(r, callback = self.parse, meta = {'tou' : id})

    def parse(self, response):
        hxs           = HtmlXPathSelector(response)
        teams         = get_nodes(hxs, '//table//tbody[contains(@class, "m")]')

        for team in teams:
            url         = extract_data(team, './/a[@class="sc"]/@href')
            other_info  = extract_data(team, './/tr[@class="referee_stadium"]//text()')
            #stadium     = re.findall('Stadium: (.*)', other_info)
            event_name  = extract_data(team, './/span[@class="rname"]//text()')
            date_time   = extract_data(team, './/tr//td[@class="c b score nob"]//text()').strip()
            status      = extract_data(team, './/tr[@class="reasonwin"]//text()')
            venue       = "".join(re.findall('Stadium: (.*)', other_info))
            loc         = {}

            if venue:
                venue   = venue.split(",")
                loc['stadium'] = venue[0]

                if '(' in venue[1]:
                    loc_det = venue[1].split('(')
                    loc['city']    = loc_det[0].strip()
                    loc['country'] = self.country_dict.get(loc_det[1].replace(')', '').strip(), '')
                else:
                    loc['city']    = venue[1].strip()
                    loc['country'] = ''
            else:
                stadium = city = country = ''

            if not url: continue

            url         = 'http://www.uefa.com' + url
            tournament  = self.tournament[response.meta['tou']]

            if 'TBD' in other_info or "." in date_time or "Match cancelled" in status:
                url      = url.replace('/index.html' ,'/prematch/player-lists/index.html')
                yield Request(url, callback=self.parse_schedules,
                              meta = {'purl'        : response.url ,'url'    : url,
                                      'date_time'  : date_time, 'event_name' : event_name,
                                      'status'  : status,      'tournament'  : tournament,
                                      'loc'      : loc})
            else:
                url      =  url.replace('/index.html', "/postmatch/index.html")
                yield Request(url, callback = self.parse_schedules,
                              meta = {'purl'      : response.url ,'url' : url,
                                      'date_time': date_time,
                                      'event_name': event_name, 'status': status,
                                      'tournament': tournament, 'loc'   : loc})



    def parse_schedules(self, response):
        hdoc        = HtmlXPathSelector(response)
        items       = []
        result      = {}
        sportssetupitem             = SportsSetupItem()
        sportssetupitem['source']   = "uefa_soccer"
        sportssetupitem['rich_data']= {}
        final_details               = {}
        home_stats                  = {}
        away_stats                  = {}
        game_sk       = re.findall('match=(\d+)', response.url)
        home_team_url = extract_data(hdoc, '//table[@class="mb-beta"]//tr//td[contains(@class,"mb-logo mb-logo-h")]//a/@href')
        away_team_url = extract_data(hdoc, '//table[@class="mb-beta"]//tr//td[contains(@class,"mb-team mb-team-a")]//div[@class="mb-team-name"]//a/@href')
        home_team     = extract_data(hdoc, '//table[@class="mb-beta"]//tr//td[contains(@class,"mb-logo mb-logo-h")]//a/@title')
        away_team     = extract_data(hdoc, '//table[@class="mb-beta"]//tr//td[contains(@class,"mb-team mb-team-a")]//div[@class="mb-team-name"]//a/@title')
        total_score   = extract_list_data(hdoc, '//td[contains(@class,"mb-result goals")]//text()')
        home_id       = "".join(re.findall('club=(\d+)', home_team_url))
        away_id       = "".join(re.findall('club=(\d+)', away_team_url))

        sportssetupitem['source_key']           = game_sk[0]

        #participants details
        participants                            = {}
        participants[home_id]                   = (1, '')
        participants[away_id]                   = (0, '')
        sportssetupitem['participants']         = participants

        final_details['status']                 = ''
        final_details['total_score']            = ''
        sportssetupitem['rich_data']['channels']= ''
        sportssetupitem['game']                 = 'soccer'
        sportssetupitem['tournament']           = response.meta['tournament']
        sportssetupitem['participant_type']     = 'team'

        #location details
        locations = response.meta['loc']
        if locations:
            sportssetupitem['rich_data']['location'] = locations

        if "Group stage" in response.meta['event_name']:
            sportssetupitem['event']  = '%s Group Stage' % (response.meta['tournament'])
        elif "Play-offs" in response.meta['event_name']:
            sportssetupitem['event']  = '%s Play-offs' % (response.meta['tournament'])
        elif "qualifying" in response.meta['event_name']:
            sportssetupitem['event']  = '%s Qualifying' % (response.meta['tournament'])
        elif "Knockout" in response.meta['event_name']:
            sportssetupitem['event']  = '%s Knockout Phase' % (response.meta['tournament'])
        elif "Final" in response.meta['event_name']:
            sportssetupitem['event']  = '%s Final' % (response.meta['tournament'])

        if '-' in total_score[0]:
            sportssetupitem['game_status']      = 'completed'
            final_details['total_score']        = total_score
            stats_nodes = get_nodes(hdoc, '//table[@class="castrolMtStat"]//tr')

            hm_stats = dict()
            aw_stats = dict()
            for stat in stats_nodes:
                name = extract_data(stat, './/td[@class="statName"]//text()')
                if name:
                    home = extract_data(stat, './/div[contains(@class, "tmTailLeft")]//p//text()')
                    away = extract_data(stat, './/div[contains(@class, "tmTailRight")]//p//text()')
                    try:
                        if home: home_stats[name] = home
                        if away: away_stats[name] = away
                    except:
                        print traceback.format_exc()
                        pass

        else:
            sportssetupitem['game_status']      = 'scheduled'

        sportssetupitem['reference_url']        = response.url


        result.setdefault(home_id, {}).update({'cornerkicks': home_stats.get('Corners',''),
                               'fouls'      : home_stats.get('Fouls suffered', ''),
                               'offsides'   : home_stats.get('Offsides',''),
                               'possession' : '','red_cards':home_stats.get('Red Cards',''),
                               'saves'      : home_stats.get('blocked', ''),
                               'shots'      : home_stats.get('Total attempts',''),
                               'shots_on_goal' : home_stats.get('on target', ''),
                               'yellow_cards'  : home_stats.get('Yellow cards', ''),
                               'final' : home_stats.get('Goals scored', '')})

        result.setdefault(away_id, {}).update({'cornerkicks'   : away_stats.get('Corners',''),
                               'fouls'         : away_stats.get('Fouls suffered', ''),
                               'offsides'      : away_stats.get('Offsides',''), 'away_possession' : '',
                               'red_cards'     : away_stats.get('Red Cards',''),
                               'saves'         : away_stats.get('blocked', ''),
                               'shots'         : away_stats.get('Total attempts',''),
                               'shots_on_goal' : away_stats.get('on target', ''),
                               'yellow_cards'  : away_stats.get('Yellow cards', ''),
                               'final' : away_stats.get('Goals scored', '')})

        winner = ''

        if sportssetupitem['game_status'] == 'completed':
            home_score = int(home_stats.get('Goals scored', '0'))
            away_score = int(away_stats.get('Goals scored', '0'))

            result.setdefault(home_id, {})['final'] = home_score
            result.setdefault(away_id, {})['final'] = away_score

            if home_score > away_score:
                winner = "".join(re.findall('club=(\d+)', home_team_url))
                result.setdefault('0', {})['winner'] = winner
            elif home_score < away_score:
                winner = "".join(re.findall('club=(\d+)', away_team_url))
                result.setdefault('0', {})['winner'] = winner

            final_details['home_goal_stats'] = {}
            final_details['away_goal_stats'] = {}

            home_stats = get_nodes(hdoc, '//div[contains(@class, "home res-scorers")]//li')
            if home_stats:
                for stat in home_stats:
                    player_url = extract_data(stat, './/a/@href')
                    player_sk  = "".join(re.findall('player=(\d+)', player_url))
                    goal_mins  = extract_data(stat, './text()').strip().replace(")", "").replace("(", "")
                    if "," in goal_mins:
                        goal_mins = goal_mins.split(",")

                        for goal in goal_mins:
                            final_details['home_goal_stats'][goal.strip()] = player_sk
                    else:
                        final_details['home_goal_stats'][goal_mins] = player_sk

            away_stats = get_nodes(hdoc, '//div[contains(@class, "away res-scorers")]//li')

            if away_stats:
                for stat in away_stats:
                    player_url = extract_data(stat, './/a/@href')
                    player_sk  = "".join(re.findall('player=(\d+)', player_url))
                    goal_mins  = extract_data(stat, './text()').strip().replace(")", "").replace("(", "")

                    if "," in goal_mins:
                        goal_mins = goal_mins.split(",")

                        for goal in goal_mins:
                            final_details['away_goal_stats'][goal.strip()] = player_sk
                    else:
                        final_details['away_goal_stats'][goal_mins] = player_sk


        if sportssetupitem['game_status'] == 'completed':
            results = {
                       'away_goal_stats'    : final_details['away_goal_stats'],
                       'home_goal_stats'    : final_details['home_goal_stats']
                      }


        sportssetupitem['result']               = result
        date_time   = extract_data(hdoc, "//h2[@class='seotitle']//text()")
        date_time   = "".join(re.findall('(\d+/\d+/\d+)', date_time))

        _time       = extract_data(hdoc, '//p[@class="venue-details"]//text()')
        _time       = "".join(re.findall('(\d+:\d+)CET', _time))

        if date_time:
            if len(response.meta['date_time']) > 2:
                if  _time:
                    date_time   = date_time + " " + _time.replace(":",".") + ".00"
                else:
                    date_time   = date_time + " " + response.meta['date_time'] + ".00"
            else:
                date_time   = date_time + " " + "00.00.00"

            date_object = datetime.datetime(*time.strptime(date_time, '%d/%m/%Y %H.%M.%S')[0:6])
            game_date   = date_object.strftime('%Y-%m-%d %H:%M:%S')
            now         = datetime.datetime.utcnow()
            date_now    = now.strftime("%Y-%m-%d %H:%M:%S")

            game_dt_time    = datetime.datetime(*time.strptime(game_date, '%Y-%m-%d %H:%M:%S')[0:6])
            utc_dt_time     = game_dt_time - datetime.timedelta(hours=2)
            utc_game_date   = time.strftime("%Y-%m-%d %H:%M:%S", utc_dt_time.timetuple())

            sportssetupitem['game_datetime']    = utc_game_date
            sportssetupitem['affiliation']      = "uefa"

        if "cancelled" in response.meta['status']:
            sportssetupitem['game_status'] = 'cancelled'
        elif "scores" in self.spider_type  and utc_dt_time.date() < now.date():
            sportssetupitem['game_status'] = 'completed'
        elif "scores" in self.spider_type  and utc_dt_time < now and now > (utc_dt_time + datetime.timedelta(hours=2)):
            sportssetupitem['game_status'] = "completed"
        elif "scores" in self.spider_type and utc_dt_time <= now and now < (utc_dt_time + datetime.timedelta(hours=2)):
            sportssetupitem['game_status'] = "ongoing"
        elif "-" in date_time[0] and "scores" in self.spider_type :
            sportssetupitem['game_status'] = 'completed'
        elif self.spider_type == "schedules" and utc_dt_time > now:
            self.outfile.write("%s\n" %repr(sportssetupitem))

        if sportssetupitem['game_status'] == 'completed':
            score = '%s-%s' %(home_score, away_score)
            result.setdefault('0', {})['score'] = score

        items.append(sportssetupitem)
        for item in items:
            yield item
            self.outfile.write('%s\n' % (item))
