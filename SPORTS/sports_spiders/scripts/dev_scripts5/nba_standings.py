from scrapy.selector import HtmlXPathSelector
from vtvspider_dev import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
import re

callsign_map = {"boston celtics": "BOS", "chicago bulls": "CHI", \
                    "atlanta hawks" : "ATL", "new jersey nets" : "BKN", \
                    "cleveland cavaliers" : "CLE", "charlotte hornets" : "CHA", \
                    "new york knicks" : "NYK", "detroit pistons" : "DET", \
                    "miami heat" : "MIA", "philadelphia sixers" : "PHI", \
                    "indiana pacers": "IND", "orlando magic" : "ORL", \
                    "toronto raptors" : "TOR", "milwaukee bucks" : "MIL", \
                    "washington wizards" : "WAS", "dallas mavericks" : "DAL", \
                    "denver nuggets" : "DEN", "golden state warriors" : "GSW", \
                    "houston rockets" : "HOU", "minnesota timberwolves" : "MIN", \
                    "l.a. lakers": "LAL", "memphis grizzlies" : "MEM", \
                    "portland blazers" : "POR", "portland trail blazers": "POR", \
                    "l.a. clippers" : "LAC", "los angeles clippers" : "LAC", \
                    "los angeles lakers": "LAL", "philadelphia 76ers": "PHI", \
                    "new orleans hornets" : "NOH", \
                    "oklahoma city thunder" : "OKC", "phoenix suns" : "PHO", \
                    "san antonio spurs" : "SAS", "utah jazz" : "UTA", \
                    "sacramento kings" : "SAC", "fbu": "FBU", "rmd": "RMD", \
                    "mps": "MPS", "eam": "EAM", "fcb": "FCB", "mac": "MAC", \
                    "ALB": "ALB", "new orleans pelicans": "NOP", \
                    "eastern conference" : "ECF", 'western conference' : "WCF", \
                    "brooklyn nets": "BKN"}

def modify(data):
    try:
        data = ''.join([chr(ord(x)) for x in data]).decode('utf8').encode('utf8')
        return data
    except ValueError or UnicodeDecodeError or UnicodeEncodeError:
        try:
            return data.encode('utf8')
        except  ValueError or UnicodeEncodeError or UnicodeDecodeError:
            try:
                return data
            except ValueError or UnicodeEncodeError or UnicodeDecodeError:
                try:
                    return data.encode('utf-8').decode('ascii')
                except UnicodeDecodeError:
                    data = normalize('NFKD', data.decode('utf-8')).encode('ascii')
                    return data

class NBAStandings(VTVSpider):
    name = "nba_standings"
    start_urls = []
    record = SportsSetupItem()
    result = {}

    def start_requests(self):
        requests = []
        top_urls = {'division': 'http://www.nba.com/standings/team_record_comparison/conferenceNew_Std_Div.html',\
                    'conference': 'http://www.nba.com/standings/team_record_comparison/conferenceNew_Std_Cnf.html',\
                    'league': 'http://espn.go.com/nba/standings/_/group/1'}

        for standings_type, url in top_urls.iteritems():
            yield Request(url, callback = self.parse, meta = {'standings_type': standings_type})

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        if response.meta['standings_type'] == 'conference':
            season = extract_data(hxs, '//table[@class="genStatTable mainStandings"]//tr/td[@id="tdimage"]/text()')
            season = "". join(re.findall('(\d+-\d+).*', season))
            nodes = get_nodes(hxs, '//table[@class="genStatTable mainStandings"]//tr[@class="title"]/following-sibling::tr')
            conf_rank = 0
            for node in nodes:
                conf_rank += 1
                if conf_rank >= 16:
                    conf_rank -= 17
                conference = "NBA" + " " + extract_data(node, './preceding-sibling::tr[@class="title"]/td[@class="name"]/text()') + " Conference"
                if "EasternWestern" in conference:
                    conference = "NBA" + " " + "western" + " Conference"
                team_half = extract_data(node, './td[@class="team"]/a/text()').lower()
                link = extract_data(node, './td[@class="team"]/a/@href').replace('/', '')
                if link not in team_half:
                    team = team_half+ " "+link
                    team = callsign_map.get(team, '')
                else:
                    team = team_half
                    team = callsign_map.get(team, '')


                wins = extract_data(node, './td[@class="team"]/following-sibling::td[1]/text()')
                losses = extract_data(node, './td[@class="team"]/following-sibling::td[2]/text()')
                pct = extract_data(node, './td[@class="team"]/following-sibling::td[3]/text()')
                gb = extract_data(node, './td[@class="team"]/following-sibling::td[4]/text()')
                conf = extract_data(node, './td[@class="team"]/following-sibling::td[5]/text()')
                div = extract_data(node, './td[@class="team"]/following-sibling::td[6]/text()')
                home = extract_data(node, './td[@class="team"]/following-sibling::td[7]/text()')
                road = extract_data(node, './td[@class="team"]/following-sibling::td[8]/text()')
                l10 = extract_data(node, './td[@class="team"]/following-sibling::td[9]/text()')
                streak = extract_data(node, './td[@class="team"]/following-sibling::td[10]/text()')
                if not team:
                    continue
                self.record['participant_type'] = "team"
                self.record['result_type'] = "group_standings"
                self.record['tournament'] = conference
                self.record['season'] = season
                self.record['source'] = "NBA"
                self.record['result'] = {team: {'win' : wins, 'loss' : losses,  'pct' : pct, \
                                        'home' : home, 'road' : road, 'div' : div, 'div_pct': l10, \
                                        'streak': streak, 'gb': gb, 'conf': conf, 'rank':conf_rank}}

                yield self.record

        elif response.meta['standings_type'] == 'division':
            nodes = get_nodes(hxs, '//table[@class="genStatTable mainStandings"]//tr[@class="title"]')
            season = extract_data(hxs, '//table[@class="genStatTable mainStandings"]//tr/td[@id="tdimage"]/text()')
            season = "". join(re.findall('(\d+-\d+).*', season))
            for di_node in nodes:
                division = "NBA" + " " + extract_data(di_node, './td[@class="name"]/text()')
                count = 0
                divs  = get_nodes(di_node, './following-sibling::tr[1]|./following-sibling::tr[2]|./following-sibling::tr[3]|./following-sibling::tr[4]|./following-sibling::tr[5]')
                for div_node in divs:
                    rank = count + 1
                    team_half = extract_data(div_node, './td[@class="team"]/a/text()').lower()
                    link = extract_data(div_node, './td[@class="team"]/a/@href').replace('/', '')
                    if link not in team_half:
                        team = team_half+ " "+link
                        team = callsign_map.get(team, '')
                    else:
                        team = team_half
                        team = callsign_map.get(team, '')
                    rank = extract_data(div_node, './td[@class="team"]/sup[@class="super"]/text()')
                    wins = extract_data(div_node, './td[@class="team"]/following-sibling::td[1]/text()')
                    losses = extract_data(div_node, './td[@class="team"]/following-sibling::td[2]/text()')
                    pct = extract_data(div_node, './td[@class="team"]/following-sibling::td[3]/text()')
                    gb = extract_data(div_node, './td[@class="team"]/following-sibling::td[4]/text()')
                    conf = extract_data(div_node, './td[@class="team"]/following-sibling::td[5]/text()')
                    div = extract_data(div_node, './td[@class="team"]/following-sibling::td[6]/text()')
                    home = extract_data(div_node, './td[@class="team"]/following-sibling::td[7]/text()')
                    road = extract_data(div_node, './td[@class="team"]/following-sibling::td[8]/text()')
                    l10 = extract_data(div_node, './td[@class="team"]/following-sibling::td[9]/text()')
                    streak = extract_data(div_node, './td[@class="team"]/following-sibling::td[10]/text()')
                    self.record['participant_type'] = "team"
                    self.record['result_type'] = "group_standings"
                    self.record['tournament'] = division
                    self.record['season'] = season
                    self.record['source'] = "NBA"
                    self.record['result']= {team : {'win' : wins, 'loss' : losses,  'pct' : pct, \
                                            'home' : home, 'road' : road, 'div' : div, 'div_pct' : l10, \
                                            'streak': streak,'rank':rank, 'gb': gb, 'conf': conf}}
                    yield self.record

        elif response.meta['standings_type'] == 'league':
            tou_name = extract_data(hxs,'//div[@class="mod-content"]//table//tr[@class="stathead"]//text()')
            season   = extract_data(hxs,'//div[@class="mod-content"]/h1[@class="h2"]/text()')
            season  = re.findall('.* (\d+-\d+)', season)
            season  = "".join(season)
            nodes = get_nodes(hxs,'//div[@class="mod-content"]//table//tr[contains(@class, "row team")]')
            count = 0
            for node in nodes:
                count += 1
                rank             = count
                team_short_title = extract_data(node,'./td/a/text()')
                team_link        = extract_data(node,'./td/a/@href')
                team_title       = team_link.split('/')[-1].replace('-', ' ').lower()
                team_title       = callsign_map.get(team_title, '')
                win              = extract_data(node,'./td[@align="left"]/following-sibling::td[1]/text()')
                loss             = extract_data(node,'./td[@align="left"]/following-sibling::td[2]/text()')
                pct              = extract_data(node,'./td[@align="left"]/following-sibling::td[3]/text()')
                gb               = modify(extract_data(node,'./td[@align="left"]/following-sibling::td[4]/text()'))
                if gb == "\xc2\xa0\xc2\xbd":
                    gb = "0.5"
                elif "\xc2\xa0\xc2\xbd" in gb:
                    gb = gb.replace('\xc2\xa0\xc2\xbd','.5')
                elif "\xc2\xbd" in gb:
                    gb = "0.5"

                home = extract_data(node,'./td[@align="left"]/following-sibling::td[5]/text()')
                road = extract_data(node,'./td[@align="left"]/following-sibling::td[6]/text()')
                div  = extract_data(node,'./td[@align="left"]/following-sibling::td[7]/text()')
                conf = extract_data(node,'./td[@align="left"]/following-sibling::td[8]/text()')
                pf   = extract_data(node,'./td[@align="left"]/following-sibling::td[9]/text()')
                pa   = extract_data(node,'./td[@align="left"]/following-sibling::td[10]/text()')
                diff = extract_data(node,'./td[@align="left"]/following-sibling::td[11]/text()')
                strk = extract_data(node,'./td[@align="left"]/following-sibling::td[12]/text()')
                l10  =  extract_data(node,'./td[@align="left"]/following-sibling::td[13]/text()')
                self.record['participant_type'] = "team"
                self.record['result_type'] = "tournament_standings"
                self.record['tournament'] = "NBA Basketball"
                self.record['season'] = season
                self.record['source'] = "NBA"
                self.record['affiliation'] = 'nba'
                self.record['result']= {team_title : {'win': win, 'loss': loss, 'pct': pct, 'gb': gb,'home': home, \
                                       'road': road, 'div': div, 'conf': conf, 'pf': pf, 'pa': pa,'diff': diff, \
                                       'strk': strk, 'l10': l10, 'rank': rank}}
                import pdb;pdb.set_tarce()
                yield self.record
