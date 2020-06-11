import datetime
from scrapy.selector import Selector
from vtvspider_dev import VTVSpider, \
extract_data, extract_list_data, get_nodes, get_utc_time
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem


events = {'fbs': {'ACC': ('atlantic-coast', 'ACC Football'), \
                  'Big 12': ('big-12', 'Big 12 Football'), \
                  'MAC ': ('mid-american', 'Mid American Football'), \
                  'MWC': ('mountain-west', 'Mountain West Football'), \
                  'Pac-12': ('pac-12', 'Pac-12 Football'), \
                  'SEC': ('southeastern', 'SEC Football'), \
                  'Sun Belt': ('sun-belt', 'Sun Belt Football'), \
                  'C-USA': ('conference-usa', 'Conference USA Football'), \
                  'Big Ten': ('big-ten', 'Big Ten Football'), \
                  'IND': ('independents-fbs-', 'FBS Independents Football'), \
                  'AAC': ('american-athletic-conference', 'American Athletic Football')},
         'fcs': {'Big South': ('big-south', 'Big South Football'), \
                 'CAA': ('colonial-athletic-association', 'CAA Football'), \
                 'Big Sky': ('big-sky', 'Big Sky Football'), \
                 'Ivy': ('ivy-league', 'Ivy Football'), \
                 'southland': ('southland' , 'Southland Football'), \
                 'Missouri Valley': ('missouri-valley', 'Missouri Valley Football'), \
                 'MEAC': ('mid-eastern-athletic', 'MEAC Football'), \
                 'OVC': ('ohio-valley', 'Ohio Valley Football'), \
                 'patriot': ('patriot-league', 'Patriot Football'), \
                 'SWAC': ('southwestern-athletic', 'Southwestern Athletic Football'), \
                 'NEC': ('northeast', 'Northeast Football'), \
                 'pioneer': ('pioneer-league', 'Pioneer Football'),
                 'IND': ('independents-fcs-', 'FCS Independents Football'), \
                 'southern': ('southern', 'Southern Football')} }


class NCAANCFStandings(VTVSpider):
    name = 'ncaa_ncfstandings'
    allowed_domains = ["www.ncaa.com"]
    start_urls = []

    def start_requests(self):
        top_urls = {}
        ncaa_dict = {'fbs': 'http://www.ncaa.com/standings/football/fbs/%s/',
                     'fcs': 'http://www.ncaa.com/standings/football/fcs/%s/'}

        for key, value in ncaa_dict.iteritems():
            if "fbs" in key:
                for aka, event in events['fbs'].iteritems():
                    event_name = event[1]
                    top_urls[value % (event[0])] = event_name
            elif "fcs" in key:
                for aka, event in events['fcs'].iteritems():
                    event_name = event[1]
                    top_urls[value % (event[0])] = event_name

        for url, event_name in top_urls.iteritems():
            yield Request(url, self.parse, meta = {'event_name' :  event_name})

    def parse(self, response):
        hxs = Selector(response)
        record  = SportsSetupItem()
        record['result'] = {}
        result = {}
        event = response.url.split('/')[-2]
        event_name = response.meta['event_name']
        print event_name
        event_name  = event_name
        nodes = get_nodes(hxs, '//div[contains(@class, "ncaa-standings-conference-show-%s")]//tr[@class="even" or @class="odd"]'%(event))
        rank = 0

        for node  in nodes:
            team_link = extract_data(node, './/td[contains(@class, "team")]/a[@class="ncaa-standing-conference-team-link"]/@href')
            if team_link != '':
                rank          += 1
                source_key = team_link.split('/')
                source_key = source_key[-2]
                overall_wins = extract_data(node, './/td[3]//text()')
                wins = overall_wins.split('-')[0]
                loss = overall_wins.split('-')[1]
                pf = extract_data(node, './/td[4]//text()')
                pa = extract_data(node, './/td[5]//text()')
                home = extract_data(node, './/td[6]//text()')
                away = extract_data(node, './/td[7]//text()')
                streak = extract_data(node, './/td[8]//text()')
                standings = {'loss': loss, \
                             'rank':rank, 'wins':wins, \
                             'streak':streak, 'home': home, 'away':away, \
                             'pa': pa, 'pf': pf}
                result.setdefault(source_key, {}).update(standings)
        record['result_type'] = 'group_standings'
        record['participant_type'] = 'team'
        record['season'] = '2015'
        record['source'] = 'ncaa_ncf'
        record['tournament'] = event_name
        record['result'] = result
        yield record

