from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data
from scrapy_spiders_dev.items import SportsSetupItem
import re
import datetime

COUNTRY_URL = "http://www.espncricinfo.com/ci/content/current/player/"

PL_LINK = "http://www.espncricinfo.com"

COUNTRIES = ('Australia', 'Bangladesh', 'England', 'India', 'New Zealand', \
             'Pakistan', 'South Africa', 'Sri Lanka', 'West Indies')

PL_ROLES = {"355269": "All-Rounder", "38058": "All-rounder",
            "51786": "Batsman", "297433": "Wicket-keeper batsman",
            "36993": "Bowler", "37091": "Top Order Batsman",
            "311158": "All-rounder", "49236": "Wicket-keeper",
            "342616": "Batsman", "10656": "Batsman",
            "38924": "Wicket-keeper batsman",
            "297583": "Bowler", "55612": "Batsman",
            "276298": "All-rounder", "46148": "Batsman",
            "39037": "Batsman", "39038": "Wicket-keeper",
            "55354": "Batting All Rounder", "235516": "Wicket Keeper",
            "323244": "Bowler", "56007": "Bowler",
            "46681": "Batsman occasional bowler", "450860": "Strike bowler",
            "233713": "Bowler", "50163": "Bowler", "55894": "All-Rounder",
            "42657": "All-rounder", "53118": "All-rounder",
            "310519": "left-handed batsman,leg-break bowler",
            "230371": "All-Rounder", "55597": "Batsman",
            "46248": "All-rounder", "230852": "Batsman",
            "288992": "bowling all-rounder", "232438": "Batsman",
            "244639": "Bowler", "8917": "All-rounder", "19264": "Bowler",
            "554691": "All-rounder", "378496": "All-rounder",
            "272485": "Batsman", "38264": "Batsman", "497543": "Bowler",
            "36581": "Middle-order batsman", "427178": "Batsman",
            "541224": "All-rounder", "453289": "All-rounder", \
            "290462": "Batsman", "333000": "Batsman", \
            "55666": "Bowler", "392945": "Bowler", "55822": "Batsman", \
            "511532": "Bowler", "56268": "Bowler", "629066": "Bowler", \
            "228349": "All-rounder", "269237": "Wicketkeeper", \
            "373538": "Batsman", "436677": "All-rounder", \
            "401057": "Bowler", "538506": "Bowler", "36948": "Opening Batsman", \
            "506612": "Bowler", "278491": "Batsman", "40250": "All-rounder", \
            "316363": "Batsman", "227771": "Bowler", "43685": "Bowler", \
            "379927": "Fast bowler" , "431909": "Batsman", "495551": "Bowler", \
            "52280": "Batsman", "52622": "Spin bowler", "314622": "Bowler", \
            "398666": "Bowler", "326625": "Bowler", "333780": "Batsman", \
            "7529": "All-rounder", "215058": "Bowler", "559066": "Bowler", \
            "343548": "Bowler", "317248": "All-rounder", "233901": "Batsman", \
            "269280": "Batsman", "379926": "All-Rounde", "550215": "Bowler", \
            "230860": "Bowler", "268845": "Bowler"}

class CricketRoster(VTVSpider):
    name = "cricket_roster"
    start_urls = ['http://www.espncricinfo.com/ci/content/current/player/index.html?']
    participants = {}

    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//div[@class="ciPlayersHomeCtryList"]/ul//li')
        for node in nodes:
            country = extract_data(node, './a/text()')
            link = extract_data(node, './a/@href')
            country_url = COUNTRY_URL + link
            if link:
                country_sk = re.findall(r'\d+', country_url)
                country_sk = country_sk[0]
                yield Request(country_url, callback = self.parse_next, \
                            meta = {'country': country, 'team_sk': country_sk})

    def parse_next(self, response):
        hxs = Selector(response)
        players_li = get_nodes(hxs, '//div[@id="rectPlyr_Playerlistall"]\
                     //table[@class="playersTable"]//tr//td/a')
        last_node = players_li[-1]
        for node in players_li[::1]:
            terminal_crawl = False
            if node == last_node:
                terminal_crawl = True
            player_url = PL_LINK + extract_data(node, './@href')
            yield Request(player_url, callback = self.parse_roster, \
                          meta = {'team_sk': response.meta['team_sk'], \
                          'terminal_crawl': terminal_crawl})

    def parse_roster(self, response):
        hxs = Selector(response)
        record = SportsSetupItem()
        player_id = re.findall(r'player.(\d+).', response.url)
        player_role = extract_data(hxs, '//b[contains(text(), "Playing role")]\
                        //following-sibling::span//text()')
        if not player_role:
            for key, value in PL_ROLES.iteritems():
                if player_id[0] in key:
                    player_role = value
        season = datetime.datetime.now().year
        players = {player_id[0]: {"player_role": player_role,
                                  "player_number": 0,
                                  "season": season, "status": 'active', \
                                  "entity_type": "participant", \
                                  "field_type": "description", "language": "ENG"}}
        self.participants.setdefault\
                (response.meta['team_sk'], {}).update(players)
        if response.meta['terminal_crawl']:
            record['source'] = 'espn_cricket'
            record['season'] = season
            record['result_type'] = 'roster'
            record['participants'] = self.participants
            yield record
