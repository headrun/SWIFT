from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider import VTVSpider, extract_data, get_nodes
import datetime
import time





class WnbaRoster(VTVSpider):
    name = "wnba_roster"
    allowed_domains = ['wnba.com']
    start_urls = ['http://www.wnba.com/statistics/feeds/json/activeleagueplayers.jsp']
    call_sign = {'F' : 'Forward', 'F-C' : 'Forward/Center', 'C' : 'Center', 'G-F' : 'Guard/Forward' , 'G' : 'Guard'}
    team_call_sign = {'Sparks' : 'Los Angeles Sparks', 'Dream' : 'Atlanta Dream', 'Lynx' : 'Minnesota Lynx', 'Sky' : 'Chicago Sky', 'Mercury' : 'Phoenix Mercury', 'Sun' : 'Connecticut Sun', 'SilverStars' : 'San Antonio Silver Stars', 'Fever' : 'Indiana Fever', 'Storm' : 'Seattle Storm', 'Liberty' : 'New York Liberty', 'Shock' : 'Tulsa Shock', 'Mystics' : 'Washington Mystics', 'Stars' : 'San Antonio Stars'}


    records = SportsSetupItem()
    def parse(self,response):
        hxs = HtmlXPathSelector(response)
        data = response.body
        record = eval(data)
        results = record.get('results', '')
        for record in results:
            player_id = record.get('playerId', '').replace(' ','')
            lastname = record.get('lastName', '').replace(' ','')
            nickname = record.get('nickname', '').replace(' ','')
            player_name = nickname + ' ' + lastname
            playercode = record.get('playercode', '').replace(' ','')
            city = record.get('city', '').replace(' ','')
            teamname = record.get('teamName', '').replace(' ','')
            team_name = self.team_call_sign.get(teamname, '')
            callsign = record.get('abbr', '').replace(' ','')
            teamcode = record.get('teamcode', '').replace(' ','')
            position = record.get('position' ,'').replace(' ','')
            role = self.call_sign.get(position, '')
            weight = record.get('weight', '').replace(' ','')
            height = record.get('height', '').replace(' ','')
            player_num = record.get('jersey', '')
            player_link = "http://www.wnba.com/playerfile/%s/index.html" % playercode
            team_link = "http://www.wnba.com/%s" % teamcode


            yield Request(player_link, callback = self.parse_player, meta =
            {'player_name' : player_name, 'player_id' :player_id, 'nickname' :nickname, 'playercode' : playercode, 'city': city, 'teamname' :team_name, 
            'callsign' : callsign, 'teamcode' : teamcode, 'position': role,
            'weight' : weight,'height' : height, 'reference_url' : player_link, 'team_link' : team_link, 'player_num' :player_num})

    def parse_player(self,response):
        hxs = HtmlXPathSelector(response)
        teamcode = response.meta['teamcode']
        player_name = response.meta['player_name']
        player_id = response.meta['player_id']
        nickname = response.meta['nickname']
        playercode = response.meta['playercode']
        city = response.meta['city']
        team_name = response.meta['teamname']
        callsign = response.meta['callsign']
        role = response.meta['position']
        weight = response.meta['weight']
        height = response.meta['height']
        player_link = response.meta['reference_url']
        team_link = response.meta['team_link']
        player_num = response.meta['player_num']
        image = extract_data(hxs, '//div[@class="photo"]/img/@src')
        image_link = "http://www.wnba.com" +image
        date_birth = extract_data(hxs, '//table[@class="pra_stats"]//tr/th[contains(text(), "Born")]/following-sibling::td/text()') 
        if date_birth:
            dob = (datetime.datetime(*time.strptime(date_birth.strip(), '%b %d, %Y',)[0:6])).date()
        else:
            dob = ""
        location = extract_data(hxs, '//table[@class="pra_stats"]//tr/th[contains(text(), "From")]/following-sibling::td/text()')
        team_image = "http://www.wnba.com/media/%s_logo_70x70.gif" % teamcode

    
        self.records['participant_type'] = "roster"
        self.records['season'] = "2014-15"
        self.records['result'] = {'0':{'team_id':team_name, 'player_id':player_id, 'player_role':role, 'player_number':player_num, 'status':"active"}}
        
        yield self.records
