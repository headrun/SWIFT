from scrapy.http import Request
from scrapy_spiders_dev.items import SportsSetupItem
from scrapy.selector import Selector
from vtvspider_new import VTVSpider, extract_data, \
             get_nodes, extract_list_data

class EuropeRugbyStandings(VTVSpider):
    name = "euro_rugbystandings"
    #start_urls = ['http://en.espn.co.uk/scrum/rugby/page/78838.html']
    start_urls = [ 'http://www.espn.co.uk/rugby/table/_/league/270559', \
                   'http://www.espn.co.uk/rugby/table/_/league/267979', \
                   'http://www.espn.co.uk/rugby/table/_/league/270557', \
                   'http://www.espn.co.uk/rugby/table/_/league/271937']

    def parse(self, response):
        sel = Selector(response)
        tou_type = extract_data(sel, '//div[@class="main-content layout-full"]//h1//text()')
        season =  tou_type.split(' - ')[1]
        tournament_type = tou_type.split(' - ')[0]
        if tournament_type == 'Top 14 Orange':
            nodes = get_nodes(sel, '//table//tr')
            for node in nodes:
                details = extract_list_data(node, './/td//text()')
                position,team, team_img, played, wins, draws, \
                    losses, pts_for, pts_again, \
                     try_for, try_again, tbp, lbp, bp, pts_diff, field_pts = details
                team  = team.lower().encode('utf-8').replace('\xe7', '').replace('\xc3\xa7', '')
                if not team:
                    continue

                record = SportsSetupItem()
                record['result_type'] = "tournament_standings"
                record['season'] = season
                record['tournament'] = "France Top 14"
                record['participant_type'] = "team"
                record['source'] = 'espn_rugby'
                record['result'] = {team: {'rank': position,  'played': played, \
                       'wins': wins, 'draws': draws, 'losses': losses, \
                       'pts_for': pts_for, 'pts_again': pts_again, \
                       'pts_diff': pts_diff, 'try_for': try_for, \
                       'try_again': try_again, 'tbp': tbp, \
                       'lbp': lbp, 'field_pts': field_pts, 'bp': bp}}
                yield record


        elif tournament_type == 'Guinness PRO12':
            nodes = get_nodes(sel, '//table//tr')
            position = 0
            for node in nodes:
                details = extract_list_data(node, './/td//text()')
                position, team, team_img, played, wins, draws, \
                    losses, pts_for, pts_again, \
                    try_for, try_again, tbp, lbp, bp, pts_diff, field_pts = details
                team = team.lower().encode('utf-8').replace('\xe7', '').replace('\xc3\xa7', '')

                if not team:
                    continue

                record = SportsSetupItem()
                record['result_type'] = "tournament_standings"
                record['season'] = season
                record['tournament'] = "Guinness Pro12"
                record['participant_type'] = "team"
                record['source'] = 'espn_rugby'
                record['result'] = {team: {'rank': position,  'played': played, \
                       'wins': wins, 'draws': draws, 'losses': losses, \
                       'pts_for': pts_for, 'pts_again': pts_again, \
                       'pts_diff': pts_diff, 'try_for': try_for, \
                       'try_again': try_again, 'tbp': tbp, \
                       'lbp': lbp, 'field_pts': field_pts, 'bp': bp }}
                yield record

        elif tournament_type == 'Aviva Premiership':
            nodes = get_nodes(sel, '//table//tr')
            for node in nodes:
                details = extract_list_data(node, './/td//text()')
                rank, team, team_img, played, wins, draws, \
                    losses, pts_for, pts_again, \
                    tbp, lbp, bp , pts_diff, field_pts = details
                team = team.lower().encode('utf-8').replace('\xe7', '').replace('\xc3\xa7', '')
                if not team:
                    continue

                record = SportsSetupItem()
                record['result_type'] = "tournament_standings"
                record['season'] = season
                record['tournament'] = "English Premiership (rugby union)"
                record['participant_type'] = "team"
                record['source'] = 'espn_rugby'
                record['result'] = {team: {'rank': rank, 'played': played, \
                       'wins': wins, 'draws': draws, 'losses': losses, \
                       'pts_for': pts_for, 'pts_again': pts_again, \
                       'pts_diff': pts_diff, 'tbp': tbp, 'lbp': lbp, \
                       'bp': bp, 'field_pts': field_pts }}
                yield record

        elif tournament_type == 'European Rugby Champions Cup':
            nodes = get_nodes(sel, '//table//tr')
            for node in nodes:
                group_name = extract_list_data(node, './preceding-sibling::thead[@class="standings-categories"]//th[1]//text()')[-1]
                group_name = "European Rugby Champions Cup " +group_name
                position  = extract_data(node, './/td//span[@class="number"]//text()')
                team      = extract_data(node, './/td//a//span[@class="team-names"]//text()').lower(). \
                encode('utf-8').replace('\xe7', '').replace('\xc3\xa7', '')
                if not team:
                    continue
                played    = extract_data(node, './/td[2]//text()')
                wins      = extract_data(node, './/td[3]//text()')
                draws     = extract_data(node, './/td[4]//text()')
                losses    = extract_data(node, './/td[5]//text()')
                pts_for   = extract_data(node, './/td[6]//text()')
                pts_again = extract_data(node, './/td[7]//text()')
                try_for   = extract_data(node, './/td[8]//text()')
                bp        = extract_data(node, './/td[9]//text()')
                pts_diff   = extract_data(node, './/td[10]//text()')
                field_pts = extract_data(node, './/td[11]//text()')

                record = SportsSetupItem()
                record['result_type'] = "group_standings"
                record['season'] = season
                record['tournament'] = group_name
                record['participant_type'] = "team"
                record['source'] = 'espn_rugby'
                record['result'] = {team: {'rank': position,  'played': played, \
                       'wins': wins, 'draws': draws, 'losses': losses, \
                       'pts_for': pts_for, 'pts_again': pts_again, \
                       'pts_diff': pts_diff, 'try_for': try_for, \
                       'bp': bp, 'field_pts': field_pts }}
                yield record


