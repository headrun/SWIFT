from vtvspider_dev import VTVSpider, extract_data, extract_list_data, get_nodes
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy_spiders_dev.items import SportsSetupItem
import re

class MLSStandings(VTVSpider):
    name = "mls_standings"
    start_urls = ['http://www.espnfc.com/major-league-soccer/19/table']

    def parse(self, response):
        hxs = Selector(response)
        record  =  SportsSetupItem()
        nodes = get_nodes(hxs, '//div[@id="tables-overall"]//div[@class="responsive-table"]')

        for node in nodes:
            conference = extract_data(node, './/tr[@class="groups"]//a/text()')
            grp_title = "MLS " + conference
            season = extract_data(hxs, '//div[@class="dropdowns"]//p[@class="dropdown-value"]/span/text()')
            team_nodes = get_nodes(node, './/div[@class="responsive-table-content"]//table/tbody/tr[@style="background-color: #FFFFFF"]')

            for team_node in team_nodes:
                team_name = extract_data(team_node, './/td[@class="team"]/a/text()')
                if not team_name:
                    team_name = extract_data(team_node, './td[@class="team"]/text()')
                rank = extract_data(team_node, './/td[@class="pos"]/text()')
                team_url = extract_data(team_node, './/td[@class="team"]/a/@href')
                team_sk = re.findall(r"\d+", team_url)
                if team_sk:
                    team_sk = team_sk[0]
                else:
                    team_sk = team_name.lower().replace(' ', '-')
                team_data = extract_list_data(team_node, './/td[@class="groupA"]/text()')
                total_played, overall_wins, overall_draw, overall_losses, overall_goal_scored, overall_goal_scored_against = team_data
                goal_difference = extract_data(team_node, './/td[@class="gd"]//text()')
                total_points = extract_data(team_node, './/td[@class="pts"]//text()')

                record['participant_type']  = 'team'
                record['result_type']      = 'group_standings'
                record['source'] =  'espn_soccer'
                record['affiliation'] = 'club-football'
                record['season'] = season
                record['tournament'] = grp_title
                record['result'] = {team_sk: {'total_played': total_played,
                                             'overall_wins': overall_wins,
                                             'overall_draw': overall_draw,
                                             'overall_losses': overall_losses,
                                             'overall_goal_scored': overall_goal_scored,
                                             'overall_goal_scored_against': overall_goal_scored_against,
                                             'goal_difference': goal_difference,
                                             'total_points': total_points,
                                             'rank': rank}}
                import pdb;pdb.set_trace()
                yield record
