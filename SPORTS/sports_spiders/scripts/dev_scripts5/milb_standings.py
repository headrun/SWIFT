from scrapy_spiders_dev.items import SportsSetupItem
from vtvspider_dev import VTVSpider


class MexBaseballStandings(VTVSpider):
    name = "mexbaseball_standings"
    start_urls = ['http://www.milb.com/lookup/json/named.milb_standings_display_flip.bam?sit_code=%27h0%27&sit_code=%27h1%27&sit_code=%27h2%27&league_id=125&org_history.org_id=125&season=2015']

    def parse(self, response):
        data = eval(response.body)
        st_nodes = data['milb_standings_display_flip']['standings_all']['queryResults']['row']
        count = 0
        for st_node in st_nodes:
            count += 1
            rank = count
            tou_name = st_node['division']. \
            replace('Norte', 'North Coference'). \
            replace('Sur', 'South Conference')
            if "South Conference" in tou_name:
                rank  = count - 8
            team_sk  = st_node['team_abbrev']
            home     = st_node['home']
            away     = st_node['away']
            last_ten = st_node['last_ten']
            loss     = st_node['l']
            wins     = st_node['w']
            pct      = st_node['pct']
            tm_gb       = st_node['gb']
            elim     = st_node['elim']
            div      = st_node['vs_division']
            strk     = st_node['streak']

            record = SportsSetupItem()
            record['result_type'] = "group_standings"
            record['season'] = response.url.split('=')[-1].strip()
            record['tournament'] = tou_name
            record['participant_type'] = "team"
            record['source'] = 'MILB'
            record['result'] = { team_sk: {'rank': rank, 'wins': wins, \
                                'losses': loss, \
                                'pct': pct, 'gb': tm_gb, 'elim': elim, \
                                'home': home, 'away': away, 'div': div, \
                                'last_ten': last_ten, 'strk': strk}}
            yield record

