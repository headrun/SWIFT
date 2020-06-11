import re
import time
from scrapy.selector import Selector
from scrapy.http import Request
from vtvspider_dev import VTVSpider, get_nodes, extract_data
from scrapy_spiders_dev.items import SportsSetupItem
import MySQLdb

TOU_RESULTS = "insert ignore into sports_tournaments_results (tournament_id, season, result_type, result_sub_type, result_value) values(%s, '2014', '%s', '%s', %s) ON DUPLICATE KEY UPDATE result_sub_type='%s', result_type = '%s'"
IGNORE_WORDS = ['presented', 'partnership', 'pres. by', ' by ', \
                    ' at ', "men's ", "women's ", "Umag"]
REPLACE_WORDS = ['waste management', 'omega', 'atp ', 'wta ', \
                 "bnp paribas ", "mutua madrilena ", "tour ", \
                 "kdb ", "grc bank", "2013", "skistar", "bet-at-home open - ", \
                 'sony', 'match schedule', 'nurnberger', 'vegeta', \
                 ', kuala lumpur', 'rakuten ', 'aircel ', '2014', ' international ']

def create_cursor():
    con = MySQLdb.connect(host="10.4.15.132", user="root", db="SPORTSDB_BKP")
    cursor = con.cursor()
    return con, cursor

def get_refined_tou_name(tou_name):
    tou_name = tou_name.lower()
    for i in IGNORE_WORDS:
        if i in tou_name:
            tou_name = tou_name.split(i)[0].strip()

    for i in REPLACE_WORDS:
        tou_name = tou_name.replace(i, '').strip()
    return tou_name


def get_player_id(sk):
    existance, result = check_source_keys(sk)
    if existance == 1:
        return result
    else:
        return None

def get_eventid(event, game, cursor, affiliation=''):
    if not event and len(event)<2:
        return 0

    event_name = "%"+event+"%"
    if game == 'golf': event_name = event+"%"

    if affiliation:
        query = "select id from sports_tournaments where game=%s and affiliation=%s and (title like %s)"
        values = (game, affiliation, event_name)
    else:
        query = "select id from sports_tournaments where game=%s and (title like %s)"
        values = (game, event_name)
    con, cursor = create_cursor()
    cursor.execute(query, values)
    event_id = cursor.fetchall()
    if event_id:
        event_id =  event_id[0][0]
    else:
        if affiliation:
            query = "select id from sports_tournaments where game=%s and affiliation=%s and (aka like %s)"
            values = (game, affiliation, event_name)
        else:
            query = "select id from sports_tournaments where game=%s and (aka like %s)"
            values = (game, event_name)
        cursor.execute(query, values)
        event_id = cursor.fetchall()
        con.close()
        if event_id:
            event_id =  event_id[0][0]
        else:
            event_id = 0

    return event_id


SK_CHECK_QUERY = 'select entity_id from sports_source_keys where entity_type="%s" and source_key="%s" and source="%s"'

def check_source_keys(sk):
    try:
        con, cursor = create_cursor()
        id_ = SK_CHECK_QUERY % ('participant', sk, 'espn_tennis')
        cursor.execute(id_)
        con.close()
    except Exception, e:
        print e

    res = [str(i[0]) for i in cursor.fetchall()]
    res_ = "<>".join(res)

    if len(res)==1:
        return 1, res_
    elif len(res)==0:
        return 0, res_
    else:
        return 2, res_



class TennisTourWinner(VTVSpider):
    name = "tennis_tour_winner"
    allowed_domains = []
    start_urls = ['http://espn.go.com/tennis/schedule/_/type/women', \
                    'http://sports.espn.go.com/sports/tennis/schedule']


    def parse(self, response):
        hxs = Selector(response)
        nodes = get_nodes(hxs, '//td[contains(text(), "Completed Tournaments")]/../../tr[contains(@class, "row")]/td[2]//a')
        for node in nodes:
            completed_tournaments_url = extract_data(node, './@href')
            completed_tournaments = extract_data(node, './text()')
        if "bracket?" not in completed_tournaments_url:
            yield Request(completed_tournaments_url,  callback = self.parse_details, meta= {'tournament': completed_tournaments})

    def parse_details(self, response):
        hxs = Selector(response)
        winner2 = ''
        con, cursor = create_cursor()
        winner = extract_data(hxs, '//div[@class="matchTitle"][contains(text(), "- Final:")]//following-sibling::div[@class="span-2 clear"][1]//td[contains(@class, "teamLine")][not(contains(@style, "font-weight:normal;"))]//a/@href')
        winners_group = extract_data(hxs, '//div[@class="matchTitle"][contains(text(), "- Final:")]//text()')
        if "double" in winners_group.lower():
            winner2 = extract_data(hxs, '//div[@class="matchTitle"][contains(text(), "- Final:")]//following-sibling::div[@class="span-2 clear"][1]//td[contains(@class, "teamLine")][not(contains(@style, "font-weight:normal;"))]/../following-sibling::tr[1]//a/@href')
        runner_up = extract_data(hxs, '//div[@class="matchTitle"][contains(text(), "- Final:")]//following-sibling::div[@class="span-2 clear"][1]//td[contains(@class, "teamLine")][contains(@style, "font-weight:normal;")]//a/@href')
        status = extract_data(hxs, '//div[@class="matchTitle"][contains(text(), "- Final:")]//following-sibling::div[@class="span-2 clear"][1]//td[@style="color:#FFF;"][not(contains(@class, "lsTop"))]//text()')
        if not winner:
            return
        winner_id = winner.split('/')[-2]
        winner_id2 = ''
        if winner2:
            winner_id2 = winner2[0].split('/')[-2]
        runner_id = runner_up[0].split('/')[-2]
        if status.lower() == "final":
            if not winner_id2:

                record = {"winner": winner_id, "runner": runner_up, \
                    "tournament": response.meta['tournament'], \
                "winner_group": winners_group, "ref_link": response.url}
                self.update_winners_info(record, cursor)
            else:
                record = {"winner": winner_id, "winner_id2": winner_id2, \
                "runner": runner_up, "tournament": response.meta['tournament'], \
                "winner_group": winners_group, "ref_link": response.url}
                self.update_winners_info(record, cursor)

    def update_winners_info(self, record, cursor):
        con, cursor = create_cursor()
        winner = record.get('winner', '')
        winner1 = record.get('winner2', '')
        tou_name = record.get('tournament', '')
        winner_group = record.get('winner_group', '')
        winner_group = winner_group.replace('Final:', '').replace('-', '').replace("'s", '').strip()
        ref_link = record.get('ref_link', '')

        tou = get_refined_tou_name(tou_name)
        tou_id = get_eventid(tou, 'tennis', cursor)
        t_name = t_id = ''
        if not tou_id and ref_link:
            ref_link = re.findall('tournamentId=(.*?)&', ref_link)[0]
            if ref_link == "350":
                tou = "German Open Tennis Championships"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "192":
                tou = "Budapest Grand Prix"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "18":
                tou = "Hall of Fame Tennis Championships"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "268":
                tou = "Swedish Open"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "7":
                tou = "Swiss Open"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "121":
                tou = "Atlanta Tennis Championships"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "304":
                tou = "Austrian Open Kitzbuhel"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "315":
                tou = "Shanghai Masters (Tennis)"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "148":
                tou = "Stockholm Open"
            elif ref_link == "276":
                tou = "WTA Tour Championships"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "23":
                tou = "Swiss Indoors"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "167":
                tou = "Fed Cup"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "339":
                tou = "ATP World Tour Finals"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "161":
                tou = "Davis Cup"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "205":
                tou = "Sydney International"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "367":
                tou = "Shenzhen Open"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "205":
                tou = "Sydney International"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "283":
                tou = "ATP Apia International Sydney"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "348":
                tou = "PTT Pattaya Open"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "4":
                tou = "Rotterdam Open"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "274":
                tou = "U.S. National Indoor Tennis Championships"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "299":
                tou = "ATP Buenos Aires"
            elif ref_link == "348":
                tou = "PTT Pattaya Open"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "4":
                tou = "Rotterdam Open"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "274":
                tou = "U.S. National Indoor Tennis Championships"
                tou_id = get_eventid(tou, 'tennis', cursor)
            elif ref_link == "299":
                tou = "ATP Buenos Aires"
                tou_id = get_eventid(tou, 'tennis', cursor)

        winner_id = winner1_id = ''
        if winner and t_id:
            winner_id = get_player_id(winner)
            if winner_id:
                cursor.execute(TOU_RESULTS %(t_id, 'winner', winner_group.lower(), winner_id, winner_group.lower(), 'winner'))
                con.close()
                cursor.execute(TOU_RESULTS %(t_id_parent, 'winner', winner_group.lower(), winner_id, winner_group.lower(), 'winner'))
                con.close()
            if winner1:
                winner1_id = get_player_id(winner1)
                cursor.execute(TOU_RESULTS %(t_id, 'winner2', winner_group.lower(), winner1_id, winner_group.lower(), 'winner2'))
                con.close()
                cursor.execute(TOU_RESULTS %(t_id_parent, 'winner2', winner_group.lower(), winner1_id, winner_group.lower(), 'winner2'))
                con.close()
        if winner and tou_id:
            winner_id = get_player_id(winner)
            if winner_id:
                cursor.execute(TOU_RESULTS %(tou_id, 'winner', winner_group.lower(), winner_id, winner_group.lower(), 'winner'))
                con.close()
        if winner1 and tou_id:
            winner1_id = get_player_id(winner1)
            if winner1_id:
                cursor.execute(TOU_RESULTS %(tou_id, 'winner2', winner_group.lower(), winner1_id, winner_group.lower(), 'winner2'))
                con.close()

