#!/usr/bin/env python
import json
import datetime
from scrapy.selector import Selector
from scrapy.http import Request
from sports_spiders.items import SportsSetupItem
from sports_spiders.vtvspider import VTVSpider, extract_data, \
    extract_list_data, get_utc_time, get_tzinfo, mysql_connection


def get_game_date_time(game_time):
    if "TBA" in game_time:
        game_time = game_time.split('TBA,')[-1].strip()
        pattern = '%B %d, %Y'
        time_unknown = 1
        game_dt = get_utc_time(game_time, pattern, 'US/Eastern')
    elif "TBD" in game_time:
        game_time = game_time.split('TBD,')[-1].strip()
        pattern = '%B %d, %Y'
        time_unknown = 1
        game_dt = get_utc_time(game_time, pattern, 'US/Eastern')
    elif "Z" in game_time:
        game_time = game_time.replace('T', ' ').replace('Z', '')
        pattern = '%Y-%m-%d %H:%M'
        time_unknown = 0
        game_dt = get_utc_time(game_time, pattern, 'UTC')
    else:
        pattern = '%I:%M %p ET, %B %d, %Y'
        time_unknown = 0
        game_dt = get_utc_time(game_time, pattern, 'US/Eastern')

    return game_dt, time_unknown


def get_event(game_time):
    game_time = datetime.datetime.strptime(game_time, '%Y-%m-%d %H:%M:%S')
    conn, cursor = mysql_connection()
    query = "select title, season_start, season_end from sports_tournaments where id in (451, 484, 895, 1280, 1683, 2555, 3011) order by season_start"
    cursor.execute(query)
    data = cursor.fetchall()
    event_name = ''

    for mx in data:
        event, start_date, end_date = mx
        if str(start_date.date()) == str(game_time.date()):
            event_name = event
            break

        elif start_date < game_time and game_time < end_date and event == "MLB Draft":
            event_name = event
            break

        elif start_date < game_time and game_time < end_date:
            event_name = event

    cursor.close()
    conn.close()
    return event_name


class MLBESPNSpider(VTVSpider):
    name = "mlb_espn_spider"
    game_url = 'http://scores.espn.go.com/mlb/conversation?gameId=%s'
    start_urls = []

    def start_requests(self):
        next_week_days = []
        top_url = 'http://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?lang=en&region=in&calendartype=blacklist&limit=100&dates=%s'

        if self.spider_type == "scores":
            for i in range(0, 10):
                next_week_days.append(
                    (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d'))

        elif self.spider_type == "schedules":
            for i in range(0, 20):
                next_week_days.append(
                    (datetime.datetime.now() + datetime.timedelta(days=i)).strftime('%Y%m%d'))

        for wday in next_week_days:
            t_url = top_url % wday
            yield Request(t_url, callback=self.parse, meta={})

    def parse(self, response):
        raw_data = json.loads(response.body)
        games = raw_data.get('events', '')
        for game in games:
            game_id = game['id']
            game_dt = game['date']
            status = game['status']['type']['description'].lower()
            team_data = game['competitions'][0]['competitors']
            game_note = game['competitions'][0]['notes']
            stadium = game['competitions'][0]['venue']['fullName']

            try:
                city = game['competitions'][0]['venue']['address']['city']
            except:
                city = ''

            try:
                state = game['competitions'][0]['venue']['address']['state']
            except:
                state = ''

            if game_note:
                game_note = game_note[0]['headline']
            else:
                game_note = ''

            teams, result = {}, {}
            for data in team_data:
                team_sk = data['team']['abbreviation']
                home_away = data['homeAway']

                if home_away.lower() == 'home':
                    is_home = 1
                    home_sk = team_sk

                elif home_away.lower() == 'away':
                    is_home = 0
                    away_sk = team_sk

                teams[team_sk] = (is_home, '')
                location = data.get('location', '')

                if status == 'final':
                    final_score = data['score']
                    line_scores = data['linescores']
                    scores = [int(list(line_score.values())[0])
                              for line_score in line_scores]
                    winner = data['winner']

                    if is_home == 1:
                        home_total = final_score
                        home_scores = scores
                        home_q9 = 0
                        home_q1 = home_scores[0]
                        home_q2 = home_scores[1]
                        home_q3 = home_scores[2]
                        home_q4 = home_scores[3]
                        home_q5 = home_scores[4]
                        home_q6 = home_scores[5]
                        home_q7 = home_scores[6]
                        home_q8 = home_scores[7]
                        if len(home_scores) > 8:
                            home_q9 = home_scores[8]

                    elif is_home == 0:
                        away_total = final_score
                        away_scores = scores
                        away_q9 = 0
                        away_q1 = away_scores[0]
                        away_q2 = away_scores[1]
                        away_q3 = away_scores[2]
                        away_q4 = away_scores[3]
                        away_q5 = away_scores[4]
                        away_q6 = away_scores[5]
                        away_q7 = away_scores[6]
                        away_q8 = away_scores[7]
                        if len(away_scores) > 8:
                            away_q9 = away_scores[8]

            scores_link = self.game_url % game_id
            if status not in ['scheduled', 'postponed', 'canceled'] and self.spider_type == 'scores':
                if status == 'final':
                    status = 'completed'
                else:
                    status = 'ongoing'
                game_score = home_total + '-' + away_total
                if (int(away_total) > int(home_total)):
                    winner = away_sk
                elif int(away_total) < int(home_total):
                    winner = home_sk
                else:
                    winner = ''
                result = {'0': {'winner': winner, 'score': game_score},
                          home_sk: {'I1': home_q1, 'I2': home_q2, 'I3': home_q3,
                                    'I4': home_q4, 'I5': home_q5, 'I6': home_q6, 'I7': home_q7,
                                    'I8': home_q8, 'I9': home_q9, 'final': home_total},
                          away_sk: {'I1': away_q1, 'I2': away_q2, 'I3': away_q3,
                                    'I4': away_q4, 'I5': away_q5, 'I6': away_q6, 'I7': away_q7,
                                    'I8': away_q8, 'I9': away_q9, 'final': away_total}}

                yield Request(scores_link, callback=self.parse_scores,
                              meta={'teams': teams, 'status': status, 'game_note': game_note,
                                    'result': result, 'game_dt': game_dt, 'location': location,
                                    'city': city, 'state': state, 'stadium': stadium})

            elif "postponed" in status.lower() or "canceled" in status.lower():
                result = {}
                yield Request(scores_link, callback=self.parse_scores,
                              meta={'teams': teams, 'status': status, 'game_note': game_note,
                                    'game_dt': game_dt, 'location': location, 'city': city,
                                    'state': state, 'stadium': stadium, 'result': result})

            elif status.lower() == 'scheduled' and self.spider_type == "schedules":
                yield Request(scores_link, self.parse_scores,
                              meta={'teams': teams, 'status': status, 'game_note': game_note,
                                    'game_dt': game_dt, 'location': location, 'city': city,
                                    'state': state, 'stadium': stadium})

    def parse_scores(self, response):
        sel = Selector(response)
        record = SportsSetupItem()
        game_note = response.meta['game_note']
        game_sk = response.url.split('=')[-1]
        game_text = " vs ".join(extract_list_data(
            sel, '//div[@class="team-info"]//span[@class="short-name"]//text()'))
        game_note = game_text + " at " + response.meta['stadium']

        if response.meta['game_dt']:
            game_dt, time_unknown = get_game_date_time(
                response.meta['game_dt'])

        city = response.meta['city']
        stadium = response.meta['stadium']
        country = 'USA'
        state = response.meta['state']

        channel = extract_data(
            sel, '//div[@class="game-vitals"]//div/p[contains(text(), "Coverage:")]/strong//text()').strip()
        if not channel:
            channel = extract_data(
                sel, '//div[@class="game-vitals"]/p[contains(text(), "Coverage:")]/strong//text()').strip()
        if "/" in channel:
            channel = channel.replace('/', '<>')

        event = get_event(game_dt)
        if not event:
            event = ""

        if response.meta['location']:
            city = response.meta['location']

        rich_data = {'channels': str(channel), 'location': {'city': city,
                                                            'state': state, 'country': country},
                     'stadium': stadium, 'game_note': game_note}

        record['game'] = "baseball"
        record['participant_type'] = "team"
        record['event'] = event
        record['tournament'] = "Major League Baseball"
        record['affiliation'] = "mlb"
        record['source'] = "espn_mlb"
        record['source_key'] = game_sk
        record['game_status'] = response.meta['status'].replace(
            'canceled', 'cancelled')
        record['participants'] = response.meta['teams']
        record['game_datetime'] = game_dt
        record['reference_url'] = response.url
        record['time_unknown'] = time_unknown
        record['rich_data'] = rich_data

        if city and game_dt:
            record['tz_info'] = get_tzinfo(city=city, game_datetime=game_dt)
            if not record['tz_info']:
                record['tz_info'] = get_tzinfo(
                    city=city, country=country, game_datetime=game_dt)

        if response.meta['status'] not in ['scheduled', 'postponed'] and self.spider_type == 'scores':
            record['result'] = response.meta['result']
            yield record

        elif response.meta['status'] not in ['final', 'ongoing'] and self.spider_type == 'schedules':
            record['result'] = {}
            yield record
