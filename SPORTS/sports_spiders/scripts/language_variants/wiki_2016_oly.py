from vtvspider import VTVSpider, get_nodes, extract_data, get_utc_time
from vtvspider import get_tzinfo
from scrapy.selector import Selector
from scrapy.http import Request
from sports_spiders.items import SportsSetupItem
import datetime
import time
import re
import codecs
import MySQLdb
from vtvspider import get_weight

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="rioolympics_wiki" and source_key= "%s"'

PL_NAME_QUERY = 'select id from sports_participants where \
title like "%s" and game="%s" and participant_type="player"'

game_dict = { 'athletics' : 'Athlete',
'boxing' : 'Boxer',
'rowing' : ' Rower',
'wrestling' : 'Wrestler',
'canoeing' : 'Canoeist',
'gymnastics' : 'Gymnast',
'taekwondo' : 'Taekwondoin',
'archery' : 'Archer',
'modern pentathlon' : 'Athlete',
'sailing' : 'Sailor',
'diving' : 'Diver',
'shooting' : 'Shooter',
'swimming' : 'Swimmer',
'table' : 'Athlete',
'beach' : 'Athlete',
'triathlon' : 'Athlete',
'cycling' : 'Cyclist',
'judo' : 'Judoka',
'weightlifting' : 'Weight lifter',
'fencing' : 'Fencer',
'synchronized' : 'Swimmer'}


class Olympics(VTVSpider):
    name = 'player_info'
    start_urls = ['https://en.wikipedia.org/wiki/2016_Summer_Olympics']

    def __init__(self):
        self.conn = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.game_dict = {}
        self.f = open('missed_games.txt', 'wb')


    def parse(self, response):
        sel = Selector(response)
        country_nodes = sel.xpath('//tr[th[a[contains(text(),"National Olympic Committees")]]]/following-sibling::tr[1]//ul/li/a')
        for node in country_nodes:
            country = extract_data(node, './text()')
            url = extract_data(node, './@href')
            if '/wiki/' not in url: continue
            if 'http' not in url:
                url = 'https://en.wikipedia.org' + url
            yield Request(url, self.parse_country, meta={'country': country})

    def parse_country(self, response):
        sel = Selector(response)
        con = response.meta['country']
        player_nodes = sel.xpath('//table[@class="wikitable"]')
        for pl_node in player_nodes:
            try:
                p_new_gender = ''.join(pl_node.xpath('./preceding-sibling::dl')[-1].xpath('./dt/text()').extract())
            except:
                p_new_gender = ''
            if 'women' in p_new_gender.lower(): p_gender = 'female'
            elif 'men' in p_new_gender.lower(): p_gender = 'male'
            else: p_gender = ''
            player_link_nodes = pl_node.xpath('.//tr[@align="center"]/td[1]/a')
            for node in player_link_nodes:
                player_name = extract_data(node, './text()')
                if 'Time trial' in player_name: continue
                player_link = extract_data(node, './@href')
                if 'http' not in player_link:
                    player_link = 'https://en.wikipedia.org' + player_link
                player_event = extract_data(node, '../following-sibling::td[1]/a/text()')
                prayer_event_link = extract_data(node, '../following-sibling::td[1]/a/@href')
                player_event_link_text = extract_data(node, '../following-sibling::td[1]/a/text()')
                game = ''
                if prayer_event_link:
                    game = prayer_event_link.split('/wiki/')[-1].split('_')[0].strip().lower()
                    if 'modern' in game:
                        game = 'modern pentathlon'
                if not p_gender:
                    if 'women' in prayer_event_link.lower() or 'women' in player_event_link_text.lower():
                        p_gender = 'female'
                    elif 'men' in prayer_event_link.lower() or 'men' in player_event_link_text.lower():
                        p_gender = 'male'
                    else:
                        p_gender = ''
                    if '49erFX' in prayer_event_link:
                        p_gender = 'female'
                if player_event:
                    self.temp = player_event
                else:
                    player_event = self.temp
                try:
                    pl_role = game_dict[game]
                except:
                    pl_role = 'athlete'
                if 'table' in game:
                    game = 'table tennis'
                if 'beach' in game:
                    game = 'beach volleyball'
                if 'synchronized' in game:
                    game = 'synchronized swimming'
                yield Request(player_link, self.parse_player, meta = {'game': game, 'gender': p_gender, 'name':player_name, 'nation':con, 'p_role':pl_role, 'event_link': prayer_event_link, 'country_link': response.url})

    def parse_player(self, response):
        sel = Selector(response)
        p_name = response.meta['name'].strip()
        p_event_link = response.meta['event_link']
        p_country_link = response.meta['country_link']
        pl_role = response.meta['p_role']
        p_gender = response.meta['gender']

        p_game = response.meta['game'].lower()
        pl_sk = response.url.split('/')[-1]
        nation = response.meta['nation']
        game_id_qry  = 'select id from sports_types where title = %s'
        game_id_vals = (p_game)
        self.cursor.execute(game_id_qry, game_id_vals)
        try:
            game_id = self.cursor.fetchall()[0][0]
        except:
            game_id = ''
        p_dob = ''
        player_bd = extract_data(sel, '//table[@class="infobox vcard"]//tr/th[contains(text(),"Born")]/following-sibling::td[1]//span[@class="bday"]/text()')
        player_height = extract_data(sel, '//table[@class="infobox vcard"]//tr/th[contains(text(),"Height")]/following-sibling::td[1]//text()')
        if player_height:
            if 'cm' in ''.join(re.findall('\((.*?)\)',player_height)):
                player_height = int(''.join(re.findall('\((.*?)\)',player_height)).replace('cm','').strip())
            elif 'm' in ''.join(re.findall('\((.*?)\)',player_height)):
                palyer_height = str(int(float(''.join(re.findall('\((.*?)\)',player_height)).replace('m','').strip()) * 100))+ ' cm'
            elif 'cm' in player_height:
                player_height = player_height.split('(')[0].strip()
            elif 'm' in player_height:
                player_height = str(int((float(player_height.split('m')[0].strip())*100))) + ' cm'
            else:
                player_height = ''
        if ' m' in player_height:
            player_height = ''.join(re.findall('(\(.*?\))',player_height)).strip(' m()')
            try:
                player_height = str(float(player_height) * 100) + ' cm'
            except:
                player_height = ''

        if player_height.strip() and 'cm' not in player_height:
            player_height = player_height.strip() + ' cm'
        player_weight = extract_data(sel, '//table[@class="infobox vcard"]//tr/th[contains(text(),"Weight")]/following-sibling::td[1]//text()')
        player_weight = ''.join(re.findall('\((.*?)\)',player_weight))
        if 'lb' in player_weight:
            player_weight = player_weight.split('lb')[0].strip()
            player_weight = get_weight(lbs = player_weight)
        if ';' in player_weight:
            player_weight = player_weight.split(';')[0].strip()
        if ',' in player_weight:
            player_weight = player_weight.split(',')[-1].strip()
        if 'kg' not in player_weight:
            player_weight = ''

        if player_bd:
            try:
                p_dob = str(datetime.datetime.strptime(player_bd, '%Y-%m-%d'))
            except:
                p_dob = ''
        p_birth_place = extract_data(sel, '//span[@class="birthplace"]//text()').replace('\n', '').replace('\t','').strip()
        if '[' in p_birth_place:
            p_birth_place = p_birth_place.split('[')[0].strip()
        p_age = extract_data(sel, '//span[@class="noprint ForceAgeToShow"]/text()')
        if p_age:
            p_age = ''.join(re.findall('\d.', p_age))

        pl_id = self.check_title(p_name, p_game)
        if pl_id:
            query = 'select birth_date from sports_players where participant_id=%s'
            self.cursor.execute(query, pl_id[0])
            data = self.cursor.fetchone()
            if data:
                db_dob = str(data[0]).split(' ')[0].strip()
            else:
                db_dob = ''
            qry = 'update sports_players set gender = %s where participant_id=%s'
            values = (p_gender, pl_id[0])
            self.cursor.execute(qry, values)

            if db_dob == p_dob.split(' ')[0]:
                self.add_source_key(str(pl_id[0]), pl_sk)
                query = "select id from sports_participants where title like "+  "'" + '%' + nation.lower() + '%' + '2016 Summer Olympics' + '%' + "'"

                self.cursor.execute(query)
                team_id = self.cursor.fetchone()[0]

                query = 'select id from sports_players where participant_id = %s'
                values = (pl_id[0])
                self.cursor.execute(query, values)
                player_id = self.cursor.fetchone()[0]

                query = 'insert into sports_roster (team_id, player_id, player_role, status, season, created_at, modified_at, player_number, status_remarks) values (%s, %s, %s, %s, %s, now(), now(), "", "") on duplicate key update modified_at = now()'
                values = (team_id, pl_id[0], pl_role, 'active', '2016')
                self.cursor.execute(query, values)

            else:
                print 'dob not matched', p_name, db_dob, p_dob
        else:
            self.cursor.execute('select id, gid from sports_participants where id in (select max(id) from sports_participants)')
            table_data = self.cursor.fetchall()
            max_id, max_gid = table_data[0]
            pl_url = response.url

            pl_loc_id = self.get_location('', nation)
            if p_game:
                next_id = max_id + 1
                next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').replace('PL', '')) + 1)

                query = "insert into sports_participants (id, gid, title, aka, game, participant_type, image_link, base_popularity, reference_url, location_id, sport_id, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
                values = (next_id, next_gid, p_name, '', p_game, 'player', '', '200', response.url, pl_loc_id, game_id)
                self.cursor.execute(query, values)

                query = "insert into sports_players (participant_id, debut, main_role, roles, gender, age, height, weight, birth_date, birth_place, salary_pop, rating_pop, weight_class, marital_status, participant_since, competitor_since, created_at, modified_at, display_title, short_title) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now(), '', '') on duplicate key update height = %s, weight = %s, birth_place = %s, modified_at = now();"
                values = (next_id, '', '', '', p_gender, p_age, player_height, player_weight, p_dob, p_birth_place, '', '', '', '', '', '', player_height, player_weight, p_birth_place)
                self.cursor.execute(query, values)

                query = "insert into sports_source_keys (entity_id, entity_type, source, source_key, created_at, modified_at) values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
                values = (next_id, 'participant', 'rioolympics_wiki', pl_sk)
                self.cursor.execute(query, values)

                query = "select id from sports_participants where title like "+  "'" + '%' + nation.lower() + '%' + '2016 Summer Olympics' + '%' + "'"

                self.cursor.execute(query)
                try:
                    team_id = self.cursor.fetchone()[0]
                    query = 'select id from sports_players where participant_id = %s'
                    values = (next_id)
                    self.cursor.execute(query, values)
                    player_id = self.cursor.fetchone()[0]

                    query = 'insert into sports_roster (team_id, player_id, player_role, status, season, created_at, modified_at, player_number, status_remarks) values (%s, %s, %s, %s, %s, now(), now(), "", "") on duplicate key update modified_at = now()'
                    values = (team_id, next_id, pl_role, 'active', '2016')
                    self.cursor.execute(query, values)

                except:
                    print query
                    print nation


                print "Added player" , p_name
            else:
                self.f.write('%s\n'%response.url)

    def get_location(self, city, country):
        query = 'select id from sports_locations where country="%s" and state="" and city=""'
        self.cursor.execute(query % country)
        data = self.cursor.fetchone()
        if data:
            data = data[0]
        else:
            data = ''
        return data

    def check_player(self, pl_sk):
        self.cursor.execute(SK_QUERY % pl_sk)
        entity_id = self.cursor.fetchone()
        if entity_id:
            pl_exists = True
        else:
            pl_exists = False
        return pl_exists, entity_id

    def check_title(self, name, game):
        pl_name = "%" + name + "%"
        self.cursor.execute(PL_NAME_QUERY % (pl_name, game))
        pl_id = self.cursor.fetchone()
        return pl_id

    def add_source_key(self, entity_id, _id):
        if _id and entity_id:
            query = "insert into sports_source_keys (entity_id, entity_type, \
                    source, source_key, created_at, modified_at) \
                    values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
            values = (entity_id, 'participant', 'rioolympics_wiki', _id)
            self.cursor.execute(query, values)

