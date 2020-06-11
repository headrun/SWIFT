import MySQLdb
from vtvspider import VTVSpider


def create_cursor():
    con = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
    #con = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
    cursor = con.cursor()
    return con, cursor


def check_player(pl_sk):
    con, cursor = create_cursor()
    cursor.execute(SK_QUERY % pl_sk)
    entity_id = cursor.fetchone()
    con.close()
    if entity_id:
        pl_exists = True
        pl_id = entity_id
    else:
        pl_exists = False
        pl_id = ''
    return pl_exists, pl_id

def add_source_key(entity_id, _id):
    if _id and entity_id:
        con, cursor = create_cursor()
        query = "insert into sports_source_keys (entity_id, entity_type, \
                 source, source_key, created_at, modified_at) \
                 values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
        values = (entity_id, 'participant', 'sanzar_rugby', _id)

        cursor.execute(query, values)
        con.close()

def check_title(name):
    con, cursor = create_cursor()
    cursor.execute(PL_NAME_QUERY % (name, GAME))
    pl_id = cursor.fetchone()
    con.close()
    return pl_id
PAR_QUERY = "insert into sports_participants (id, gid, title, aka, game, \
             participant_type, image_link, base_popularity, reference_url, \
             location_id, created_at, modified_at) \
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_QUERY = "insert into sports_players (participant_id, debut, main_role, \
            roles, gender, age, height, weight, birth_date, birth_place, \
            salary_pop, rating_pop, weight_class, marital_status, \
            participant_since, competitor_since, created_at, modified_at) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
            %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now();"
MAX_ID_QUERY = 'select id, gid from sports_participants where id in \
                (select max(id) from sports_participants)'

PL_NAME_QUERY = 'select id from sports_participants where \
title = "%s" and game="%s" and participant_type="player"';

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source = "sanzar_rugby" and source_key= "%s"'


GAME = 'rugby union'
PAR_TYPE = 'player'
BASE_POP = "200"
LOC = '0'
DEBUT = "0000-00-00"
ROLES = ''
SAL_POP = ''
RATING_POP = ''
MARITAL_STATUS = ''
PAR_SINCE = COMP_SINCE = ''
WEIGHT_CLASS = AKA = ''
GENDER = 'male'


class RugbyChampPlayers(VTVSpider):
    name            = "rugbychamp_players"
    allowed_domains = []
    start_urls      = ['http://omo.akamai.opta.net/competition.php?feed_type=ru4&competition=214&season_id=2016&user=owv2&psw=wacRUs5U&jsoncallback=RU4_205_2016_t73']

    def parse(self, response):
        raw_data    = response.body.replace('RU4_205_2016_t73(', '').replace(')', '')
        data        = eval(raw_data)
        if data:
            pl_data = data.get('seasonstats', '').get('teams', '').get('team', '')
            for pl_dat in pl_data:
                player_data = pl_dat.get('players', '').get('player', '')
                for pla_dat in player_data:
                    pl_dob = pla_dat.get('@attributes', '').get('dob', '')
                    height = pla_dat.get('@attributes', '').get('height', '')
                    place_birth = ''
                    if place_birth:
                        birth_place = place_birth.replace('(', ',').replace(')', '').strip()
                    else:
                        birth_place = ''
                    player_id = pla_dat.get('@attributes', '').get('player_id', '')
                    player_name = pla_dat.get('@attributes', '').get('player_known_name', '')
                    if player_name:
                        player_name  = player_name.encode('utf-8')
                    position = pla_dat.get('@attributes', '').get('regular_position', '')
                    weight = pla_dat.get('@attributes', '').get('weight', '')
                    team_id = pla_dat.get('playerstats', '').get('@attributes', '').get('player_team_id', '')
                    player_image = 'http://images.akamai.opta.net/rugby/player/%s_103x155/%s.jpg' %(team_id, player_id)
                    ref_url = 'http://www.sanzarrugby.com/superrugby/player-profile/?season=2015&competition=205&team=%s&player=%s' %(team_id,player_id)
                    if weight:
                        weight = weight + " kg"
                    if height:
                        height = height + " cm"
                    loc_id = ''
                    con, cursor = create_cursor()
                    pl_exists, pl_id = check_player(player_id)
                    if pl_exists == False:
                        pts_id = check_title(player_name)
                        if pts_id:
                            print "source key", player_name
                            add_source_key(str(pts_id[0]), player_id)
                        else:
                            print "add player", player_name
                            age = ''
                            cursor.execute(MAX_ID_QUERY)
                            pl_data = cursor.fetchall()
                            max_id, max_gid = pl_data[0]
                            next_id = max_id + 1
                            next_gid = 'PL' + str(int(max_gid.replace('TEAM', '').\
                                    replace('PL', '')) + 1)

                            values = (next_id, next_gid, player_name, AKA, GAME, PAR_TYPE, player_image, \
                                  BASE_POP, ref_url, loc_id)
                            cursor.execute(PAR_QUERY, values)
                            values = (next_id, DEBUT, position, ROLES, GENDER, \
                                  age, height, weight, pl_dob, birth_place, SAL_POP, RATING_POP, \
                                  WEIGHT_CLASS, MARITAL_STATUS, PAR_SINCE, COMP_SINCE)

                            cursor.execute(PL_QUERY, values)
                            add_source_key(next_id, player_id)
                            con.close()
