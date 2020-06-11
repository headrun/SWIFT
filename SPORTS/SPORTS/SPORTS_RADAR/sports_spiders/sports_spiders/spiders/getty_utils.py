HOST    = "10.28.218.81"
DB_NAME = "GETTY_IMAGES"
USER    = "veveo"
passwd = "veveo123"

#players
INS_QUERY = "insert into getty_games(getty_event_id, name, date, sub_header, league, is_crawled, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_INS_QUERY = "insert into getty_players(getty_player_id, name, sub_header, league, is_crawled,created_at, modified_at) values(%s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"

PL_MAP_QRY = "insert into getty_players_mapping(entity_id, entity_type, getty_player_id, is_primary, created_at,modified_at)values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()"

GM_MAP_QRY = "insert into getty_games_mapping(entity_id, entity_type, getty_event_id, is_primary, created_at,modified_at)values(%s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()"

pl_po_sel_qry1 = 'select id from SPORTSDB.sports_participants where sport_id="%s" and participant_type="player" and title like "%s"'

pl_po_sel_qry2 = 'select id from SPORTSDB.sports_participants where sport_id="%s" and participant_type="player" and aka like "%s"'

pl_po_sel_qry3 = 'select R.player_id, R.team_id from SPORTSDB.sports_roster R, SPORTSDB.sports_tournaments_participants TP, SPORTSDB.sports_tournaments T where TP.participant_id=R.team_id and T.title=%s and T.id=TP.tournament_id and R.player_id=%s'

GM_PAR_QRY1 = "select distinct SP.id from SPORTSDB.sports_participants SP left join SPORTSDB.sports_teams ST on SP.id = ST.participant_id where title like %s and tournament_id = %s and sport_id = %s"

GM_PAR_QRY2 = "select distinct SP.id from SPORTSDB.sports_participants SP left join SPORTSDB.sports_teams ST on SP.id = ST.participant_id where aka like %s and tournament_id = %s and sport_id = %s"

GM_PAR_QRY3 = "select distinct SP.id from SPORTSDB.sports_participants SP left join SPORTSDB.sports_teams ST on SP.id = ST.participant_id where aka like %s and tournament_id = %s and sport_id = %s"

GM_PAR_QRY4 = "select id from SPORTSDB.sports_participants where title like %s"

GAME_QRY = "select distinct game_id from SPORTSDB.sports_games SG left join SPORTSDB.sports_games_participants GP on SG.id = GP.game_id where participant_id in (%s, %s) and game_datetime like %s and tournament_id = %s"

#images

DB1 = "SPORTSDB"
IMAGE_QUERY = 'insert into sports_images (url_sk, image_url, image_type, height, width, description, image_created, image_updated, league, keywords, weight, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), image_type= %s, image_url=%s, keywords=%s, weight=%s'

IMG_MAP_QUERY = 'insert into sports_images_mapping(entity_id, entity_type, image_id, is_primary, created_at, modified_at) values(%s, %s, %s, 0, now(), now()) on duplicate key update modified_at=now()'

RIGHT_QUERY = 'insert into sports_image_rights_mapping(image_id, right_id, created_at, modified_at) values(%s, 1, now(), now()) on duplicate key update modified_at=now(), right_id = 1'

PLAYERS_QUERY = "select getty_player_id, league from GETTY_IMAGES.getty_players where is_crawled=0 and league = 'Liga MX'"

EVENTS_QUERY = "update GETTY_IMAGES.getty_players set is_crawled=1 where getty_player_id=%s limit 1"

query = "select entity_id from GETTY_IMAGES.getty_players_mapping where getty_player_id=%s and entity_type='player'"

sel_qry = 'select id from sports_images where url_sk=%s'

insert_qrt_he = "insert into sports_images_status(user_id,image_id,status,flag,created_at,modified_at) values(%s,%s,%s,%s,now(),now()) on duplicate key update modified_at=now()"

upadte_qry = "update sports_images_status set flag='0' where id=%s and user_id=%s"

sel_qry_h = "select id,image_id from sports_images_status where flag='1' and user_id=%s and image_id in(select id from sports_images where image_type='headshots')"

sel_qry_he = 'select id,image_created from sports_images where url_sk=%s and image_type="headshots"'

upda_qry = "update sports_images_status set flag='1' where image_id=%s"

sel_qry_a = "select id,image_id from sports_images_status where flag='1' and user_id = %s and image_id in(select id from sports_images where image_type='actionshots')"

sel_qry_ac = 'select id,image_created from sports_images where url_sk=%s and image_type="actionshots"'

upda_qry_ac = "update sports_images_status set flag='1' where image_id=%s and user_id=%s"
