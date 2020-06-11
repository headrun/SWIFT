import genericFileInterfaces
import foldFileInterface
from datetime import datetime
import requests
import json
from pprint import pprint
import time
from data_schema import get_schema

url ="http://api.thuuz.com/2.4/games?auth_code=14bfa6bb838386c8&type=normal&status=7&days=14&sport_leagues=soccer.fran,basketball.nba,soccer.euro"
GENRE_DICT = {'basketball' : 'RVG2647', 'soccer' : 'RVG2906'}
programs_file = open("thuuz.data", "w+")
schedule_file = file("DATA_AVAILABILITY_FILE.thuuz", "w+")

schema = ['Gi', 'Sk', 'Ti', 'Ep', 'Vt', 'Ot', 'Ll', 'Du', 'Ge', 'De', 'Od', 'Tg']
schema = get_schema(schema)
x = requests.get(url).text



j_data = json.loads(x)
for json_data in j_data['ratings']:
    game_id         = str(json_data['id'])
    home_team       = json_data['home_team']
    away_team       = json_data['away_team']
    game_datetime   = json_data['date']
    sport           = json_data['sport']
    league          = json_data['league']

    game_datetime   = datetime.strptime(game_datetime, "%Y-%m-%d %H:%M:%S")
    game_date       = game_datetime.date
    genre           = "%s{%s}" %(sport.capitalize(), GENRE_DICT[sport])

    data_record = [game_id, game_id, league.upper(), "%s at %s" %(away_team, home_team),
                   "tvvideo", "Other", "english", "150", "%s{eng}" %genre,
                   "%s at %s" %(away_team, home_team), game_date().strftime("%Y-%m-%d"), "game"]
    genericFileInterfaces.writeSingleRecord(programs_file, data_record, schema, 'utf-8')

    ci = "Ci: %s" %game_id
    at = "At: T: %s#4500<>Li:" %(game_datetime.strftime("%d#%m#%Y#%H#%M"))
    tg = "Tg: LIVE"

    record = "%s#<>#%s#<>#%s" %(ci, at, tg)
    schedule_file.write("%s\n" %record)
