import MySQLdb
import datetime
import urllib
import time

SK_QUERY = 'select entity_id from sports_source_keys where \
entity_type="participant" and source="basketball_realgm" and source_key= "%s"'

TEAM_URL = """http://10.4.18.34/cgi-bin/add_teams.py?name=%s&aka=%s&game=%s&participant_type=%s&img=%s&bp=%s&ref=%s&loc=%s&short=%s&callsign=%s&category=%s&kws=%s&tou_name=%s&division=%s&gender=%s&formed=%s&tz=%s&logo=%s&vtap=%s&yt=%s&std=%s&src=%s&sk=%s&action=submit"""

class ScandinaviaBasketball:

    def __init__(self):
        #self.conn   = MySQLdb.connect(host="10.4.18.34", user="root", db="SPORTSDB_BKP", charset='utf8', use_unicode=True)
        self.conn   = MySQLdb.connect(host="10.4.18.183", user="root", db="SPORTSDB", charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def main(self):
        data = open('scan_teams', 'r').readlines()
        for record in data:
            team_data = [rec.replace('"', '').strip() for rec in record.strip().split(',')]
            team_name = team_data[0]
            tou_name = team_data[1]
            source_key = team_data[4]
            image_link = team_data[5]
            location_info = team_data[6]

            city = country = ''

            if "<>" in location_info:
                city, country = location_info.split('<>')
            elif location_info:
                country = location_info
                city = ''
            found = team_data[7]
            formed = ''
            if found:
                if "/" in found:
                    formed = (datetime.datetime(*time.strptime(found.strip(), '%m/%d/%Y')[0:6])).date()
                else:
                    formed = (datetime.datetime(*time.strptime(found.strip(), '%Y')[0:6])).date()
            stadium_name = team_data[8]
            team_wiki = team_data[9]
            reference_url = team_data[11]
            short_title = team_data[2]
            if location_info:
                loc_id = 'select id from sports_locations where city ="%s" and country = "%s" limit 1' %(city, country)
                self.cursor.execute(loc_id)
                loc_id = self.cursor.fetchall()
                if not loc_id:
                    loc_id = 'select id from sports_locations where city ="" and state = "" and country = "%s" limit 1' %(country)
                    self.cursor.execute(loc_id)
                    loc_id = self.cursor.fetchall()
                if loc_id:
                    loc_id = str(loc_id[0][0])
            else:
                loc_id = ''
            if stadium_name:
                std_id = 'select id from sports_stadiums where title =  "%s"' %(stadium_name)
                self.cursor.execute(std_id)
                std_id = self.cursor.fetchall()
                if std_id:
                    std_id = str(std_id[0][0])
            else:
                std_id = ''

            pl_exists, pl_id = self.check_player(source_key)

            if pl_id:
                print "team_name", team_name
                continue
            else:
                aka, bp = "", "-150"
                source = "basketball_realgm"
                category = "euro-nba"
                par_type = "team"
                game = "basketball"
                gender = "male"
                vtap = ""
                you_tube = ""
                logo_url = ""
                tz = ""

                team_values = (team_name, aka, game, par_type, image_link, bp, reference_url, loc_id, short_title, '', category, "", tou_name, "", gender, formed, tz, logo_url, vtap, you_tube, std_id, source, source_key)
                if team_name:
                    pl_add_url = (TEAM_URL % team_values).replace('\n', '')
                    urllib.urlopen(pl_add_url)


    def check_player(self, source_key):
        self.cursor.execute(SK_QUERY % source_key)

        entity_id = self.cursor.fetchone()

        if entity_id:
            pl_exists = True
            pl_id = entity_id
        else:
            pl_exists = False
            pl_id = ''
        return pl_exists, pl_id



if __name__ == '__main__':
    OBJ = ScandinaviaBasketball()
    OBJ.main()

